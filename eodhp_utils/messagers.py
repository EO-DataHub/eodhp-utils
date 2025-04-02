import dataclasses
import json
import logging
import typing
from abc import ABC, abstractmethod
from typing import Optional, Sequence, Union

import botocore
import botocore.exceptions
import pulsar
import pulsar.exceptions
from opentelemetry.propagate import inject
from pulsar import Message
from pulsar.schema import BytesSchema, JsonSchema, Record, Schema

import eodhp_utils
import eodhp_utils.pulsar.messages


class TemporaryFailure(Exception):
    """
    Throw exceptions of this type if an error occurs during processing and it makes sense to
    retry later. Examples might be network failures or running out of disk space.

    If the exception will definitely occur again, such as a message being unparseable, then
    any other Exception can be thrown.
    """

    pass


def _is_boto_error_temporary(
    exc: Union[botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError],
) -> bool:
    temp_excepts = (
        botocore.exceptions.ConnectionError,
        botocore.exceptions.HTTPClientError,
        botocore.exceptions.NoCredentialsError,
        botocore.exceptions.PaginationError,
        botocore.exceptions.ChecksumError,
        botocore.exceptions.WaiterError,
        botocore.exceptions.IncompleteReadError,
        botocore.exceptions.CapacityNotAvailableError,
    )

    if isinstance(exc, botocore.exceptions.ClientError):
        return exc.response["ResponseMetadata"]["HTTPStatusCode"] >= 500

    return any(map(lambda excclass: isinstance(exc, excclass), temp_excepts))


def _is_pulsar_error_temporary(exc: pulsar.exceptions.PulsarException) -> bool:
    # These are exceptions that sound from the name to be things we can retry after.
    # They're listed in pulsar/exceptions.py
    temp_excepts = (
        pulsar.exceptions.Timeout,
        pulsar.exceptions.ConnectError,
        pulsar.exceptions.ReadError,
        pulsar.exceptions.BrokerPersistenceError,
        pulsar.exceptions.ChecksumError,
        pulsar.exceptions.ConsumerBusy,
        pulsar.exceptions.NotConnected,
        pulsar.exceptions.AlreadyClosed,
        pulsar.exceptions.ProducerBusy,
        pulsar.exceptions.TooManyLookupRequestException,
        pulsar.exceptions.ServiceUnitNotReady,
        pulsar.exceptions.ProducerBlockedQuotaExceededError,
        pulsar.exceptions.ProducerBlockedQuotaExceededException,
        pulsar.exceptions.ProducerQueueIsFull,
        pulsar.exceptions.InvalidTxnStatusError,
        pulsar.exceptions.TransactionConflict,
        pulsar.exceptions.TransactionNotFound,
        pulsar.exceptions.MemoryBufferIsFull,
        pulsar.exceptions.Interrupted,
    )

    return any(map(lambda excclass: isinstance(exc, excclass), temp_excepts))


class Messager[MSGTYPE](ABC):
    """
    This is an abstract base class for creating 'messagers'. Messagers are classes which consume
    and/or produce catalogue Pulsar messages and entries in S3. Harvesters, transformers and
    ingesters can all be written as messagers. Messagers can be tested without any mocks.

    Messagers are triggered by some input and return a list of actions as output:
      * OutputFileAction:
          Write an entry to or delete an entry from the catalogue population bucket and emit it as
          an added, changed or deleted key in a Pulsar message.
      * S3UploadAction:
          Write a file to an S3 bucket.
      * FailureAction:
          Mark the whole processing or a single catalogue key as having failed permanent (no retry
          allowed) or temporarily (retry allowed).

    You should inherit from the correct subclass:
      * Producing Pulsar messages only (harvester):
          Inherit from Messager and implement process_msg(self, msg: <my obj>).
          Return a list of actions, probably OutputFileActions.

          Design your harvester to encapsulate each harvested entry as a <my obj> and call
          my_harvester.consume(my_obj).

          Write your tests to call process_msg directly.

      * Consuming Pulsar catalogue change messages only (ingester):
          You have four choices, in decreasing preference:
            * Inherit from CatalogueSTACChangeMessager and implement the process_update_stac and
              process_delete methods. For every new or changed STAC entry in the messages
              received by consume(), process_update_stac will be called with the STAC in
              dictionary form.
            * Inherit from CatalogueChangeBodyMessager and implement process_update_body. This
              works even if the file bodies are not STAC. Your process_update_body method will
              receive the file contents for the files referred-to in the message.
            * Inherit from CatalogueChangeMessager and implement process_update and process_delete.
              Your methods will be called with the location in the S3 bucket of the inputs but
              not the file contents.
            * Inherit from PulsarJSONMessager and implement process_payload.
              This is suitable for components outside the harvest pipeline that process
              non-catalogue messages. The message format is described via Pulsar Schema
              (by writing a Python class for it) and decoded before the messager is called.

          Return an empty list if no further action is needed.

      * Consuming and producing Pulsar cataloge change messages (transformer):
          This is the same as for ingesters but, instead of returning an empty list, return a list
          of OutputFileActions. For a STAC transformer this means Messager will receive new/changed
          STAC in calls to process_update_stac, generate the transformed STAC and return
          OutputFileActions with the transformed STAC inside. The Messager framework will handle
          uploading this to S3 in the output location and sending a new message.
    """

    def __init__(
        self,
        s3_client=None,
        output_bucket=None,
        cat_output_prefix="",
        producer: pulsar.Producer = None,
    ):
        """
        s3_client should be an authenticated boto3 S3 client, such as the result of boto3.client("s3").
        output_bucket is used for all S3 operations where no bucket is specified.
        cat_output_prefix is used to derive S3 keys from catalogue paths. It's not used for S3UploadActions,
        only OutputFileAction.
        producer is used to send catalogue change messages listing the changes returned via OutputFileAction.
        It can be None if these are never returned.
        """
        self.s3_client = s3_client
        self.output_bucket = output_bucket
        self.cat_output_prefix = cat_output_prefix
        self.producer = producer

    class Action(ABC):  # noqa: B024
        """
        An Action is something that this class will do in response to a subclass's processing of
        a message.
        """

        pass

    @dataclasses.dataclass(kw_only=True)
    class S3Action(Action, ABC):
        bucket: str = None  # Defaults to messager.output_bucket
        file_body: str
        mime_type: str = "application/json"
        cache_control: str = "max-age=0"

    @dataclasses.dataclass(kw_only=True)
    class OutputFileAction(S3Action):
        """
        An OutputFileAction emits a file as a catalogue change action. Specifically:
        * If file_body is not None:
            * `file_body` is written to our output bucket in S3. The key will be
              <self.output_prefix>/<action.cat_path>. eg, this might be written to
              s3://eodhp-dev-catalogue-population/transformed/supported-datasets/ceda-stac-catalogue/
              `transformed/` is the output prefix.
            * After all other actions are completed, a catalogue change message is sent to Pulsar
              with the above key in `added_keys` or `updated_keys`.
        * If file_body is None the key defined above will be deleted from the bucket and put into
          `deleted_keys`.
        """

        cat_path: str

    @dataclasses.dataclass(kw_only=True)
    class FailureAction(Action):
        """
        This indicates a failure occured and which may have occurred processing only a particular
        catalogue change message entry.
        """

        key: str = None
        permanent: bool = True

    @dataclasses.dataclass(kw_only=True)
    class Failures:
        """
        Describes the type of errors encountered during message processing.

        If 'permanent' is True then an error occured which will definitely not be resolved through
        retries. If 'temporary' is True then an error which is potentially resolvable this way
        occured. Both can be set.

        `key_permanent` and `key_temporary` are the same but for specific keys mentioned in
        catalogue change messages.
        """

        key_permanent: list[str] = dataclasses.field(default_factory=list)
        key_temporary: list[str] = dataclasses.field(default_factory=list)
        permanent: bool = False
        temporary: bool = False

        def any_permanent(self):
            return self.permanent or self.key_permanent

        def any_temporary(self):
            return self.temporary or self.key_temporary

        def add(self, f):
            return Messager[MSGTYPE].Failures(
                key_permanent=self.key_permanent + f.key_permanent,
                key_temporary=self.key_temporary + f.key_temporary,
                permanent=self.permanent or f.permanent,
                temporary=self.temporary or f.temporary,
            )

    @dataclasses.dataclass(kw_only=True)
    class CatalogueChanges:
        added: list[str] = dataclasses.field(default_factory=list)
        updated: list[str] = dataclasses.field(default_factory=list)
        deleted: list[str] = dataclasses.field(default_factory=list)

        def add(self, other):
            return Messager.CatalogueChanges(
                added=self.added + other.added,
                updated=self.updated + other.changed,
                deleted=self.deleted + other.deleted,
            )

        def __bool__(self):
            return bool(self.added or self.updated or self.deleted)

    @dataclasses.dataclass(kw_only=True)
    class S3UploadAction(S3Action):
        """
        An S3UploadAction uploads a file to an S3 bucket at a specified key. `output_prefix` is not
        used and no Pulsar message is sent. `file_body` can be None to specify deletion.
        """

        key: str

    @abstractmethod
    def process_msg(self, msg: MSGTYPE) -> Sequence[Action]: ...

    @abstractmethod
    def gen_empty_catalogue_message(self, msg: MSGTYPE) -> dict:
        """
        This should generate a catalogue change message without updated_keys, deleted_keys or added_keys.
        """
        ...

    def gen_catalogue_message(self, msg: MSGTYPE, cat_changes: CatalogueChanges) -> dict:
        msg = self.gen_empty_catalogue_message(msg)
        msg["added_keys"] = cat_changes.added
        msg["updated_keys"] = cat_changes.updated
        msg["deleted_keys"] = cat_changes.deleted

        return msg

    def is_temporary_error(self, e: Exception):
        """
        This guesses whether an exception is a temporary or permanent error.

        Subclasses may override this to handle exceptions not handled here. This handles only
        boto and Pulsar exceptions.
        """
        return _is_boto_error_temporary(e) or _is_pulsar_error_temporary(e)

    def _runaction(self, action: Action, cat_changes: CatalogueChanges, failures: Failures):
        """
        Runs a single action. cat_changes is updated to add any catalogue changes we must publish
        as a result of them. failures is updated with any known failures.

        Exceptions may still be thrown due to bugs.
        """
        if isinstance(action, Messager.S3Action):
            bucket = action.bucket or self.output_bucket
            key = None

            try:
                if isinstance(action, Messager.OutputFileAction):
                    key = self.cat_output_prefix + action.cat_path

                    if action.file_body is None:
                        cat_changes.deleted.append(key)
                    else:
                        try:
                            self.s3_client.head_object(Bucket=bucket, Key=key)
                            cat_changes.updated.append(key)
                        except botocore.exceptions.ClientError as e:
                            # The string "404" is seen with moto.
                            if (
                                e.response["Error"]["Code"] == "NoSuchKey"
                                or e.response["Error"]["Code"] == "404"
                            ):
                                cat_changes.added.append(key)
                            else:
                                raise

                elif isinstance(action, Messager.S3UploadAction):
                    key = action.key

                if action.file_body is None:
                    self.s3_client.delete_object(Bucket=bucket, Key=key)
                    logging.info(f"Deleted {key} in {bucket}")
                else:
                    self.s3_client.put_object(
                        Body=action.file_body,
                        Bucket=bucket,
                        Key=key,
                        ContentType=action.mime_type,
                        CacheControl=action.cache_control,
                    )

                    logging.info(f"Updated/created {key} in {bucket}")
            except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
                if _is_boto_error_temporary(e):
                    failures.temporary = True
                else:
                    failures.permanent = True

                return failures
        elif isinstance(action, Messager.FailureAction):
            if action.key:
                lst = failures.key_permanent if action.permanent else failures.key_temporary
                lst.append(action.key)
            elif action.permanent:
                failures.permanent = True
            else:
                failures.temporary = True
        else:
            raise AssertionError(f"BUG: Saw unknown action type {action}")

    def consume(
        self,
        msg: MSGTYPE,
    ) -> Failures:
        """
        This consumes an input, asks the Messager (via an implementation in a task-specific
        subclass) to process it, then runs the set of actions requested by that processing.

        This returns an object specified any failures that occurred and whether retrying is
        sensible. This means it doesn't throw exceptions.
        """
        failures = Messager.Failures()

        try:
            actions = self.process_msg(msg)

            cat_changes = Messager.CatalogueChanges()
            for action in actions:
                self._runaction(action, cat_changes, failures)

            if cat_changes:
                # At least one OutputFileAction was encountered so we have to send a Pulsar catalogue
                # change message.
                change_message = self.gen_catalogue_message(msg, cat_changes)
                data = json.dumps(change_message).encode("utf-8")

                # Inject OpenTelemetry trace context into message properties
                properties = {}
                inject(properties)

                # Send Pulsar message with trace context
                self.producer.send(data, properties=properties)

                logging.debug(f"Catalogue change message sent to Pulsar : {properties}")
        except TemporaryFailure:
            logging.exception("Temporary failure processing message %s", msg)
            failures.temporary = True
        except pulsar.exceptions.PulsarException as e:
            if _is_pulsar_error_temporary(e):
                failures.temporary = True
            else:
                failures.permanent = True
        except Exception as e:
            logging.exception("Exception processing message %s", msg)
            if self.is_temporary_error(e):
                failures.temporary = True
            else:
                failures.permanent = True

        return failures

    @classmethod
    def get_schema(cls) -> Optional[Schema]:
        """
        If this returns a non-None value then the consumer used to obtain messages for this
        Messager must be registered using the Schema this returns.
        """
        return BytesSchema()


class CatalogueChangeMessager(Messager[Message], ABC):
    """
    This is an abstract Messager subclass for consuming catalogue entry change messages from
    Pulsar. This is suitable for use with transformers and ingesters.

    Subclasses should implement process_update (for updated and created keys) and
    process_delete. These will be called once for each updated/created/deleted key in the
    consumed message.
    """

    @abstractmethod
    def process_update(
        self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
    ) -> Sequence[Messager.Action]: ...

    @abstractmethod
    def process_delete(
        self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
    ) -> Sequence[Messager.Action]: ...

    def gen_empty_catalogue_message(self, msg: Message) -> dict:
        return {
            "id": self.input_change_msg.get("id"),
            "workspace": self.input_change_msg.get("workspace"),
            "bucket_name": self.output_bucket,
            "source": self.input_change_msg.get("source"),
            "target": self.input_change_msg.get("target"),
        }

    def process_msg(self, msg: Message) -> Sequence[Messager.Action]:
        """
        This processes an input catalogue change message, loops over each changed entry in it,
        asks the implementation (in a task-specific subclass) to process each one separately.
        The set of actions is then returned for the superclass to run.
        """
        harvest_schema = eodhp_utils.pulsar.messages.generate_harvest_schema()
        self.input_change_msg = eodhp_utils.pulsar.messages.get_message_data(msg, harvest_schema)
        input_change_msg = self.input_change_msg

        # Does anything need this? Maybe configure the logger with it?
        # id = input_change_msg.get("id")
        input_bucket = input_change_msg.get("bucket_name")
        source = input_change_msg.get("source")
        target = input_change_msg.get("target")

        all_actions = []
        for change_type in ("added_keys", "updated_keys", "deleted_keys"):
            for key in input_change_msg.get(change_type):
                # The key in the source bucket has format
                # "<harvest-pipeline-component>/<catalogue-path>"
                #
                # These two pieces must be separated.
                previous_step_prefix, cat_path = key.split("/", 1)

                try:
                    if change_type == "deleted_keys":
                        entry_actions = self.process_delete(
                            input_bucket,
                            key,
                            cat_path,
                            source,
                            target,
                        )
                    else:
                        # Updated or added.
                        entry_actions = self.process_update(
                            input_bucket,
                            key,
                            cat_path,
                            source,
                            target,
                        )

                    logging.debug(f"{entry_actions=}")
                    all_actions += entry_actions
                except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
                    if _is_boto_error_temporary(e):
                        logging.exception(f"Temporary Boto error for {key=}")
                        all_actions.append(Messager.FailureAction(key=key, permanent=False))
                    else:
                        logging.exception(f"Permanent Boto error for {key=}")
                        all_actions.append(Messager.FailureAction(key=key, permanent=True))
                except TemporaryFailure:
                    logging.exception(f"TemporaryFailure processing {key=}")
                    all_actions.append(Messager.FailureAction(key=key, permanent=False))
                except Exception as e:
                    logging.exception(f"Exception processing {key=}")
                    all_actions.append(
                        Messager.FailureAction(key=key, permanent=not self.is_temporary_error(e))
                    )

        return all_actions


class CatalogueChangeBodyMessager(CatalogueChangeMessager):
    """
    This extends CatalogueChangeMessager so that rather than passing buckets and keys to the
    processor subclass it passes the file contents. This only works for add/update and not
    delete.

    Subclasses should implement consume_update_body and consume_delete.
    """

    def process_update(
        self,
        input_bucket: str,
        input_key: str,
        cat_path: str,
        source: str,
        target: str,
        retries: int = 0,
    ) -> Sequence[Messager.Action]:
        try:
            get_result = self.s3_client.get_object(Bucket=input_bucket, Key=input_key)
        except botocore.exceptions.ClientError as e:
            # On some occasions we have seen Pulsar messages arrive before the file is in S3.
            if e.response["Error"]["Code"] == "NoSuchKey" and retries < 3:
                logging.warning("Key was not present, trying again")
                retries += 1
                return self.process_update(
                    input_bucket, input_key, cat_path, source, target, retries
                )
            else:
                raise
        entry_body = get_result["Body"].read()

        # Transformer needs updating to ensure that content type is set to this
        # if get_result["ResponseMetadata"]["HTTPHeaders"]["content-type"] == "application/json":
        try:
            entry_body = json.loads(entry_body)
        except ValueError:
            # Not a JSON file - consume it as a string
            logging.info(f"File {input_key} is not valid JSON.")

        return self.process_update_body(entry_body, cat_path, source, target)

    @abstractmethod
    def process_update_body(
        self, entry_body: Union[dict, str], cat_path: str, source: str, target: str
    ) -> Sequence[CatalogueChangeMessager.Action]: ...


class CatalogueSTACChangeMessager(CatalogueChangeBodyMessager, ABC):
    """
    A type of messager that ignores any updates or creations which aren't STAC. Subclasses will
    receive STAC as a dict to their process_update_stac method.

    Deletes are not affected - consume_delete must still be implemented.
    """

    def process_update_body(
        self, entry_body: Union[dict, str], cat_path: str, source: str, target: str
    ) -> Sequence[CatalogueChangeMessager.Action]:
        if not isinstance(entry_body, dict) or "stac_version" not in entry_body:
            return []

        return self.process_update_stac(entry_body, cat_path, source, target)

    @abstractmethod
    def process_update_stac(
        self,
        stac: dict,
        cat_path: str,
        source: str,
        target: str,
    ) -> Sequence[CatalogueChangeMessager.Action]: ...


class PulsarJSONMessager[PAYLOADOBJ: Record](Messager[Message], ABC):
    """
    This is an abstract Messager subclass for consuming Pulsar messages whose payload follows
    a Pulsar JSON schema defined by a Python object (PAYLOADOBJ) inheriting from
    pulsar.schema.Record.

    Subclasses should implement process_payload.
    """

    @classmethod
    def get_schema(cls) -> Schema:
        """
        This returns a Pulsar Schema for the PAYLOADOBJ type. This is what Pulsar uses to convert
        the encoded message into a Python object.
        """
        bases = typing.types.get_original_bases(cls)
        for base in bases:
            if typing.get_origin(base) == PulsarJSONMessager:
                payloadobj_class = typing.get_args(base)[0]
                return JsonSchema(payloadobj_class)

        raise ValueError("cls doesn't inherit from PulsarJSONMessager")

    @abstractmethod
    def process_payload(self, obj: PAYLOADOBJ) -> Sequence[Messager.Action]: ...

    def gen_empty_catalogue_message(self, msg):
        # This is overridden due to a design flaw in Messagers: Messager assumes that all
        # messagers are catalogue messagers rather than components that interact with Pulsar
        # for other purposes.
        raise NotImplementedError()

    def process_msg(self, msg: Message) -> Sequence[Messager.Action]:
        """
        This passes the decoded payload from the Pulsar message to the subclass's
        process_payload method.

        This relies on the schema returned by get_schema having been passed to Pulsar when
        registering the consumer.
        """
        return self.process_payload(msg.value())
