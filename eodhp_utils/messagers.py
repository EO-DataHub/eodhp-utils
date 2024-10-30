# This is a prototype of how we might remove some of the duplicated Pulsar and S3 related
# functionality in the ingesters, harvesters and transformer.
import copy
import dataclasses
import json
import logging
from abc import ABC, abstractmethod
from typing import Sequence, Union

import pulsar
from boto3 import S3
from pulsar import Message

import eodhp_utils


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

    You should inherit from the correct subclass:
      * Producing Pulsar messages only (harvester):
          Inherit from Messager and implement process_msg(self, msg: <my obj>, **kwargs).
          Return a list of actions, probably OutputFileActions.

          Design your harvester to encapsulate each harvested entry as a <my obj> and call
          my_harvester.consume(my_obj).

          Write your tests to call process_msg directly.

      * Consuming Pulsar catalogue change messages only (ingester):
          Inherit from CatalogueChangeMessager and implement process_update and process_delete.
          Alternatively, use one of the subclasses defined here, such as
          CatalogueSTACChangeMessager, which will fetch the entry and ensure it's STAC before
          calling your subclass's process_stac_update method.

          Return an empty list if no further action is needed.

      * Consuming and producing Pulsar cataloge change messages (transformer):
          As for consuming Pulsar messages in the previous bullet except you should return a list
          of OutputFileActions.
    """

    def __init__(
        self, s3_client, output_bucket, output_prefix, producer: pulsar.Producer = None, **kwargs
    ):
        self._s3_client = s3_client
        self.output_bucket = output_bucket
        self.output_prefix = output_prefix
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
              with the above key in `added_keys` or `changed_keys`.
        * If file_body is None the key defined above will be deleted from the bucket and put into
          `deleted_keys`.
        """

        cat_path: str

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

        key_permanent: Sequence[str]
        key_temporary: Sequence[str]
        permanent: bool
        temporary: bool

    @dataclasses.dataclass
    class S3UploadAction(S3Action):
        """
        An S3UploadAction uploads a file to an S3 bucket at a specified key. `output_prefix` is not
        used and no Pulsar message is sent. `file_body` can be None to specify deletion.
        """

        key: str

    @abstractmethod
    def process_msg(self, msg: MSGTYPE) -> Sequence[Action]: ...

    @abstractmethod
    def gen_catalogue_message(
        self, msg: MSGTYPE, added_keys, updated_keys, deleted_keys
    ) -> dict: ...

    def consume(self, msg: MSGTYPE) -> Failures:
        """
        This consumes an input, asks the Messager (via an implementation in a task-specific
        subclass) to process it, then runs the set of actions requested by that processing.
        """
        # TODO: Catch and distinguish properly between temporary and permanent failures.
        #       Code currently in the transformer could move here to do this.
        failures = Messager.Failures([], [], False, False)

        # If any OutputFileActions are produced then we fill these in order to generate a
        # catalogue change message. They will remain empty if not.
        added_keys = []
        updated_keys = []
        deleted_keys = []

        actions = self.process_msg(msg)

        for action in actions:
            if isinstance(action, Messager.S3Action):
                bucket = action.bucket or self.output_bucket

                if isinstance(action, Messager.OutputFileAction):
                    key = self.output_prefix + "/" + action.cat_path

                    if action.file_body is None:
                        deleted_keys.append(key)
                    else:
                        try:
                            self.s3_client.head_object(Bucket=bucket, Key=key)
                            updated_keys.append(key)
                        except S3.Client.exceptions.NoSuchKey:
                            added_keys.append(key)
                elif isinstance(action, Messager.S3UploadAction):
                    key = action.key

                if action.file_body is None:
                    self.s3_client.delete_object(Bucket=bucket, Key=key)
                    logging.info(f"Deleted {key} in {bucket}")
                else:
                    self.s3_client.put_object(
                        Body=action.file_body, Bucket=bucket, Key=key, ContentType=action.mime_type
                    )

                    logging.info(f"Updated/created {key} in {bucket}")

            else:
                raise AssertionError(f"BUG: Saw unknown action type {action}")

        if added_keys or updated_keys or deleted_keys:
            # At least one OutputFileAction was encountered so we have to send a Pulsar catalogue
            # change message.
            change_message = self.gen_catalogue_message(
                added_keys=added_keys, updated_keys=updated_keys, deleted_keys=deleted_keys
            )

            try:
                data = json.dumps(change_message).encode("utf-8")
            except (ValueError, UnicodeEncodeError) as e:
                logging.error("Failed to encode message output: %e", e)
                failures.permanent = True
            else:
                self.producer.send(data)
                logging.debug("Catalogue change message sent to Pulsar")

        return failures


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

    def gen_catalogue_message(self, msg: Message, added_keys, updated_keys, deleted_keys) -> dict:
        return {
            "id": self.input_change_msg.get("id"),
            "workspace": self.input_change_msg.get("workspace"),
            "bucket_name": self.output_bucket,
            "added_keys": added_keys,
            "updated_keys": updated_keys,
            "deleted_keys": deleted_keys,
            "source": self.input_change_msg.get("source"),
            "target": self.input_change_msg.get("target"),
        }

    def consume(self, msg: Message, output_root):
        """
        This consumes an input catalogue change message, loops over each changed entry in it,
        asks the implementation (in a task-specific subclass) to process each one separately,
        then runs the set of actions requested by all of those invocations.
        """

        harvest_schema = eodhp_utils.pulsar.messages.generate_harvest_schema()
        self.input_change_msg = eodhp_utils.pulsar.messages.get_message_data(msg, harvest_schema)
        input_change_msg = self.input_change_msg

        # Does anything need this? Maybe configure the logger with it?
        # id = input_change_msg.get("id")
        input_bucket = input_change_msg.get("bucket_name")
        source = input_change_msg.get("source")
        target = input_change_msg.get("target")

        output_data = copy.deepcopy(input_change_msg)
        output_data["added_keys"] = []
        output_data["updated_keys"] = []
        output_data["deleted_keys"] = []
        output_data["failed_files"] = {
            "temp_failed_keys": {
                "updated_keys": [],
                "added_keys": [],
                "deleted_keys": [],
            },
            "perm_failed_keys": {
                "updated_keys": [],
                "added_keys": [],
                "deleted_keys": [],
            },
        }
        error_data = copy.deepcopy(output_data)

        for change_type in ("added_keys", "updated_keys", "deleted_keys"):
            for key in input_change_msg.get(change_type):
                # The key in the source bucket has format
                # "<harvest-pipeline-component>/<catalogue-path>"
                #
                # These two pieces must be separated.
                previous_step, cat_path = key.split("/", 1)

                try:
                    if change_type == "deleted_keys":
                        actions = self.process_delete(
                            input_bucket,
                            key,
                            cat_path,
                            source,
                            target,
                        )
                    else:
                        # Updated or added.
                        actions = self.process_update(
                            input_bucket,
                            key,
                            cat_path,
                            source,
                            target,
                        )

                    # TODO: Process the actions
                    logging.debug(actions)
                except eodhp_utils.pulsar.messages.URLAccessError as e:
                    logging.error(f"Unable to access key {key}: {e}")
                    error_data["failed_files"]["perm_failed_keys"][change_type].append(key)
                    continue
                except eodhp_utils.pulsar.messages.ClientError as e:
                    logging.error(f"Temporary error processing {change_type} key {key}: {e}")
                    output_data["failed_files"]["temp_failed_keys"][change_type].append(key)
                    continue
                except Exception as e:
                    logging.exception(f"Permanent error processing {change_type} key {key}: {e}")
                    output_data["failed_files"]["perm_failed_keys"][change_type].append(key)
                    continue

        return output_data, error_data


class CatalogueChangeBodyMessager(CatalogueChangeMessager):
    """
    This extends CatalogueChangeMessager to read changed files from S3 in consume_update and pass
    them to consume_update_entry_body.

    Subclasses should implement consume_update_entry_body.
    """

    def consume_update(
        self, key: str, source: str, target: str, output_root: str, bucket: str
    ) -> Sequence[CatalogueChangeMessager.Action]:
        file_body = eodhp_utils.aws.s3.get_file_s3(bucket, key, self._s3_client)

        try:
            file_body = json.loads(file_body)
        except ValueError:
            # Not a JSON file - consume it as a string
            logging.info(f"File {key} is not valid JSON.")

        return self.consume_update_file_contents(file_body)

    @abstractmethod
    def consume_update_entry_contents(
        self, file_body: Union[dict, str]
    ) -> Sequence[CatalogueChangeMessager.Action]: ...


class CatalogueSTACChangeMessager(CatalogueChangeBodyMessager, ABC):
    """
    A type of messager that ignores any updates or creations which aren't STAC.

    Deletes are not affected.

    Inherit from this and implement consume_stac_update.
    """

    def consume_update_entry_contents(
        self,
        cat_path: str,
        entry_body: Union[dict, str],
        **kwargs,
    ) -> Sequence[CatalogueChangeMessager.Action]:
        if not isinstance(entry_body, dict) or "stac_version" not in entry_body:
            return None

        return self.consume_stac_update(cat_path, entry_body, **kwargs)

    @abstractmethod
    def consume_stac_update(
        self,
        cat_path: str,
        stac: dict,
        **kwargs,
    ) -> Sequence[CatalogueChangeMessager.Action]: ...
