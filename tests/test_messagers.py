import json
import sys
from typing import Sequence
from unittest.mock import Mock

import boto3
import botocore
import moto
import pulsar
import pulsar.exceptions
import pytest
from pulsar import Message

from eodhp_utils.messagers import (
    CatalogueChangeMessager,
    CatalogueSTACChangeMessager,
    Messager,
    PulsarJSONMessager,
    TemporaryFailure,
)
from eodhp_utils.pulsar.messages import BillingEvent

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


@pytest.fixture
def s3_client():
    # See https://github.com/getmoto/moto/issues/1568 for some details on the AWS mocks.
    #
    # This must be a context manager (no @mock_aws annotation), this fixture must yield and not
    # return and the tests shouldn't have @mock_aws themselves (although this seems to work now).
    with moto.mock_aws():
        conn = boto3.resource("s3")
        client = boto3.client("s3")
        cbconfig = {
            "LocationConstraint": "eu-west-2",
        }
        conn.create_bucket(
            Bucket="testbucket",
            CreateBucketConfiguration=cbconfig,
        )
        conn.create_bucket(
            Bucket="testbucket2",
            CreateBucketConfiguration=cbconfig,
        )

        yield client


def pulsar_message_from_dict(val: dict) -> Message:
    content = json.dumps(val)
    testmsg = Mock()
    testmsg.data = Mock(return_value=bytes(content, "utf-8"))

    return testmsg


def test_messages_delivered_to_messager_subclass():
    messages_received = []

    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            messages_received.append(msg)
            return []

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    testmessager = TestMessager(None, "testbucket", "testprefix")

    testmessager.consume("string-1")
    testmessager.consume("string-2")

    assert messages_received == ["string-1", "string-2"]


def test_temporary_or_permanent_failure_from_subclass_recorded_in_result():
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            if msg == "temp":
                raise TemporaryFailure("process_msg")
            elif msg == "perm":
                raise Exception("process_msg")
            else:
                return []

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    testmessager = TestMessager(None, "testbucket", "testprefix")

    assert testmessager.consume("temp") == Messager.Failures(permanent=False, temporary=True)
    assert testmessager.consume("perm") == Messager.Failures(permanent=True, temporary=False)
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=False)


def test_s3_upload_action_processed(s3_client):
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            return (
                Messager.S3UploadAction(file_body="test_body1", bucket="testbucket2", key="k1"),
                Messager.S3UploadAction(file_body="test_body2", key="k2"),
                Messager.S3UploadAction(file_body="test_body3", mime_type="x-test", key="k3"),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    testmessager = TestMessager(s3_client, "testbucket", "testprefix/")
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=False)

    obj1 = s3_client.get_object(Bucket="testbucket2", Key="k1")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body1"

    obj1 = s3_client.get_object(Bucket="testbucket", Key="k2")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body2"

    obj1 = s3_client.get_object(Bucket="testbucket", Key="k3")
    assert obj1["ContentType"] == "x-test"
    assert obj1["Body"].read() == b"test_body3"


def test_s3_upload_to_nonexistent_bucket_produces_permanent_error_result(s3_client):
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            return (
                Messager.S3UploadAction(file_body="test_body1", bucket="nonexistent", key="k1"),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    testmessager = TestMessager(s3_client, "testbucket", "testprefix/")
    assert testmessager.consume("") == Messager.Failures(permanent=True, temporary=False)


def test_s3_timeout_error_produces_temporary_error_result():
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            return (
                Messager.S3UploadAction(file_body="test_body1", bucket="nonexistent", key="k1"),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    client = Mock()
    client.put_object.side_effect = botocore.exceptions.ConnectTimeoutError(endpoint_url="")

    testmessager = TestMessager(client, "testbucket", "testprefix/")
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=True)


def test_output_file_action_catalogue_change_message_sent_and_s3_updated(s3_client):
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body="test_body1", bucket="testbucket2", cat_path="k1"
                ),
                Messager.OutputFileAction(file_body="test_body2", cat_path="k2"),
                Messager.OutputFileAction(
                    file_body="test_body3", mime_type="x-test", cat_path="k3"
                ),
                Messager.OutputFileAction(file_body="test_body4", cat_path="k4"),
                Messager.OutputFileAction(cat_path="k5", file_body=None),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {
                "id": "test",
            }

    s3_client.put_object(Bucket="testbucket", Key="testprefix/k4", Body="unset")
    s3_client.put_object(Bucket="testbucket", Key="testprefix/k5", Body="unset")

    producer = Mock()

    testmessager = TestMessager(s3_client, "testbucket", "testprefix/", producer)
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=False)

    obj1 = s3_client.get_object(Bucket="testbucket2", Key="testprefix/k1")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body1"

    obj1 = s3_client.get_object(Bucket="testbucket", Key="testprefix/k2")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body2"

    obj1 = s3_client.get_object(Bucket="testbucket", Key="testprefix/k3")
    assert obj1["ContentType"] == "x-test"
    assert obj1["Body"].read() == b"test_body3"

    obj1 = s3_client.get_object(Bucket="testbucket", Key="testprefix/k4")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body4"

    try:
        obj1 = s3_client.get_object(Bucket="testbucket", Key="testprefix/k5")
    except botocore.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] == "NoSuchKey" or e.response["Error"]["Code"] == "404"

    message = producer.send.call_args.args[0]
    sys.stderr.write(f"{message=}")

    assert json.loads(message) == {
        "id": "test",
        "added_keys": ["testprefix/k1", "testprefix/k2", "testprefix/k3"],
        "updated_keys": ["testprefix/k4"],
        "deleted_keys": ["testprefix/k5"],
    }


def test_output_file_action_treats_invalid_message_json_as_permanent_error(s3_client):
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            return (Messager.OutputFileAction(file_body="test_body", cat_path="k"),)

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {"id": sys.stderr}

    producer = Mock()

    testmessager = TestMessager(s3_client, "testbucket", "testprefix/", producer)
    assert testmessager.consume("") == Messager.Failures(permanent=True, temporary=False)


def test_output_file_action_treats_pulsar_timeout_as_temporary_error(s3_client):
    class TestMessager(Messager[str, bytes]):
        def process_msg(self, msg: str) -> Sequence[Messager.Action]:
            return (Messager.OutputFileAction(file_body="test_body", cat_path="k"),)

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {"id": "test"}

    producer = Mock()
    producer.send.side_effect = pulsar.exceptions.Timeout("")

    testmessager = TestMessager(s3_client, "testbucket", "testprefix/", producer)
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=True)


def test_adding_changes_to_cataloguechanges_object_results_in_new_object_with_all_changes():
    changes1 = Messager[bytes, bytes].CatalogueChanges(
        added=["a", "b"], updated=["c", "d"], deleted=["e", "f"]
    )
    changes2 = Messager[bytes, bytes].CatalogueChanges(
        added=["1", "2"], updated=["3", "4"], deleted=["5", "6"]
    )

    assert changes1.add(changes2) == Messager[bytes, bytes].CatalogueChanges(
        added=["a", "b", "1", "2"], updated=["c", "d", "3", "4"], deleted=["e", "f", "5", "6"]
    )


@pytest.mark.parametrize(
    "added,updated,deleted,expected",
    [
        pytest.param([], [], [], False),
        pytest.param([], [], [], False),
        pytest.param([], [], [], False),
        pytest.param([], [], [], False),
        pytest.param([], [], [], False),
    ],
)
def test_cataloguechanges_evaluates_to_true_only_if_change_is_present(
    added, updated, deleted, expected
):
    changes = Messager[bytes, bytes].CatalogueChanges(added=added, updated=updated, deleted=deleted)
    assert bool(changes) == expected


def test_catalogue_change_messager_processes_individual_changes(s3_client):
    class TestCatalogueChangeMessager(CatalogueChangeMessager):
        def process_update(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body=(
                        f"Updated: {input_bucket=}, {input_key=}, {cat_path=}, {source=}, {target=}"
                    ),
                    cat_path=cat_path,
                ),
            )

        def process_delete(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body=(
                        f"Deleted: {input_bucket=}, {input_key=}, {cat_path=}, {source=}, {target=}"
                    ),
                    cat_path=cat_path,
                ),
            )

    s3_client.put_object(Bucket="testbucket", Key="testprefix-out/path/k1", Body="k1")

    producer = Mock()

    testmessager = TestCatalogueChangeMessager(s3_client, "testbucket", "testprefix-out/", producer)
    testmsg = pulsar_message_from_dict(
        {
            "id": "harvest-source-id",
            "workspace": "workspace-id",
            "bucket_name": "testbucket-in",
            "source": "source-path",
            "target": "target-path",
            "updated_keys": ["testprefix-in/path/k1", "testprefix-in/path2/k2"],
            "added_keys": [
                "testprefix-in/path/k3",
            ],
            "deleted_keys": [
                "testprefix-in/path/k4",
            ],
        }
    )

    failures = testmessager.consume(testmsg)
    assert failures == Messager.Failures(permanent=False, temporary=False)

    obj1 = s3_client.get_object(Bucket="testbucket", Key="testprefix-out/path/k1")
    assert str(obj1["Body"].read(), "utf-8") == (
        "Updated: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path/k1', "
        + "cat_path='path/k1', "
        + "source='source-path', "
        + "target='target-path'"
    )

    obj2 = s3_client.get_object(Bucket="testbucket", Key="testprefix-out/path2/k2")
    assert str(obj2["Body"].read(), "utf-8") == (
        "Updated: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path2/k2', "
        + "cat_path='path2/k2', "
        + "source='source-path', "
        + "target='target-path'"
    )

    obj3 = s3_client.get_object(Bucket="testbucket", Key="testprefix-out/path/k3")
    assert str(obj3["Body"].read(), "utf-8") == (
        "Updated: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path/k3', "
        + "cat_path='path/k3', "
        + "source='source-path', "
        + "target='target-path'"
    )

    obj4 = s3_client.get_object(Bucket="testbucket", Key="testprefix-out/path/k4")
    assert str(obj4["Body"].read(), "utf-8") == (
        "Deleted: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path/k4', "
        + "cat_path='path/k4', "
        + "source='source-path', "
        + "target='target-path'"
    )

    message = producer.send.call_args.args[0]
    sys.stderr.write(f"{message=}")

    assert json.loads(message) == {
        "id": "harvest-source-id",
        "workspace": "workspace-id",
        "bucket_name": "testbucket",
        "source": "source-path",
        "target": "target-path",
        # Note: these depend on the actions returned by process_* and the existing bucket contents.
        #       They're not expected to match the incoming message.
        "updated_keys": [
            "testprefix-out/path/k1",
        ],
        "added_keys": [
            "testprefix-out/path/k3",
            "testprefix-out/path2/k2",
            "testprefix-out/path/k4",
        ],
        "deleted_keys": [],
    }


def test_catalogue_change_messager_aggregates_individual_failures(s3_client):
    class TestCatalogueChangeMessager(CatalogueChangeMessager):
        def process_update(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            if cat_path.endswith("permerror"):
                raise ValueError("test")

            if cat_path.endswith("temperror"):
                raise TemporaryFailure()

            return []

        def process_delete(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return []

    producer = Mock()

    testmessager = TestCatalogueChangeMessager(s3_client, "testbucket", "testprefix-out/", producer)
    testmsg = pulsar_message_from_dict(
        {
            "id": "harvest-source-id",
            "workspace": "workspace-id",
            "bucket_name": "testbucket-in",
            "source": "source-path",
            "target": "target-path",
            "updated_keys": [
                "testprefix-in/path/permerror",
                "testprefix-in/path2/permerror",
                "testprefix-in/path/temperror",
                "testprefix-in/noerror",
            ],
            "added_keys": [],
            "deleted_keys": [],
        }
    )

    failures = testmessager.consume(testmsg)
    assert failures == Messager.Failures(
        permanent=False,
        temporary=False,
        key_permanent=["testprefix-in/path/permerror", "testprefix-in/path2/permerror"],
        key_temporary=["testprefix-in/path/temperror"],
    )


def test_stac_change_messager_processes_only_stac(s3_client):
    class TestCatalogueChangeMessager(CatalogueSTACChangeMessager):
        def process_update_stac(
            self, stac: dict, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body=(f"STAC: {stac.get('id')=}, {cat_path=}, {source=}, {target=}"),
                    cat_path=cat_path,
                ),
            )

        def process_delete(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return []

    s3_client.put_object(
        Bucket="testbucket",
        Key="testprefix-in/path/k1",
        ContentType="application/json",
        Body=b"notjson",
    )

    s3_client.put_object(
        Bucket="testbucket", Key="testprefix-in/path/k2", ContentType="text/plain", Body=b"notjson"
    )

    s3_client.put_object(
        Bucket="testbucket",
        Key="testprefix-in/path/k3",
        ContentType="application/json",
        Body=b'{"not": "stac"}',
    )

    s3_client.put_object(
        Bucket="testbucket",
        Key="testprefix-in/path/k4",
        ContentType="application/json",
        Body=json.dumps(
            {
                "type": "Collection",
                "stac_version": "1.0.0",
                "stac_extensions": [],
                "id": "sentinel2_ard",
            }
        ),
    )

    testmessager = TestCatalogueChangeMessager(s3_client, "testbucket", "testprefix-out/")
    testmsg = pulsar_message_from_dict(
        {
            "id": "harvest-source-id",
            "workspace": "workspace-id",
            "bucket_name": "testbucket",
            "source": "source-path",
            "target": "target-path",
            "updated_keys": ["testprefix-in/path/k1", "testprefix-in/path/k2"],
            "added_keys": [
                "testprefix-in/path/k3",
                "testprefix-in/path/k4",
            ],
            "deleted_keys": [
                "testprefix-in/path/k5",
            ],
        }
    )

    actions = testmessager.process_msg(testmsg)
    assert actions == [
        Messager.OutputFileAction(
            file_body=(
                "STAC: stac.get('id')='sentinel2_ard', "
                + "cat_path='path/k4', "
                + "source='source-path', "
                + "target='target-path'"
            ),
            cat_path="path/k4",
        ),
    ]


@pytest.fixture
def fake_billingevent():
    return BillingEvent.get_fake()


def test_pulsarjsonmessager_decodes_billingevent_message_correctly(fake_billingevent):

    class TestJSONMessager(PulsarJSONMessager[BillingEvent, bytes]):
        billingevents_received = []

        def process_payload(self, be):
            self.billingevents_received.append(be)
            print(f"{be=}")
            return []

    schema = TestJSONMessager.get_schema()

    # This tests a round trip with the Schema without testing the message reception.
    enced = schema.encode(fake_billingevent)
    decd = schema.decode(enced)
    assert decd == fake_billingevent

    # This is a bit more dodgy because it knows about Pulsar client library internals.
    # 'msg' is a fake Pulsar message on which .value() will work.
    testmsg = Mock()
    testmsg.data = Mock(return_value=schema.encode(fake_billingevent))
    msg = Message._wrap(testmsg)
    msg._schema = schema

    testmessager = TestJSONMessager(None, None)
    testmessager.consume(msg)
    assert testmessager.billingevents_received == [fake_billingevent]
