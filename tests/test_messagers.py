import json
import sys
from argparse import Action
from typing import Sequence
from unittest.mock import Mock

import boto3
import botocore
import pulsar
import pulsar.exceptions
from moto import mock_aws
from pulsar import Message

from eodhp_utils.messagers import (
    CatalogueChangeMessager,
    CatalogueSTACChangeMessager,
    Messager,
    TemporaryFailure,
)

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


def pulsar_message_from_dict(val: dict) -> Message:
    content = json.dumps(val)
    testmsg = Mock()
    testmsg.data = Mock(return_value=bytes(content, "utf-8"))

    return testmsg


def test_messages_delivered_to_messager_subclass():
    messages_received = []

    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            messages_received.append(msg)
            return []

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    testmessager = TestMessager(None, "testbucket", "testprefix")

    testmessager.consume("string-1")
    testmessager.consume("string-2")

    assert messages_received == ["string-1", "string-2"]


def test_temporary_or_permanent_failure_from_subclass_recorded_in_result():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
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


@mock_aws
def test_s3_upload_action_processed():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            return (
                Messager.S3UploadAction(file_body=b"test_body1", bucket="testbucket2", key="k1"),
                Messager.S3UploadAction(file_body=b"test_body2", key="k2"),
                Messager.S3UploadAction(file_body=b"test_body3", mime_type="x-test", key="k3"),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")
    conn.create_bucket(Bucket="testbucket2")

    testmessager = TestMessager(client, "testbucket", "testprefix/")
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=False)

    obj1 = client.get_object(Bucket="testbucket2", Key="k1")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body1"

    obj1 = client.get_object(Bucket="testbucket", Key="k2")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body2"

    obj1 = client.get_object(Bucket="testbucket", Key="k3")
    assert obj1["ContentType"] == "x-test"
    assert obj1["Body"].read() == b"test_body3"


@mock_aws
def test_s3_upload_to_nonexistent_bucket_produces_permanent_error_result():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            return (
                Messager.S3UploadAction(file_body=b"test_body1", bucket="nonexistent", key="k1"),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")

    testmessager = TestMessager(client, "testbucket", "testprefix/")
    assert testmessager.consume("") == Messager.Failures(permanent=True, temporary=False)


@mock_aws
def test_s3_timeout_error_produces_temporary_error_result():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            return (
                Messager.S3UploadAction(file_body=b"test_body1", bucket="nonexistent", key="k1"),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {}

    client = Mock()
    client.put_object.side_effect = botocore.exceptions.ConnectTimeoutError(endpoint_url="")

    testmessager = TestMessager(client, "testbucket", "testprefix/")
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=True)


@mock_aws
def test_output_file_action_catalogue_change_message_sent_and_s3_updated():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            return (
                Messager.OutputFileAction(
                    file_body=b"test_body1", bucket="testbucket2", cat_path="k1"
                ),
                Messager.OutputFileAction(file_body=b"test_body2", cat_path="k2"),
                Messager.OutputFileAction(
                    file_body=b"test_body3", mime_type="x-test", cat_path="k3"
                ),
                Messager.OutputFileAction(file_body=b"test_body4", cat_path="k4"),
                Messager.OutputFileAction(cat_path="k5", file_body=None),
            )

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {
                "id": "test",
            }

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")
    conn.create_bucket(Bucket="testbucket2")
    client.put_object(Bucket="testbucket", Key="testprefix/k4", Body="unset")
    client.put_object(Bucket="testbucket", Key="testprefix/k5", Body="unset")

    producer = Mock()

    testmessager = TestMessager(client, "testbucket", "testprefix/", producer)
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=False)

    obj1 = client.get_object(Bucket="testbucket2", Key="testprefix/k1")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body1"

    obj1 = client.get_object(Bucket="testbucket", Key="testprefix/k2")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body2"

    obj1 = client.get_object(Bucket="testbucket", Key="testprefix/k3")
    assert obj1["ContentType"] == "x-test"
    assert obj1["Body"].read() == b"test_body3"

    obj1 = client.get_object(Bucket="testbucket", Key="testprefix/k4")
    assert obj1["ContentType"] == "application/json"
    assert obj1["Body"].read() == b"test_body4"

    try:
        obj1 = client.get_object(Bucket="testbucket", Key="testprefix/k5")
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


@mock_aws
def test_output_file_action_treats_invalid_message_json_as_permanent_error():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            return (Messager.OutputFileAction(file_body=b"test_body", cat_path="k"),)

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {"id": sys.stderr}

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")

    producer = Mock()

    testmessager = TestMessager(client, "testbucket", "testprefix/", producer)
    assert testmessager.consume("") == Messager.Failures(permanent=True, temporary=False)


@mock_aws
def test_output_file_action_treats_pulsar_timeout_as_temporary_error():
    class TestMessager(Messager[str]):
        def process_msg(self, msg: str) -> Sequence[Action]:
            return (Messager.OutputFileAction(file_body=b"test_body", cat_path="k"),)

        def gen_empty_catalogue_message(self, msg: str) -> dict:
            return {"id": "test"}

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")

    producer = Mock()
    producer.send.side_effect = pulsar.exceptions.Timeout("")

    testmessager = TestMessager(client, "testbucket", "testprefix/", producer)
    assert testmessager.consume("") == Messager.Failures(permanent=False, temporary=True)


@mock_aws
def test_catalogue_change_messager_processes_individual_changes():
    class TestCatalogueChangeMessager(CatalogueChangeMessager):
        def process_update(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body=bytes(
                        f"Updated: {input_bucket=}, {input_key=}, {cat_path=}, {source=}, {target=}",
                        "utf-8",
                    ),
                    cat_path=cat_path,
                ),
            )

        def process_delete(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body=bytes(
                        f"Deleted: {input_bucket=}, {input_key=}, {cat_path=}, {source=}, {target=}",
                        "utf-8",
                    ),
                    cat_path=cat_path,
                ),
            )

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")
    client.put_object(Bucket="testbucket", Key="testprefix-out/path/k1", Body="k1")

    producer = Mock()

    testmessager = TestCatalogueChangeMessager(client, "testbucket", "testprefix-out/", producer)
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

    obj1 = client.get_object(Bucket="testbucket", Key="testprefix-out/path/k1")
    assert str(obj1["Body"].read(), "utf-8") == (
        "Updated: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path/k1', "
        + "cat_path='path/k1', "
        + "source='source-path', "
        + "target='target-path'"
    )

    obj2 = client.get_object(Bucket="testbucket", Key="testprefix-out/path2/k2")
    assert str(obj2["Body"].read(), "utf-8") == (
        "Updated: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path2/k2', "
        + "cat_path='path2/k2', "
        + "source='source-path', "
        + "target='target-path'"
    )

    obj3 = client.get_object(Bucket="testbucket", Key="testprefix-out/path/k3")
    assert str(obj3["Body"].read(), "utf-8") == (
        "Updated: input_bucket='testbucket-in', "
        + "input_key='testprefix-in/path/k3', "
        + "cat_path='path/k3', "
        + "source='source-path', "
        + "target='target-path'"
    )

    obj4 = client.get_object(Bucket="testbucket", Key="testprefix-out/path/k4")
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


@mock_aws
def test_catalogue_change_messager_aggregates_individual_failures():
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

    # conn = boto3.resource("s3")
    client = boto3.client("s3")
    # conn.create_bucket(Bucket="testbucket")
    # client.put_object(Bucket="testbucket", Key="testprefix-out/path/k1", Body="k1")

    producer = Mock()

    testmessager = TestCatalogueChangeMessager(client, "testbucket", "testprefix-out/", producer)
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


@mock_aws
def test_stac_change_messager_processes_only_stac():
    class TestCatalogueChangeMessager(CatalogueSTACChangeMessager):
        def process_update_stac(
            self, stac: dict, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return (
                Messager.OutputFileAction(
                    file_body=bytes(
                        f"STAC: {stac.get('id')=}, {cat_path=}, {source=}, {target=}",
                        "utf-8",
                    ),
                    cat_path=cat_path,
                ),
            )

        def process_delete(
            self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
        ) -> Sequence[Messager.Action]:
            return []

    conn = boto3.resource("s3")
    client = boto3.client("s3")
    conn.create_bucket(Bucket="testbucket")

    client.put_object(
        Bucket="testbucket",
        Key="testprefix-in/path/k1",
        ContentType="application/json",
        Body=b"notjson",
    )

    client.put_object(
        Bucket="testbucket", Key="testprefix-in/path/k2", ContentType="text/plain", Body=b"notjson"
    )

    client.put_object(
        Bucket="testbucket",
        Key="testprefix-in/path/k3",
        ContentType="application/json",
        Body=b'{"not": "stac"}',
    )

    client.put_object(
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

    testmessager = TestCatalogueChangeMessager(client, "testbucket", "testprefix-out/")
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
                b"STAC: stac.get('id')='sentinel2_ard', "
                + b"cat_path='path/k4', "
                + b"source='source-path', "
                + b"target='target-path'"
            ),
            cat_path="path/k4",
        ),
    ]