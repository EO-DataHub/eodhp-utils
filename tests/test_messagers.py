import json
import sys
from argparse import Action
from typing import Sequence
from unittest.mock import Mock

import boto3
import botocore
from moto import mock_aws

from eodhp_utils.messagers import Messager, TemporaryFailure

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


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
