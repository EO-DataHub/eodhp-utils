from argparse import Action
from typing import Sequence

import boto3
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

        def gen_catalogue_message(self, msg: str, added_keys, updated_keys, deleted_keys) -> dict:
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

        def gen_catalogue_message(self, msg: str, added_keys, updated_keys, deleted_keys) -> dict:
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

        def gen_catalogue_message(self, msg: str, added_keys, updated_keys, deleted_keys) -> dict:
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
