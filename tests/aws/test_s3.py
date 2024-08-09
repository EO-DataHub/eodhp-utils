import os
import tempfile

import boto3
import moto
import pytest

from eodhp_utils.aws.s3 import delete_file_s3, get_file_s3, upload_file_s3


@pytest.fixture
def bucket_name():
    return "test_bucket"


def test_upload_file_s3(bucket_name, monkeypatch):
    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        body = "file contents"
        file_name = "s3.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        path = f"{folder_path}/{file_name}"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket_name)

        with open(path, "w") as temp_file:
            temp_file.write("file contents\n")
            temp_file.flush()

        upload_file_s3(body=body, bucket=bucket_name, key=path, s3_client=s3)

        s3_resource = boto3.resource("s3")
        s3_files = list(s3_resource.Bucket(bucket_name).objects.all())
        assert len(s3_files) == 1

        response = s3.get_object(Bucket=bucket_name, Key=s3_files[0].key)
        file_content = response.get("Body").read().decode("utf-8")
        assert file_content == body


def test_get_file_s3(bucket_name):
    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        body = "file contents"
        file_name = "s3.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        path = f"{folder_path}/{file_name}"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket_name)

        with open(path, "w") as temp_file:
            temp_file.write("file contents\n")
            temp_file.flush()

        s3.upload_file(path, bucket_name, path)

        s3_resource = boto3.resource("s3")
        s3_files = list(s3_resource.Bucket(bucket_name).objects.all())
        assert len(s3_files) == 1

        file = get_file_s3(bucket_name, path, boto3.client("s3"))
        assert file == body + "\n"  # a new line is added to the file


def test_delete_file_s3(bucket_name, monkeypatch):
    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        file_name = "s3.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        path = f"{folder_path}/{file_name}"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket_name)

        with open(path, "w") as temp_file:
            temp_file.write("file contents\n")
            temp_file.flush()

        s3.upload_file(path, bucket_name, path)

        s3_resource = boto3.resource("s3")
        s3_files = list(s3_resource.Bucket(bucket_name).objects.all())
        assert len(s3_files) == 1

        delete_file_s3(bucket_name, path, boto3.client("s3"))

        s3_files = list(s3_resource.Bucket(bucket_name).objects.all())
        assert len(s3_files) == 0
