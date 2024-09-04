import logging
import os
import tempfile

import boto3
import moto
import pytest
from botocore.stub import Stubber

from eodhp_utils.aws.s3 import S3Client


@pytest.fixture
def mock_bucket_name():
    return "test_bucket"

@pytest.fixture
def s3_client():
    return S3Client()


def test_create_s3_client_with_env_vars(monkeypatch):
    # Set environment variables
    monkeypatch.setenv("AWS_ACCESS_KEY", "test_access_key")
    monkeypatch.setenv("AWS_SECRET_KEY", "test_secret_key")

    s3_client = S3Client()
    client = s3_client.create_s3_client()

    assert client._request_signer._credentials.access_key == "test_access_key"
    assert client._request_signer._credentials.secret_key == "test_secret_key"

def test_create_s3_client_without_env_vars(monkeypatch):
    # Unset environment variables
    monkeypatch.delenv("AWS_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SECRET_KEY", raising=False)

    s3_client = S3Client()
    client = s3_client.create_s3_client()

    # Check if the client is created without specific credentials
    assert client._request_signer._credentials.access_key is not None
    assert client._request_signer._credentials.secret_key is not None

def test_upload_file_s3__success(mock_bucket_name, s3_client):
    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        body = "file contents"
        file_name = "s3.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        path = f"{folder_path}/{file_name}"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=mock_bucket_name)

        with open(path, "w") as temp_file:
            temp_file.write("file contents\n")
            temp_file.flush()

        s3_client.upload_file_s3(body=body, bucket=mock_bucket_name, key=path)

        s3_resource = boto3.resource("s3")
        s3_files = list(s3_resource.Bucket(mock_bucket_name).objects.all())
        assert len(s3_files) == 1

        response = s3.get_object(Bucket=mock_bucket_name, Key=s3_files[0].key)
        file_content = response.get("Body").read().decode("utf-8")
        assert file_content == body


def test_upload_file_s3__error(s3_client, caplog, mock_bucket_name):
    s3 = boto3.client("s3")
    stubber = Stubber(s3)
    s3_client.s3_client = s3

    stubber.add_client_error(
        "put_object", service_error_code="500", service_message="Internal Server Error"
    )

    with stubber, caplog.at_level(logging.WARNING):
        s3_client.upload_file_s3(body="test_data", bucket=mock_bucket_name, key="test_key")
        assert "File upload failed" in caplog.text


def test_get_file_s3__success(s3_client, mock_bucket_name):
    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        body = "file contents"
        file_name = "s3.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        path = f"{folder_path}/{file_name}"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=mock_bucket_name)

        with open(path, "w") as temp_file:
            temp_file.write("file contents\n")
            temp_file.flush()

        s3.upload_file(path, mock_bucket_name, path)

        s3_resource = boto3.resource("s3")
        s3_files = list(s3_resource.Bucket(mock_bucket_name).objects.all())
        assert len(s3_files) == 1

        file = s3_client.get_file_s3(mock_bucket_name, path)
        assert file == body + "\n"  # a new line is added to the file


def test_get_file_s3__error(s3_client, caplog, mock_bucket_name):
    s3 = boto3.client("s3")
    stubber = Stubber(s3)
    s3_client.s3_client = s3

    stubber.add_client_error(
        "get_object", service_error_code="500", service_message="Internal Server Error"
    )

    with stubber, caplog.at_level(logging.WARNING):
        s3_client.get_file_s3(bucket=mock_bucket_name, key="test_key")
        assert "File retrieval failed" in caplog.text


def test_delete_file_s3__success(s3_client, mock_bucket_name, monkeypatch):
    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        file_name = "s3.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        path = f"{folder_path}/{file_name}"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=mock_bucket_name)

        with open(path, "w") as temp_file:
            temp_file.write("file contents\n")
            temp_file.flush()

        s3.upload_file(path, mock_bucket_name, path)

        s3_resource = boto3.resource("s3")
        s3_files = list(s3_resource.Bucket(mock_bucket_name).objects.all())
        assert len(s3_files) == 1

        s3_client.delete_file_s3(mock_bucket_name, path)

        s3_files = list(s3_resource.Bucket(mock_bucket_name).objects.all())
        assert len(s3_files) == 0


def test_delete_file_s3__error(caplog,mock_bucket_name, s3_client):
    s3 = boto3.client("s3")
    stubber = Stubber(s3)
    s3_client.s3_client = s3

    stubber.add_client_error(
        "delete_object", service_error_code="500", service_message="Internal Server Error"
    )

    with stubber, caplog.at_level(logging.WARNING):
        s3_client.delete_file_s3(bucket=mock_bucket_name, key="test_key")
        assert "File deletion failed" in caplog.text
