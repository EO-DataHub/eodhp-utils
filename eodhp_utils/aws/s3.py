import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError


def upload_file_s3(body: str, bucket: str, key: str, s3_client: boto3.client) -> None:
    """Upload data to an S3 bucket"""
    try:
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")


def get_file_s3(bucket: str, key: str, s3_client: boto3.client) -> Optional[str]:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"File retrieval failed: {e}")
        return None


def delete_file_s3(bucket: str, key: str, s3_client: boto3.client) -> None:
    """Delete file from an S3 bucket"""

    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File deletion failed: {e}")
