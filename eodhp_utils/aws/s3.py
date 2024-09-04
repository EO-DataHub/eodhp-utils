import logging
import os

import boto3
from botocore.exceptions import ClientError


class S3Client:
    def __init__(self):
        self.s3_client = self.create_s3_client()

    def create_s3_client(self):
        if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_KEY"):
            session = boto3.session.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            )
            return session.client("s3")
        else:
            return boto3.client("s3")

    def upload_file_s3(self, body: str, bucket: str, key: str):
        """Upload data to an S3 bucket"""
        try:
            self.s3_client.put_object(Body=body, Bucket=bucket, Key=key)
        except ClientError as e:
            logging.error(f"File upload failed: {e}")

    def get_file_s3(self, bucket: str, key: str) -> str:
        """Retrieve data from an S3 bucket"""
        try:
            file_obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            return file_obj["Body"].read().decode("utf-8")
        except ClientError as e:
            logging.error(f"File retrieval failed: {e}")

    def delete_file_s3(self, bucket: str, key: str) -> str:
        """Delete file from an S3 bucket"""
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logging.error(f"File deletion failed: {e}")
