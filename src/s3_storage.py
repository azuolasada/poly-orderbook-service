"""Module for handling S3 storage operations."""

import asyncio
import os
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.utils.logger import logger


class S3Storage:
    """
    A class to handle S3 storage operations using boto3.

    Attributes:
        bucket (str): The name of the S3 bucket.
        client (boto3.client): The boto3 S3 client.
    """

    def __init__(self, bucket: str | None = None):
        """
        Initializes the S3Storage with a bucket name and S3 client.

        Args:
            bucket (str, optional): The name of the S3 bucket. If not provided,
                it will be retrieved from the 'S3_BUCKET' environment variable.
        """
        self.bucket = bucket or os.getenv("S3_BUCKET")
        self.client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        )

    def ensure_bucket_exists(self) -> None:
        """
        Checks if the bucket exists and creates it if it does not.

        Raises:
            ClientError: If there's an error checking or creating the bucket.
            Exception: For any other unexpected errors.
        """
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info("Bucket '%s' already exists", self.bucket)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404" or error_code == 404:
                logger.warning("Bucket '%s' not found, creating it", self.bucket)
                self._create_bucket()
            else:
                logger.error("Error checking bucket '%s': %s", self.bucket, e)
                raise
        except Exception as e:
            logger.error("Unexpected error checking bucket '%s': %s", self.bucket, e)
            raise

    def _create_bucket(self) -> None:
        """
        Creates the S3 bucket.

        Note:
            This is an internal method called by ensure_bucket_exists.
        """
        self.client.create_bucket(Bucket=self.bucket)
        logger.info("Bucket '%s' created successfully", self.bucket)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ClientError, BotoCoreError)),
        reraise=True
    )
    async def upload_bytes(self, data: bytes, key: str) -> None:
        """
        Uploads bytes to the S3 bucket.

        Args:
            data (bytes): The data to upload.
            key (str): The S3 key (file path) for the uploaded data.

        Raises:
            ClientError: If there's an error during the upload.
            BotoCoreError: If there's a boto3 related error during the upload.
        """
        try:
            await asyncio.to_thread(self.client.put_object, Bucket=self.bucket, Key=key, Body=data)
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Error uploading to S3 (key: {key}): {e}")
            raise
