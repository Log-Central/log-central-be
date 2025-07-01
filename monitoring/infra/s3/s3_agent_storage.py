import boto3
from botocore.exceptions import ClientError
from typing_extensions import override

from common.utils.singleton import Singleton
from config import settings
from monitoring.service.i_storage.i_storage_provider import IAgentStorageProvider


class S3AgentStorageProvider(IAgentStorageProvider, metaclass=Singleton):
    def __init__(self):
        self.bucket = settings.S3_BUCKET_NAME
        self.region = settings.S3_AWS_REGION
        self.client = boto3.client(
            "s3",
            region_name="ap-northeast-2",
            aws_access_key_id=settings.S3_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.S3_AWS_SECRET_ACCESS_KEY,
        )

    @override
    def get_object_key(self, resource_id: str, ts: int, filename: str) -> str:
        return f"configs/{resource_id}/{ts}/{filename}"

    @override
    def get_object_url(self, key: str) -> str:
        if self.region == "us-east-1":
            return f"https://{self.bucket}.s3.amazonaws.com/{key}"
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{key}"

    @override
    def get_base_static_url(self) -> str:
        if self.region == "us-east-1":
            return f"https://{self.bucket}.s3.amazonaws.com/harvester"
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/harvester"

    @override
    def upload(
        self, data: bytes, key: str, content_type: str = "application/octet-stream"
    ) -> str:
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=key, Body=data, ContentType=content_type
            )
        except ClientError as e:
            raise RuntimeError(f"S3 upload failed: {e}")

        return self.get_object_url(key)
