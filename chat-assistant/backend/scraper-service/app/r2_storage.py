import gzip
import aioboto3
from botocore.config import Config as BotoConfig
from .config import settings

r2_boto_config = BotoConfig(signature_version='s3v4', s3={'addressing_style': 'path'})

async def upload_to_r2(key: str, body_bytes: bytes, content_type: str ="text/html"):
  gz = gzip.compress(body_bytes)
  session = aioboto3.Session()

  async with session.client(
    "s3",
    endpoint_url=settings.r2_endpoint,
    aws_access_key_id=settings.r2_access_key_id,
    aws_secret_access_key=settings.r2_secret_access_key,
    config=r2_boto_config
  ) as client:
    await client.put_object(Bucket=settings.r2_bucket, Key=key, Body=gz, ContentType=content_type, ContentEncoding="gzip")


def r2_object_url(key: str):

  return f"{settings.r2_endpoint.rstrip('/')}/{settings.r2_bucket}/{key}"

