# services/r2_client.py
import hashlib
import mimetypes
import boto3
from botocore.exceptions import ClientError
from settings import (
    USE_R2, R2_BUCKET, R2_S3_ENDPOINT, R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY, R2_REGION, R2_PREFIX, r2_public_url
)

def get_r2_client():
    if not USE_R2:
        raise RuntimeError("R2 is not configured in settings")
    return boto3.client(
        "s3",
        endpoint_url=R2_S3_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION
    )

def generate_r2_key(file_bytes: bytes, original_filename: str = None) -> str:
    """
    Generate a stable hash-based key for deduplication.
    Example: abc123.webp
    """
    sha1 = hashlib.sha1(file_bytes).hexdigest()
    ext = mimetypes.guess_extension(mimetypes.guess_type(original_filename or "")[0] or "image/webp")
    return f"{R2_PREFIX}{sha1}{ext}"

def upload_image_to_r2(file_bytes: bytes, key: str, content_type: str) -> str:
    client = get_r2_client()
    try:
        client.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=file_bytes,
            ContentType=content_type,
            ACL="public-read"
        )
    except ClientError as e:
        raise RuntimeError(f"R2 upload failed: {e}")
    return r2_public_url(key)

def delete_image_from_r2(key: str) -> bool:
    client = get_r2_client()
    try:
        client.delete_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return False
        raise

def image_exists_in_r2(key: str) -> bool:
    """
    Returns True if the object exists in R2, False otherwise.
    """
    client = get_r2_client()
    try:
        client.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise
