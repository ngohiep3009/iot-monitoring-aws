
import os
import boto3
from datetime import datetime, timezone

BUCKET_NAME = os.getenv("S3_BUCKET", "iot-monitoring-hiep-2026")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")


def get_s3_client():
    # Trên máy cá nhân: dùng profile iot-project.
    # Trên EC2: dùng IAM Role, không lưu Access Key trong code.
    profile = os.getenv("AWS_PROFILE")

    if profile:
        session = boto3.Session(
            profile_name=profile,
            region_name=AWS_REGION
        )
    else:
        session = boto3.Session(region_name=AWS_REGION)

    return session.client("s3")


def upload_csv_content(csv_content):
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    s3_key = f"exports/readings_{timestamp}.csv"

    s3 = get_s3_client()

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_content.encode("utf-8-sig"),
        ContentType="text/csv; charset=utf-8"
    )

    return {
        "bucket": BUCKET_NAME,
        "key": s3_key
    }
