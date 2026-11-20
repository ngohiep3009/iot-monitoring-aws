import os
import time
import json
import boto3
from database import init_db, save_reading

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
NORMAL_QUEUE_URL = os.getenv("NORMAL_QUEUE_URL")

sqs_client = boto3.client("sqs", region_name=AWS_REGION)


def run_worker():
    init_db()
    print("IoT SQS Worker started...", flush=True)

    while True:
        if not NORMAL_QUEUE_URL:
            print("NORMAL_QUEUE_URL is not set. Waiting 5s...", flush=True)
            time.sleep(5)
            continue

        try:
            response = sqs_client.receive_message(
                QueueUrl=NORMAL_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            messages = response.get("Messages", [])

            for msg in messages:
                reading = json.loads(msg["Body"])
                save_reading(reading)

                sqs_client.delete_message(
                    QueueUrl=NORMAL_QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )
        except Exception as exc:
            print(f"Worker error: {exc}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    run_worker()