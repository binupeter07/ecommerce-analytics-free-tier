import json
from collections import Counter, defaultdict
from pathlib import Path

import boto3


RUNS = {
    "prov_e2e_10epm": 10,
    "prov_e2e_20epm_final": 20,
    "prov_e2e_50epm": 50,
    "prov_e2e_100epm": 100,
    "prov_e2e_500epm": 500,
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.json"

with CONFIG_PATH.open(encoding="utf-8") as f:
    CONFIG = json.load(f)

REGION = CONFIG.get("region", "eu-west-1")
S3_BUCKET = CONFIG["s3_bucket"]

s3 = boto3.client("s3", region_name=REGION)


def read_run(run_id):
    prefix = f"raw/{run_id}/"
    records = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            response = s3.get_object(
                Bucket=S3_BUCKET,
                Key=key,
            )

            data = json.loads(
                response["Body"].read().decode("utf-8")
            )

            if isinstance(data, dict):
                data = [data]

            if not isinstance(data, list):
                continue

            for record in data:
                if not isinstance(record, dict):
                    continue

                record = dict(record)
                record["_s3_key"] = key
                records.append(record)

    return records


def lambda_to_ddb_ms(record):
    lambda_start = record.get(
        "lambda_invocation_started_ms"
    )
    ddb_done = record.get(
        "ddb_write_completed_ms"
    )

    if (
        isinstance(lambda_start, (int, float))
        and isinstance(ddb_done, (int, float))
    ):
        return ddb_done - lambda_start

    return None


def analyse_run(run_id, rate):
    records = read_run(run_id)

    if not records:
        print(f"\nNo records found for {run_id}")
        return

    event_ids = [
        record.get("event_id")
        for record in records
        if record.get("event_id")
    ]

    duplicate_event_ids = [
        event_id
        for event_id, count in Counter(event_ids).items()
        if count > 1
    ]

    message_to_objects = defaultdict(set)

    for record in records:
        message_id = record.get("sqs_message_id")

        if message_id:
            message_to_objects[message_id].add(
                record["_s3_key"]
            )

    repeated_messages = {
        message_id: objects
        for message_id, objects in message_to_objects.items()
        if len(objects) > 1
    }

    lambda_to_ddb_values = []

    for record in records:
        value = lambda_to_ddb_ms(record)

        if isinstance(value, (int, float)):
            lambda_to_ddb_values.append(value)

    print("\n" + "=" * 68)
    print(f"{run_id} ({rate} events/min)")
    print("=" * 68)

    print(f"Events                : {len(records):,}")
    print(
        f"Duplicate event IDs   : "
        f"{len(duplicate_event_ids)}"
    )
    print(
        f"Messages in >1 object : "
        f"{len(repeated_messages)}"
    )

    if lambda_to_ddb_values:
        print(
            f"Max Lambda -> DynamoDB: "
            f"{max(lambda_to_ddb_values) / 1000:.3f} s"
        )
    else:
        print("Max Lambda -> DynamoDB: N/A")

    print("\nRun check")
    print("-" * 68)

    if not duplicate_event_ids and not repeated_messages:
        print("Duplicate/redelivery evidence: NONE FOUND")
    else:
        print("Duplicate/redelivery evidence: CHECK REQUIRED")

    print(
        "The result describes the measured run only. "
        "It does not show that a 30-second visibility timeout "
        "is suitable for production use."
    )


def main():
    print("DELIVERY AND VISIBILITY CHECK")
    print(
        "Reading the existing S3 run data. "
        "No AWS settings are changed."
    )

    for run_id, rate in RUNS.items():
        analyse_run(run_id, rate)


if __name__ == "__main__":
    main()