import json
import math
from pathlib import Path

import boto3


RUNS = {
    "prov_e2e_10epm": 10,
    "prov_e2e_20epm_final": 20,
    "prov_e2e_50epm": 50,
    "prov_e2e_100epm": 100,
    "prov_e2e_500epm": 500,
}

PACK_WINDOW_SECONDS = 13
MAX_PACK_EVENTS = 120

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
                if isinstance(record, dict):
                    records.append(record)

    return records


def analyse_run(run_id, rate):
    records = read_run(run_id)

    if not records:
        return None

    unique_messages = {
        record.get("sqs_message_id")
        for record in records
        if record.get("sqs_message_id")
    }

    message_count = len(unique_messages)

    events_per_message = (
        len(records) / message_count
        if message_count
        else 0
    )

    observed_reduction = (
        (1 - message_count / len(records)) * 100
        if message_count
        else 0
    )

    expected_pack_size = min(
        MAX_PACK_EVENTS,
        max(
            1,
            math.ceil(
                rate * PACK_WINDOW_SECONDS / 60
            ),
        ),
    )

    expected_reduction = (
        1 - 1 / expected_pack_size
    ) * 100

    return {
        "rate": rate,
        "events_per_message": events_per_message,
        "observed_reduction": observed_reduction,
        "expected_pack_size": expected_pack_size,
        "expected_reduction": expected_reduction,
    }


def main():
    print("PACKING ANALYSIS")
    print(
        "Reading existing S3 run data. "
        "No AWS settings are changed.\n"
    )

    print(
        f"{'Rate':<8}"
        f"{'Events/msg':<14}"
        f"{'Observed reduction':<22}"
        f"{'Expected reduction':<20}"
    )

    print("-" * 64)

    for run_id, rate in RUNS.items():
        result = analyse_run(run_id, rate)

        if result is None:
            print(f"{rate:<8}No data")
            continue

        print(
            f"{rate:<8}"
            f"{result['events_per_message']:<14.2f}"
            f"{result['observed_reduction']:<22.2f}%"
            f"{result['expected_reduction']:<20.2f}%"
        )


if __name__ == "__main__":
    main()