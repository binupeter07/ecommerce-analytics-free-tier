import json
import math
from collections import Counter
from pathlib import Path

import boto3


RUNS = {
    "prov_e2e_500epm": 500,
    "repeat2_500epm": 500,
    "repeat3_500epm": 500,
    "repeat2_50epm": 50,
}

SLOW_THRESHOLD_MS = 15000

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.json"

with CONFIG_PATH.open(encoding="utf-8") as f:
    CONFIG = json.load(f)

REGION = CONFIG.get("region", "eu-west-1")
S3_BUCKET = CONFIG["s3_bucket"]

s3 = boto3.client("s3", region_name=REGION)


def percentile(values, p):
    if not values:
        return None

    values = sorted(values)

    if len(values) == 1:
        return float(values[0])

    position = (len(values) - 1) * (p / 100)
    low = math.floor(position)
    high = math.ceil(position)

    if low == high:
        return float(values[low])

    weight = position - low

    return (
        values[low] * (1 - weight)
        + values[high] * weight
    )


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


def stage_times(record):
    producer_to_sqs = record.get("producer_to_sqs_ms")
    sqs_to_lambda = record.get("sqs_to_lambda_start_ms")

    lambda_start = record.get(
        "lambda_invocation_started_ms"
    )
    ddb_done = record.get(
        "ddb_write_completed_ms"
    )

    lambda_to_ddb = None

    if (
        isinstance(lambda_start, (int, float))
        and isinstance(ddb_done, (int, float))
    ):
        lambda_to_ddb = ddb_done - lambda_start

    return (
        producer_to_sqs,
        sqs_to_lambda,
        lambda_to_ddb,
    )


def analyse_run(run_id, rate):
    records = read_run(run_id)

    latencies = [
        float(record["e2e_latency_ms"])
        for record in records
        if (
            isinstance(
                record.get("e2e_latency_ms"),
                (int, float),
            )
            and record["e2e_latency_ms"] >= 0
        )
    ]

    if not latencies:
        print(f"\nNo latency data found for {run_id}")
        return

    slow_records = [
        record
        for record in records
        if (
            isinstance(
                record.get("e2e_latency_ms"),
                (int, float),
            )
            and record["e2e_latency_ms"] > SLOW_THRESHOLD_MS
        )
    ]

    valid_records = [
        record
        for record in records
        if (
            isinstance(
                record.get("e2e_latency_ms"),
                (int, float),
            )
            and record["e2e_latency_ms"] >= 0
        )
    ]

    worst = max(
        valid_records,
        key=lambda record: record["e2e_latency_ms"],
    )

    worst_p2s, worst_s2l, worst_l2d = stage_times(worst)

    slow_p2s = []
    slow_s2l = []
    slow_l2d = []

    for record in slow_records:
        p2s, s2l, l2d = stage_times(record)

        if isinstance(p2s, (int, float)):
            slow_p2s.append(p2s)

        if isinstance(s2l, (int, float)):
            slow_s2l.append(s2l)

        if isinstance(l2d, (int, float)):
            slow_l2d.append(l2d)

    slow_message_counts = Counter(
        record.get("sqs_message_id")
        for record in slow_records
        if record.get("sqs_message_id")
    )

    print("\n" + "=" * 68)
    print(f"{run_id} ({rate} events/min)")
    print("=" * 68)

    print(
        f"P95                  : "
        f"{percentile(latencies, 95) / 1000:.3f} s"
    )
    print(
        f"P99                  : "
        f"{percentile(latencies, 99) / 1000:.3f} s"
    )
    print(
        f"Maximum              : "
        f"{max(latencies) / 1000:.3f} s"
    )
    print(
        f"Events > 15 s        : "
        f"{len(slow_records):,}"
    )

    print("\nWorst event")
    print("-" * 68)

    print(
        f"End-to-end           : "
        f"{worst['e2e_latency_ms'] / 1000:.3f} s"
    )

    if worst_p2s is not None:
        print(
            f"Producer -> SQS      : "
            f"{worst_p2s / 1000:.3f} s"
        )
    else:
        print("Producer -> SQS      : N/A")

    if worst_s2l is not None:
        print(
            f"SQS -> Lambda        : "
            f"{worst_s2l / 1000:.3f} s"
        )
    else:
        print("SQS -> Lambda        : N/A")

    if worst_l2d is not None:
        print(
            f"Lambda -> DynamoDB   : "
            f"{worst_l2d / 1000:.3f} s"
        )
    else:
        print("Lambda -> DynamoDB   : N/A")

    print("\nMedian stage times for events over 15 s")
    print("-" * 68)

    if not slow_records:
        print("No events exceeded 15 seconds.")
    else:
        if slow_p2s:
            print(
                f"Producer -> SQS      : "
                f"{percentile(slow_p2s, 50) / 1000:.3f} s"
            )
        else:
            print("Producer -> SQS      : N/A")

        if slow_s2l:
            print(
                f"SQS -> Lambda        : "
                f"{percentile(slow_s2l, 50) / 1000:.3f} s"
            )
        else:
            print("SQS -> Lambda        : N/A")

        if slow_l2d:
            print(
                f"Lambda -> DynamoDB   : "
                f"{percentile(slow_l2d, 50) / 1000:.3f} s"
            )
        else:
            print("Lambda -> DynamoDB   : N/A")

    print(
        f"\nSQS messages containing slow events: "
        f"{len(slow_message_counts)}"
    )

    if slow_message_counts:
        print("Most affected messages:")

        for message_id, count in slow_message_counts.most_common(10):
            print(
                f"  {message_id}: "
                f"{count} slow events"
            )


def main():
    print("TAIL LATENCY CHECK")
    print(
        "Reading existing S3 run data. "
        "No AWS settings are changed."
    )

    for run_id, rate in RUNS.items():
        analyse_run(run_id, rate)


if __name__ == "__main__":
    main()