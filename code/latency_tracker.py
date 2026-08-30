import argparse
import json
import math
import os
import statistics

import boto3


def load_project_defaults():
    """
    Load the AWS region and S3 bucket from config/settings.json if available.
    Otherwise, use the default values below.
    """
    region = "eu-west-1"
    bucket = "ecommerce-analytics-bucket"

    config_path = os.path.join("config", "settings.json")

    if os.path.isfile(config_path):
        try:
            with open(config_path, encoding="utf-8") as f:
                cfg = json.load(f)

            region = (
                cfg.get("region")
                or cfg.get("aws_region")
                or region
            )

            bucket = (
                cfg.get("s3_bucket")
                or cfg.get("s3_bucket_name")
                or cfg.get("bucket_name")
                or bucket
            )
        except (OSError, json.JSONDecodeError):
            pass

    return region, bucket


def percentile(values, percentile_value):
    """Calculate a percentile using linear interpolation."""
    if not values:
        return None

    ordered = sorted(values)

    if len(ordered) == 1:
        return float(ordered[0])

    rank = (len(ordered) - 1) * (percentile_value / 100.0)
    lower = math.floor(rank)
    upper = math.ceil(rank)

    if lower == upper:
        return float(ordered[lower])

    weight = rank - lower

    return (
        ordered[lower] * (1.0 - weight)
        + ordered[upper] * weight
    )


def read_latencies(s3, bucket, run_id):
    prefix = f"raw/{run_id}/"

    paginator = s3.get_paginator("list_objects_v2")

    latencies = []
    object_count = 0
    event_count = 0
    missing_latency = 0
    negative_latency = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read().decode("utf-8")
            data = json.loads(body)

            object_count += 1

            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                continue

            for record in records:
                if not isinstance(record, dict):
                    continue

                event_count += 1
                value = record.get("e2e_latency_ms")

                if not isinstance(value, (int, float)):
                    missing_latency += 1
                    continue

                value = float(value)

                if value < 0:
                    negative_latency += 1
                    continue

                latencies.append(value)

    return {
        "latencies": latencies,
        "object_count": object_count,
        "event_count": event_count,
        "missing_latency": missing_latency,
        "negative_latency": negative_latency,
    }


def main():
    default_region, default_bucket = load_project_defaults()

    parser = argparse.ArgumentParser(
        description=(
            "Read event-level end-to-end latency values "
            "from S3 and calculate latency statistics."
        )
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="Run ID, for example e2e_10epm",
    )

    parser.add_argument(
        "--bucket",
        default=default_bucket,
        help=f"S3 bucket to read from (default: {default_bucket})",
    )

    parser.add_argument(
        "--region",
        default=default_region,
        help=f"AWS region (default: {default_region})",
    )

    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)

    result = read_latencies(
        s3=s3,
        bucket=args.bucket,
        run_id=args.run_id,
    )

    latencies = result["latencies"]

    print("=" * 68)
    print("EVENT-LEVEL END-TO-END LATENCY")
    print("=" * 68)
    print(f"Run ID               : {args.run_id}")
    print(f"S3 bucket            : {args.bucket}")
    print(f"Region               : {args.region}")
    print(f"S3 objects read      : {result['object_count']}")
    print(f"Business events read : {result['event_count']}")
    print(f"Latency samples      : {len(latencies)}")
    print(f"Missing latency      : {result['missing_latency']}")
    print(f"Negative latency     : {result['negative_latency']}")

    if not latencies:
        print("\nNo e2e_latency_ms samples found.")
        return

    mean_ms = statistics.fmean(latencies)
    median_ms = statistics.median(latencies)
    stddev_ms = statistics.pstdev(latencies)

    p50_ms = percentile(latencies, 50)
    p90_ms = percentile(latencies, 90)
    p95_ms = percentile(latencies, 95)
    p99_ms = percentile(latencies, 99)

    minimum_ms = min(latencies)
    maximum_ms = max(latencies)

    print("\nLatency statistics")
    print("-" * 68)
    print(f"Mean                 : {mean_ms:,.2f} ms  ({mean_ms/1000:.3f} s)")
    print(f"Median               : {median_ms:,.2f} ms  ({median_ms/1000:.3f} s)")
    print(f"P50                  : {p50_ms:,.2f} ms  ({p50_ms/1000:.3f} s)")
    print(f"P90                  : {p90_ms:,.2f} ms  ({p90_ms/1000:.3f} s)")
    print(f"P95                  : {p95_ms:,.2f} ms  ({p95_ms/1000:.3f} s)")
    print(f"P99                  : {p99_ms:,.2f} ms  ({p99_ms/1000:.3f} s)")
    print(f"Minimum              : {minimum_ms:,.2f} ms  ({minimum_ms/1000:.3f} s)")
    print(f"Maximum              : {maximum_ms:,.2f} ms  ({maximum_ms/1000:.3f} s)")
    print(
        f"Population std. dev. : "
        f"{stddev_ms:,.2f} ms  ({stddev_ms/1000:.3f} s)"
    )

    below_15 = sum(
        1
        for value in latencies
        if value <= 15_000
    )
    below_15_pct = below_15 / len(latencies) * 100

    print("\n15-second check")
    print("-" * 68)
    print(f"Events <= 15 s       : {below_15}/{len(latencies)}")
    print(f"Share <= 15 s        : {below_15_pct:.2f}%")

    print("\nValidation")
    print("-" * 68)

    if (
        result["missing_latency"] == 0
        and result["negative_latency"] == 0
    ):
        print("Instrumentation       : PASS")
    else:
        print("Instrumentation       : CHECK REQUIRED")

    print(
        f"P90 < 15 s            : "
        f"{'YES' if p90_ms < 15_000 else 'NO'}"
    )
    print(
        f"P95 < 15 s            : "
        f"{'YES' if p95_ms < 15_000 else 'NO'}"
    )


if __name__ == "__main__":
    main()