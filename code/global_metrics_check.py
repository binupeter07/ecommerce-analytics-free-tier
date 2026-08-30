import json
from collections import Counter
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
                if isinstance(record, dict):
                    records.append(record)

    return records


def analyse_run(run_id, rate):
    records = read_run(run_id)

    if not records:
        print(f"\nNo records found for {run_id}")
        return

    active_users = {
        record.get("user_id")
        for record in records
        if record.get("user_id") is not None
    }

    product_views = Counter(
        record.get("product_id")
        for record in records
        if (
            record.get("event_type") == "view"
            and record.get("product_id") is not None
        )
    )

    print("\n" + "=" * 68)
    print(f"{run_id} ({rate} events/min)")
    print("=" * 68)

    print(f"Events              : {len(records):,}")
    print(f"Active users        : {len(active_users):,}")
    print("Top 5 viewed products:")

    for product_id, views in product_views.most_common(5):
        print(f"  {product_id} -> {views:,} views")


def main():
    print("GLOBAL METRICS CHECK")
    print(
        "Reading archived events from S3. "
        "No AWS settings are changed."
    )

    for run_id, rate in RUNS.items():
        analyse_run(run_id, rate)


if __name__ == "__main__":
    main()