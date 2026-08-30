import json
from pathlib import Path

import boto3


RUNS = {
    "prov_e2e_10epm": 10,
    "prov_e2e_20epm_final": 20,
    "prov_e2e_50epm": 50,
    "prov_e2e_100epm": 100,
    "prov_e2e_500epm": 500,
}

# Scale a 2-hour run to a 30-day month.
MONTHLY_SCALE = 360


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.json"

with CONFIG_PATH.open(encoding="utf-8") as f:
    CONFIG = json.load(f)

REGION = CONFIG.get("region", "eu-west-1")
S3_BUCKET = CONFIG["s3_bucket"]

s3 = boto3.client("s3", region_name=REGION)


def read_archive_size(run_id):
    prefix = f"raw/{run_id}/"

    object_count = 0
    total_bytes = 0

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            object_count += 1
            total_bytes += obj.get("Size", 0)

    return object_count, total_bytes


def analyse_run(run_id, rate):
    object_count, total_bytes = read_archive_size(run_id)

    two_hour_gib = total_bytes / (1024 ** 3)

    projected_puts = (
        object_count * MONTHLY_SCALE
    )

    projected_gib = (
        total_bytes * MONTHLY_SCALE
    ) / (1024 ** 3)

    print("\n" + "=" * 68)
    print(f"{run_id} ({rate} events/min)")
    print("=" * 68)

    print(
        f"2-hour object count   : "
        f"{object_count:,}"
    )
    print(
        f"2-hour archive size   : "
        f"{two_hour_gib:.4f} GiB"
    )
    print(
        f"30-day projected PUTs : "
        f"{projected_puts:,}"
    )
    print(
        f"30-day projected data : "
        f"{projected_gib:.4f} GiB"
    )


def main():
    print("S3 ARCHIVE PROJECTION")
    print(
        "The calculation uses the measured 2-hour archive "
        "data to estimate 30-day usage."
    )
    print(
        "AWS request and storage prices are applied "
        "separately.\n"
    )

    for run_id, rate in RUNS.items():
        analyse_run(run_id, rate)


if __name__ == "__main__":
    main()