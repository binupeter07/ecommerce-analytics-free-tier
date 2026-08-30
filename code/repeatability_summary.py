import json
import math
import statistics
from pathlib import Path

import boto3


RUNS = {
    10: [
        "prov_e2e_10epm",
        "repeat2_10epm",
        "repeat3_10epm",
    ],
    20: [
        "prov_e2e_20epm_final",
        "repeat2_20epm",
        "repeatt3_20epm",
    ],
    50: [
        "prov_e2e_50epm",
        "repeat2_50epm",
        "repeat3_50epm",
    ],
    100: [
        "prov_e2e_100epm",
        "repeat2_100epm",
        "repeat3_100epm",
    ],
    500: [
        "prov_e2e_500epm",
        "repeat2_500epm",
        "repeat3_500epm",
    ],
}

THRESHOLD_MS = 15000

# 95% t critical value for three runs (df = 2)
T_95_DF2 = 4.303


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


def read_latencies(run_id):
    prefix = f"raw/{run_id}/"

    latencies = []
    object_count = 0
    event_count = 0
    missing_latency = 0
    negative_latency = 0

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
        "objects": object_count,
        "events": event_count,
        "missing": missing_latency,
        "negative": negative_latency,
    }


def analyse_one_run(run_id):
    data = read_latencies(run_id)
    latencies = data["latencies"]

    if not latencies:
        raise RuntimeError(
            f"No e2e_latency_ms data found for {run_id}"
        )

    mean_ms = statistics.fmean(latencies)
    p95_ms = percentile(latencies, 95)
    p99_ms = percentile(latencies, 99)
    maximum_ms = max(latencies)

    within_15 = sum(
        1
        for value in latencies
        if value <= THRESHOLD_MS
    )

    within_15_percent = (
        within_15 / len(latencies)
    ) * 100

    return {
        "run_id": run_id,
        "objects": data["objects"],
        "events": data["events"],
        "samples": len(latencies),
        "missing": data["missing"],
        "negative": data["negative"],
        "mean_s": mean_ms / 1000,
        "p95_s": p95_ms / 1000,
        "p99_s": p99_ms / 1000,
        "max_s": maximum_ms / 1000,
        "within_15_percent": within_15_percent,
    }


def three_run_summary(values):
    mean_value = statistics.fmean(values)

    variance = statistics.variance(values)
    sd = statistics.stdev(values)

    margin = (
        T_95_DF2
        * sd
        / math.sqrt(len(values))
    )

    return {
        "mean": mean_value,
        "variance": variance,
        "sd": sd,
        "ci_low": mean_value - margin,
        "ci_high": mean_value + margin,
    }


def main():
    print("REPEATABILITY ANALYSIS")
    print(
        "Reading existing S3 run data. "
        "No AWS settings are changed.\n"
    )

    all_results = {}

    for rate, run_ids in RUNS.items():
        print("=" * 88)
        print(f"{rate} events/min")
        print("=" * 88)

        results = []

        for number, run_id in enumerate(
            run_ids,
            start=1,
        ):
            result = analyse_one_run(run_id)
            results.append(result)

            print(
                f"Run {number}: {run_id}\n"
                f"  Events       : {result['events']:,}\n"
                f"  S3 objects   : {result['objects']:,}\n"
                f"  Samples      : {result['samples']:,}\n"
                f"  Missing      : {result['missing']:,}\n"
                f"  Negative     : {result['negative']:,}\n"
                f"  Mean latency : {result['mean_s']:.3f} s\n"
                f"  P95          : {result['p95_s']:.3f} s\n"
                f"  P99          : {result['p99_s']:.3f} s\n"
                f"  Maximum      : {result['max_s']:.3f} s\n"
                f"  Within 15 s  : "
                f"{result['within_15_percent']:.2f}%\n"
            )

        all_results[rate] = results

    print("\n" + "=" * 88)
    print("RUN-TO-RUN SUMMARY")
    print("=" * 88)

    for rate, results in all_results.items():
        p95_values = [
            result["p95_s"]
            for result in results
        ]

        mean_values = [
            result["mean_s"]
            for result in results
        ]

        p95_summary = three_run_summary(
            p95_values
        )

        mean_summary = three_run_summary(
            mean_values
        )

        print(f"\n{rate} events/min")
        print("-" * 88)

        print(
            "P95 values      : "
            + ", ".join(
                f"{value:.3f}"
                for value in p95_values
            )
            + " s"
        )

        print(
            f"Mean P95        : "
            f"{p95_summary['mean']:.3f} s"
        )

        print(
            f"P95 variance    : "
            f"{p95_summary['variance']:.6f} s^2"
        )

        print(
            f"P95 run SD      : "
            f"{p95_summary['sd']:.3f} s"
        )

        print(
            "P95 95% CI      : "
            f"{p95_summary['ci_low']:.3f} to "
            f"{p95_summary['ci_high']:.3f} s"
        )

        print(
            "Mean latencies  : "
            + ", ".join(
                f"{value:.3f}"
                for value in mean_values
            )
            + " s"
        )

        print(
            f"Mean of means   : "
            f"{mean_summary['mean']:.3f} s"
        )

        print(
            f"Mean run SD     : "
            f"{mean_summary['sd']:.3f} s"
        )

        print(
            "Mean 95% CI     : "
            f"{mean_summary['ci_low']:.3f} to "
            f"{mean_summary['ci_high']:.3f} s"
        )

    print("\n" + "=" * 88)
    print("NOTES")
    print("=" * 88)

    print(
        "Run SD is calculated from the three separate "
        "runs at each workload."
    )

    print(
        "It is different from the event-level standard "
        "deviation within an individual run."
    )

    print(
        "The 95% confidence interval uses Student's t "
        "distribution with 2 degrees of freedom "
        "(t = 4.303)."
    )


if __name__ == "__main__":
    main()