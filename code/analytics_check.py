import json
import math
from collections import Counter, defaultdict
from pathlib import Path

import boto3


# ---------------------------------------------------------------------
# 1. Final experiment runs
# ---------------------------------------------------------------------

RUNS = {
    "prov_e2e_10epm": 10,
    "prov_e2e_20epm_final": 20,
    "prov_e2e_50epm": 50,
    "prov_e2e_100epm": 100,
    "prov_e2e_500epm": 500,
}

PACK_WINDOW_SECONDS = 13
MAX_PACK_EVENTS = 120

# 30 days = 720 hours.
# Each experiment represents 2 hours.
# 720 / 2 = 360.
MONTHLY_SCALE = 360

# Any event above 15 seconds is treated as a slow event.
SLOW_THRESHOLD_MS = 15000


# ---------------------------------------------------------------------
# 2. Read AWS settings
# ---------------------------------------------------------------------

# analytics_check.py is inside the code folder.
# parent = code
# parent.parent = project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "settings.json"
)

with CONFIG_PATH.open(encoding="utf-8") as f:
    CONFIG = json.load(f)

REGION = CONFIG.get(
    "region",
    "eu-west-1",
)

S3_BUCKET = CONFIG["s3_bucket"]

s3 = boto3.client(
    "s3",
    region_name=REGION,
)


# ---------------------------------------------------------------------
# 3. Percentile helper
# ---------------------------------------------------------------------

def percentile(values, p):

    if not values:
        return None

    # Percentiles require values to be ordered.
    values = sorted(values)

    if len(values) == 1:
        return float(values[0])

    # Example:
    # 60,000 events and P95:
    #
    # position =
    # (60000 - 1) * 0.95
    # = 56999.05
    position = (
        (len(values) - 1)
        * (p / 100)
    )

    low = math.floor(position)
    high = math.ceil(position)

    # If the percentile falls exactly on one value,
    # simply return that value.
    if low == high:
        return float(values[low])

    # Otherwise find how far the percentile lies
    # between the lower and upper values.
    weight = position - low

    return (
        values[low] * (1 - weight)
        + values[high] * weight
    )


# ---------------------------------------------------------------------
# 4. Read one experiment run from S3
# ---------------------------------------------------------------------

def read_run(run_id):

    # Example:
    # raw/prov_e2e_500epm/
    prefix = f"raw/{run_id}/"

    # All business-event records will be stored here.
    records = []

    # Number of S3 JSON objects for the run.
    object_count = 0

    # Combined size of all objects.
    total_bytes = 0

    # S3 may return files in multiple pages.
    # The paginator makes sure we read every page.
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    ):

        for obj in page.get(
            "Contents",
            [],
        ):

            key = obj["Key"]

            # Ignore folder-like entries.
            if key.endswith("/"):
                continue

            object_count += 1

            total_bytes += obj.get(
                "Size",
                0,
            )

            # Download this S3 JSON object.
            response = s3.get_object(
                Bucket=S3_BUCKET,
                Key=key,
            )

            # S3 returns bytes.
            # Decode them to text,
            # then convert JSON to Python.
            data = json.loads(
                response[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )

            # If there is only one dictionary,
            # convert it into a list containing
            # one dictionary.
            if isinstance(
                data,
                dict,
            ):
                data = [data]

            # Ignore unexpected formats.
            if not isinstance(
                data,
                list,
            ):
                continue

            # Read every business event
            # inside this S3 object.
            for record in data:

                if not isinstance(
                    record,
                    dict,
                ):
                    continue

                # Copy the record before adding
                # analysis-only information.
                record = dict(record)

                # Remember which S3 object this
                # event came from.
                #
                # This helps detect whether the same
                # SQS message appeared in more than
                # one Lambda/S3 object.
                record["_s3_key"] = key

                records.append(record)

    return (
        records,
        object_count,
        total_bytes,
    )


# ---------------------------------------------------------------------
# 5. Split one event's latency into stages
# ---------------------------------------------------------------------

def stage_times(record):

    # Event enters producer
    #       ->
    # SQS message receives SentTimestamp
    producer_to_sqs = record.get(
        "producer_to_sqs_ms"
    )

    # SQS SentTimestamp
    #       ->
    # Lambda invocation starts
    sqs_to_lambda = record.get(
        "sqs_to_lambda_start_ms"
    )

    # Absolute Lambda start timestamp
    lambda_start = record.get(
        "lambda_invocation_started_ms"
    )

    # Absolute DynamoDB-completion timestamp
    ddb_done = record.get(
        "ddb_write_completed_ms"
    )

    lambda_to_ddb = None

    # Only subtract timestamps if both
    # are valid numbers.
    if (
        isinstance(
            lambda_start,
            (int, float),
        )
        and isinstance(
            ddb_done,
            (int, float),
        )
    ):
        lambda_to_ddb = (
            ddb_done
            - lambda_start
        )

    return (
        producer_to_sqs,
        sqs_to_lambda,
        lambda_to_ddb,
    )


# ---------------------------------------------------------------------
# 6. Analyse one experiment run
# ---------------------------------------------------------------------

def analyse_run(
    run_id,
    rate,
):

    # -------------------------------------------------------------
    # Load the complete run
    # -------------------------------------------------------------

    (
        records,
        object_count,
        total_bytes,
    ) = read_run(run_id)

    # -------------------------------------------------------------
    # Build the latency list
    # -------------------------------------------------------------

    latencies = [
        float(
            r["e2e_latency_ms"]
        )
        for r in records
        if isinstance(
            r.get(
                "e2e_latency_ms"
            ),
            (int, float),
        )
    ]

    if not latencies:

        print(
            f"\nNo latency data "
            f"found for {run_id}"
        )

        return

    # -------------------------------------------------------------
    # Find every event above 15 seconds
    # -------------------------------------------------------------

    slow_records = [
        r
        for r in records
        if (
            isinstance(
                r.get(
                    "e2e_latency_ms"
                ),
                (int, float),
            )
            and r[
                "e2e_latency_ms"
            ] > SLOW_THRESHOLD_MS
        )
    ]

    # -------------------------------------------------------------
    # Find the single worst latency event
    # -------------------------------------------------------------

    worst = max(
        records,
        key=lambda r: r.get(
            "e2e_latency_ms",
            -1,
        ),
    )

    (
        worst_p2s,
        worst_s2l,
        worst_l2d,
    ) = stage_times(worst)


    # -------------------------------------------------------------
    # Duplicate / redelivery evidence
    # -------------------------------------------------------------

    event_ids = [
        r.get("event_id")
        for r in records
        if r.get("event_id")
    ]

    # Find event IDs that appear more than once.
    duplicate_event_ids = [
        event_id
        for (
            event_id,
            count,
        ) in Counter(
            event_ids
        ).items()
        if count > 1
    ]

    # Map:
    #
    # SQS message ID
    #        ->
    # set of S3 objects containing it
    message_to_objects = defaultdict(
        set
    )

    for r in records:

        message_id = r.get(
            "sqs_message_id"
        )

        if message_id:

            message_to_objects[
                message_id
            ].add(
                r["_s3_key"]
            )

    # Same message ID appearing inside
    # one object is normal because one
    # packed SQS message contains many events.
    #
    # We care about the same message ID
    # appearing in DIFFERENT objects.
    repeated_messages = {
        message_id: objects
        for (
            message_id,
            objects,
        ) in message_to_objects.items()
        if len(objects) > 1
    }

    unique_messages = len(
        message_to_objects
    )


    # -------------------------------------------------------------
    # Packing benefit
    # -------------------------------------------------------------

    observed_events_per_message = (
        len(records)
        / unique_messages
        if unique_messages
        else 0
    )

    # Compare against:
    # one business event per SQS message.
    observed_reduction = (
        (
            1
            - unique_messages
            / len(records)
        )
        * 100
        if (
            records
            and unique_messages
        )
        else 0
    )

    # Analytical estimate based on
    # arrival rate and 13-second age bound.
    #
    # Example:
    # 500 events/min:
    #
    # 500 * 13 / 60
    # = 108.33
    # -> approximately 109 events.
    expected_pack_size = min(
        MAX_PACK_EVENTS,
        max(
            1,
            math.ceil(
                rate
                * PACK_WINDOW_SECONDS
                / 60
            ),
        ),
    )

    expected_reduction = (
        1
        - 1 / expected_pack_size
    ) * 100


    # -------------------------------------------------------------
    # Exact global reference metrics
    # -------------------------------------------------------------

    # A set automatically removes duplicate users.
    active_users = {
        r.get("user_id")
        for r in records
        if (
            r.get("user_id")
            is not None
        )
    }

    # Count views for every product.
    product_views = Counter(
        r.get("product_id")
        for r in records
        if (
            r.get("event_type")
            == "view"
            and r.get(
                "product_id"
            )
            is not None
        )
    )


    # -------------------------------------------------------------
    # Lambda -> DynamoDB times for every event
    # -------------------------------------------------------------

    lambda_to_ddb_values = []

    for r in records:

        (
            _,
            _,
            value,
        ) = stage_times(r)

        if isinstance(
            value,
            (int, float),
        ):
            lambda_to_ddb_values.append(
                value
            )


    # -------------------------------------------------------------
    # Stage timings for ALL slow events
    # -------------------------------------------------------------

    slow_p2s = []
    slow_s2l = []
    slow_l2d = []

    for r in slow_records:

        (
            p2s,
            s2l,
            l2d,
        ) = stage_times(r)

        if isinstance(
            p2s,
            (int, float),
        ):
            slow_p2s.append(
                p2s
            )

        if isinstance(
            s2l,
            (int, float),
        ):
            slow_s2l.append(
                s2l
            )

        if isinstance(
            l2d,
            (int, float),
        ):
            slow_l2d.append(
                l2d
            )


    # -------------------------------------------------------------
    # Find which SQS messages contained slow events
    # -------------------------------------------------------------

    slow_message_ids = [
        r.get(
            "sqs_message_id"
        )
        for r in slow_records
        if r.get(
            "sqs_message_id"
        )
    ]

    slow_message_counts = Counter(
        slow_message_ids
    )


    # -------------------------------------------------------------
    # Print results
    # -------------------------------------------------------------

    print(
        "\n"
        + "=" * 72
    )

    print(
        f"{run_id}   "
        f"({rate} events/min)"
    )

    print(
        "=" * 72
    )


    # -------------------------------------------------------------
    # A. Basic result
    # -------------------------------------------------------------

    print(
        "\nA. Basic result"
    )

    print(
        f"Events                : "
        f"{len(records):,}"
    )

    print(
        f"S3 objects            : "
        f"{object_count:,}"
    )

    print(
        f"P95                   : "
        f"{percentile(latencies, 95)/1000:.3f} s"
    )

    print(
        f"P99                   : "
        f"{percentile(latencies, 99)/1000:.3f} s"
    )

    print(
        f"Maximum               : "
        f"{max(latencies)/1000:.3f} s"
    )

    print(
        f"Events > 15 s         : "
        f"{len(slow_records):,}"
    )


    # -------------------------------------------------------------
    # B. Worst event
    # -------------------------------------------------------------

    print(
        "\nB. Worst-event latency split"
    )

    print(
        f"End-to-end            : "
        f"{worst['e2e_latency_ms']/1000:.3f} s"
    )

    if worst_p2s is not None:

        print(
            f"Producer -> SQS       : "
            f"{worst_p2s/1000:.3f} s"
        )

    else:

        print(
            "Producer -> SQS       : N/A"
        )

    if worst_s2l is not None:

        print(
            f"SQS -> Lambda start   : "
            f"{worst_s2l/1000:.3f} s"
        )

    else:

        print(
            "SQS -> Lambda start   : N/A"
        )

    if worst_l2d is not None:

        print(
            f"Lambda -> DynamoDB    : "
            f"{worst_l2d/1000:.3f} s"
        )

    else:

        print(
            "Lambda -> DynamoDB    : N/A"
        )


    # -------------------------------------------------------------
    # C. All slow events
    # -------------------------------------------------------------

    print(
        "\nC. All slow events (>15 s)"
    )

    if slow_records:

        if slow_p2s:

            print(
                f"Producer->SQS median  : "
                f"{percentile(slow_p2s, 50)/1000:.3f} s"
            )

        if slow_s2l:

            print(
                f"SQS->Lambda median    : "
                f"{percentile(slow_s2l, 50)/1000:.3f} s"
            )

        if slow_l2d:

            print(
                f"Lambda->DDB median    : "
                f"{percentile(slow_l2d, 50)/1000:.3f} s"
            )

    else:

        print(
            "No events exceeded 15 seconds."
        )


    # -------------------------------------------------------------
    # C.1 Slow-event message clustering
    # -------------------------------------------------------------

    print(
        "\nC.1 Slow-event message clustering"
    )

    print(
        "Distinct SQS messages with slow events:",
        len(
            slow_message_counts
        ),
    )

    if slow_message_counts:

        print(
            "Top affected messages:"
        )

        for (
            message_id,
            count,
        ) in slow_message_counts.most_common(
            10
        ):

            print(
                message_id,
                ":",
                count,
                "slow events",
            )

    else:

        print(
            "No slow-event messages."
        )


    # -------------------------------------------------------------
    # D. Duplicate / redelivery evidence
    # -------------------------------------------------------------

    print(
        "\nD. Duplicate / redelivery evidence"
    )

    print(
        f"Duplicate event IDs   : "
        f"{len(duplicate_event_ids)}"
    )

    print(
        f"Messages in >1 object : "
        f"{len(repeated_messages)}"
    )


    # -------------------------------------------------------------
    # E. Packing benefit
    # -------------------------------------------------------------

    print(
        "\nE. Packing benefit"
    )

    print(
        f"Unique SQS messages   : "
        f"{unique_messages:,}"
    )

    print(
        f"Observed events/msg   : "
        f"{observed_events_per_message:.2f}"
    )

    print(
        f"Observed reduction    : "
        f"{observed_reduction:.2f}%"
    )

    print(
        f"Age-bound estimate    : "
        f"~{expected_pack_size} events/msg"
    )

    print(
        f"Estimated reduction   : "
        f"{expected_reduction:.2f}%"
    )


    # -------------------------------------------------------------
    # F. Exact global reference metrics
    # -------------------------------------------------------------

    print(
        "\nF. Global reference metrics"
    )

    print(
        f"Exact active users    : "
        f"{len(active_users):,}"
    )

    print(
        f"Top 5 viewed products : "
        f"{product_views.most_common(5)}"
    )


    # -------------------------------------------------------------
    # G. Visibility-timeout supporting evidence
    # -------------------------------------------------------------

    print(
        "\nG. Visibility-timeout supporting evidence"
    )

    if lambda_to_ddb_values:

        print(
            f"Max Lambda->DDB time  : "
            f"{max(lambda_to_ddb_values)/1000:.3f} s"
        )

    print(
        "Note: this can support "
        "'no observed effect', "
        "but it does NOT prove "
        "30 s visibility is safe."
    )


    # -------------------------------------------------------------
    # H. S3 archival projection
    # -------------------------------------------------------------

    print(
        "\nH. S3 archival projection"
    )

    two_hour_gib = (
        total_bytes
        / (1024 ** 3)
    )

    monthly_gib = (
        total_bytes
        * MONTHLY_SCALE
    ) / (1024 ** 3)

    print(
        f"2-hour archive size   : "
        f"{two_hour_gib:.4f} GiB"
    )

    print(
        f"30-day projected PUTs : "
        f"{object_count * MONTHLY_SCALE:,}"
    )

    print(
        f"30-day projected data : "
        f"{monthly_gib:.4f} GiB"
    )


# ---------------------------------------------------------------------
# 7. Run all five final workloads
# ---------------------------------------------------------------------

def main():

    print(
        "Reading existing S3 data only. "
        "No AWS configuration will be changed."
    )

    for (
        run_id,
        rate,
    ) in RUNS.items():

        analyse_run(
            run_id,
            rate,
        )


if __name__ == "__main__":
    main()