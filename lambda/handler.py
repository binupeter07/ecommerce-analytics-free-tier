import json
import logging
import os
import time
from decimal import Decimal

import boto3


log = logging.getLogger()
log.setLevel(logging.INFO)


REGION = os.environ.get("AWS_REGION", "eu-north-1")
DYNAMODB_TABLE = os.environ.get(
    "DYNAMODB_TABLE",
    "analytics_table",
)
S3_BUCKET = os.environ.get(
    "S3_BUCKET",
    "ecommerce-analytics-bucket",
)


dynamodb = boto3.resource(
    "dynamodb",
    region_name=REGION,
)
table = dynamodb.Table(DYNAMODB_TABLE)

s3 = boto3.client(
    "s3",
    region_name=REGION,
)


def _epoch_ms():
    """Return the current Unix time in milliseconds."""
    return time.time_ns() // 1_000_000


def _to_decimal(value):
    """Convert a numeric value for DynamoDB."""
    return Decimal(str(round(value, 6)))


def _compute_metrics(records):
    """Calculate the analytics for one Lambda invocation."""

    total = len(records)

    views = sum(
        1
        for record in records
        if record.get("event_type") == "view"
    )

    carts = sum(
        1
        for record in records
        if record.get("event_type") == "add_to_cart"
    )

    purchases = sum(
        1
        for record in records
        if record.get("event_type")
        in ("purchase", "transaction")
    )

    conversion_rate = (
        purchases / views
        if views
        else 0.0
    )

    abandonment_rate = (
        (carts - purchases) / carts
        if carts
        else 0.0
    )

    active_users = len({
        record["user_id"]
        for record in records
        if "user_id" in record
    })

    view_counts = {}

    for record in records:
        if (
            record.get("event_type") == "view"
            and "product_id" in record
        ):
            product_id = str(
                record["product_id"]
            )

            view_counts[product_id] = (
                view_counts.get(product_id, 0)
                + 1
            )

    top_products = sorted(
        view_counts,
        key=view_counts.get,
        reverse=True,
    )[:5]

    # Keep the older pre-write latency field
    # for compatibility with previous runs.
    now = time.time()

    latencies = [
        now - record["sent_at"]
        for record in records
        if isinstance(
            record.get("sent_at"),
            (int, float),
        )
    ]

    avg_latency_ms = (
        sum(latencies)
        / len(latencies)
        * 1000
        if latencies
        else None
    )

    return {
        "total": total,
        "views": views,
        "carts": carts,
        "purchases": purchases,
        "conversion_rate": conversion_rate,
        "abandonment_rate": abandonment_rate,
        "avg_latency_ms": avg_latency_ms,
        "active_users": active_users,
        "top_products": top_products,
    }


def _write_dynamodb(
    timestamp,
    test_run_id,
    metrics,
):
    """Write one analytics result to DynamoDB."""

    item = {
        "timestamp": _to_decimal(timestamp),
        "test_run_id": test_run_id,
        "total": metrics["total"],
        "views": metrics["views"],
        "carts": metrics["carts"],
        "purchases": metrics["purchases"],
        "conversion_rate": _to_decimal(
            metrics["conversion_rate"]
        ),
        "abandonment_rate": _to_decimal(
            metrics["abandonment_rate"]
        ),
        "active_users": metrics["active_users"],
        "top_products": metrics["top_products"],
    }

    if metrics["avg_latency_ms"] is not None:
        item["avg_latency_ms"] = _to_decimal(
            metrics["avg_latency_ms"]
        )

    table.put_item(
        Item=item
    )

    # This timestamp marks the end of the
    # event-level latency measurement.
    ddb_write_completed_ms = _epoch_ms()

    log.info(
        "DynamoDB write complete | "
        "run=%s | timestamp=%s | "
        "events=%d | completed_ms=%d",
        test_run_id,
        timestamp,
        metrics["total"],
        ddb_write_completed_ms,
    )

    return ddb_write_completed_ms


def _attach_event_level_latency(
    records,
    ddb_write_completed_ms,
    lambda_invocation_started_ms,
):
    """Add event-level latency values to the processed events."""

    latencies_ms = []
    missing_start_timestamps = 0
    negative_latencies = 0

    for record in records:
        producer_entered_ms = record.get(
            "producer_entered_ms"
        )

        if not isinstance(
            producer_entered_ms,
            (int, float),
        ):
            missing_start_timestamps += 1
            continue

        producer_entered_ms = int(
            producer_entered_ms
        )

        e2e_latency_ms = (
            int(ddb_write_completed_ms)
            - producer_entered_ms
        )

        record["ddb_write_completed_ms"] = int(
            ddb_write_completed_ms
        )

        record[
            "lambda_invocation_started_ms"
        ] = int(
            lambda_invocation_started_ms
        )

        record["e2e_latency_ms"] = int(
            e2e_latency_ms
        )

        # Store stage timings when the
        # SQS timestamp is available.
        sqs_sent_ms = record.get(
            "sqs_sent_ms"
        )

        if isinstance(
            sqs_sent_ms,
            (int, float),
        ):
            sqs_sent_ms = int(
                sqs_sent_ms
            )

            record["producer_to_sqs_ms"] = (
                sqs_sent_ms
                - producer_entered_ms
            )

            record[
                "sqs_to_lambda_start_ms"
            ] = (
                int(
                    lambda_invocation_started_ms
                )
                - sqs_sent_ms
            )

        latencies_ms.append(
            int(e2e_latency_ms)
        )

        if e2e_latency_ms < 0:
            negative_latencies += 1

    return (
        latencies_ms,
        missing_start_timestamps,
        negative_latencies,
    )


def _write_s3(
    timestamp,
    test_run_id,
    records,
):
    """Store the processed events in the run-specific S3 prefix."""

    safe_run_id = test_run_id.replace(
        "/",
        "_",
    )

    key = (
        f"raw/{safe_run_id}/"
        f"{timestamp}.json"
    )

    body = json.dumps(
        records,
        default=str,
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    log.info(
        "S3 write complete | "
        "run=%s | key=%s | records=%d",
        test_run_id,
        key,
        len(records),
    )


def lambda_handler(event, context):
    """Process SQS events and store the calculated analytics."""

    lambda_start = time.time()
    lambda_invocation_started_ms = (
        _epoch_ms()
    )

    records = []
    parse_errors = 0

    for sqs_record in event.get(
        "Records",
        [],
    ):
        try:
            body = json.loads(
                sqs_record["body"]
            )

            sqs_sent_timestamp = (
                sqs_record
                .get("attributes", {})
                .get("SentTimestamp")
            )

            try:
                sqs_sent_ms = (
                    int(sqs_sent_timestamp)
                    if sqs_sent_timestamp
                    is not None
                    else None
                )

            except (TypeError, ValueError):
                sqs_sent_ms = None

            sqs_message_id = (
                sqs_record.get(
                    "messageId"
                )
            )

            # Read the packed format used in
            # the final experiment.
            if (
                isinstance(body, dict)
                and isinstance(
                    body.get("events"),
                    list,
                )
            ):
                message_events = (
                    body["events"]
                )

            # Keep support for the earlier
            # one-event message format.
            elif isinstance(body, dict):
                message_events = [body]

            else:
                log.warning(
                    "Unsupported SQS body | "
                    "messageId=%s",
                    sqs_message_id,
                )

                parse_errors += 1
                continue

            for record in message_events:
                if not isinstance(
                    record,
                    dict,
                ):
                    parse_errors += 1
                    continue

                record = dict(record)

                if not record.get(
                    "test_run_id"
                ):
                    log.warning(
                        "Record %s has no "
                        "test_run_id",
                        sqs_message_id,
                    )

                record[
                    "sqs_message_id"
                ] = sqs_message_id

                if sqs_sent_ms is not None:
                    record[
                        "sqs_sent_ms"
                    ] = sqs_sent_ms

                records.append(record)

        except (
            json.JSONDecodeError,
            KeyError,
            TypeError,
        ) as exc:
            log.warning(
                "Skipping malformed record: "
                "%s | error=%s",
                sqs_record.get(
                    "messageId"
                ),
                exc,
            )

            parse_errors += 1

    if not records:
        log.warning(
            "No valid records in batch "
            "(parse_errors=%d)",
            parse_errors,
        )

        raise RuntimeError(
            "No valid records in SQS batch "
            f"(parse_errors={parse_errors})"
        )

    run_ids = {
        record.get("test_run_id")
        for record in records
        if record.get("test_run_id")
    }

    if len(run_ids) > 1:
        log.error(
            "Multiple test_run_id values "
            "in one Lambda batch: %s",
            run_ids,
        )

        raise RuntimeError(
            "Mixed test_run_id values "
            "in one SQS/Lambda batch"
        )

    test_run_id = next(
        iter(run_ids),
        "legacy_unknown",
    )

    metrics = _compute_metrics(
        records
    )

    timestamp = records[-1].get(
        "timestamp",
        time.time(),
    )

    log.info(
        "Batch processed | run=%s | "
        "timestamp=%s | total=%d | "
        "views=%d | carts=%d | "
        "purchases=%d | "
        "prewrite_avg_latency_ms=%s",
        test_run_id,
        timestamp,
        metrics["total"],
        metrics["views"],
        metrics["carts"],
        metrics["purchases"],
        (
            f"{metrics['avg_latency_ms']:.1f}"
            if metrics[
                "avg_latency_ms"
            ] is not None
            else "N/A"
        ),
    )

    errors = []
    ddb_write_completed_ms = None
    event_latencies_ms = []
    missing_start_timestamps = 0
    negative_latencies = 0

    # Write the live analytics first.
    try:
        ddb_write_completed_ms = (
            _write_dynamodb(
                timestamp,
                test_run_id,
                metrics,
            )
        )

        (
            event_latencies_ms,
            missing_start_timestamps,
            negative_latencies,
        ) = _attach_event_level_latency(
            records,
            ddb_write_completed_ms,
            lambda_invocation_started_ms,
        )

        if event_latencies_ms:
            mean_e2e_latency_ms = (
                sum(event_latencies_ms)
                / len(event_latencies_ms)
            )

            log.info(
                "E2E latency | run=%s | "
                "samples=%d | mean_ms=%.2f | "
                "min_ms=%d | max_ms=%d | "
                "missing_start=%d | negative=%d",
                test_run_id,
                len(event_latencies_ms),
                mean_e2e_latency_ms,
                min(event_latencies_ms),
                max(event_latencies_ms),
                missing_start_timestamps,
                negative_latencies,
            )

        else:
            log.warning(
                "No E2E latency samples | "
                "run=%s | missing_start=%d",
                test_run_id,
                missing_start_timestamps,
            )

        if negative_latencies:
            log.error(
                "Negative E2E latency | "
                "run=%s | count=%d",
                test_run_id,
                negative_latencies,
            )

    except Exception as exc:
        log.exception(
            "DynamoDB write or latency "
            "measurement failed"
        )

        errors.append(
            f"dynamodb: {exc}"
        )

    # Write the archive after DynamoDB.
    try:
        _write_s3(
            timestamp,
            test_run_id,
            records,
        )

    except Exception as exc:
        log.exception(
            "S3 write failed"
        )

        errors.append(
            f"s3: {exc}"
        )

    execution_ms = round(
        (
            time.time()
            - lambda_start
        )
        * 1000,
        2,
    )

    log.info(
        "Lambda execution | "
        "run=%s | duration_ms=%.2f",
        test_run_id,
        execution_ms,
    )

    if errors:
        # Raising an exception lets the SQS
        # event-source mapping retry the batch.
        raise RuntimeError(
            "Downstream write failure: "
            + "; ".join(errors)
        )

    mean_e2e_latency_ms = (
        sum(event_latencies_ms)
        / len(event_latencies_ms)
        if event_latencies_ms
        else None
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": len(records),
            "parse_errors": parse_errors,
            "execution_ms": execution_ms,
            "avg_latency_ms": (
                metrics["avg_latency_ms"]
            ),
            "e2e_latency_samples": (
                len(event_latencies_ms)
            ),
            "mean_e2e_latency_ms": (
                mean_e2e_latency_ms
            ),
            "missing_producer_entered_ms": (
                missing_start_timestamps
            ),
            "negative_e2e_latency_samples": (
                negative_latencies
            ),
            "ddb_write_completed_ms": (
                ddb_write_completed_ms
            ),
            "test_run_id": test_run_id,
        }),
    }
