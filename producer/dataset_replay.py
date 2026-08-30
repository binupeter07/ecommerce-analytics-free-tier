import argparse
import csv
import json
import logging
import os
import sys
import time
import uuid
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

log = logging.getLogger(__name__)


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

DEFAULT_CSV = os.path.join(
    PROJECT_ROOT,
    "retailrocket",
    "events.csv",
)

CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "config",
    "settings.json",
)


try:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        CONFIG = json.load(f)

    REGION = CONFIG["region"]
    QUEUE_URL = CONFIG["sqs_queue_url"]

except (OSError, json.JSONDecodeError, KeyError) as exc:
    log.error("Could not load configuration: %s", exc)
    sys.exit(1)


try:
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
    )

except Exception as exc:
    log.error("Could not create the SQS client: %s", exc)
    sys.exit(1)


EVENT_TYPE_MAP = {
    "view": "view",
    "addtocart": "add_to_cart",
    "transaction": "purchase",
}


def parse_row(row: dict) -> Optional[dict]:
    """Convert a RetailRocket row to the event format used by the pipeline."""

    event_type = EVENT_TYPE_MAP.get(
        row.get("event", "").strip()
    )

    if event_type is None:
        return None

    try:
        visitor_id = int(row["visitorid"])
        product_id = int(row["itemid"])
        original_ts_ms = int(row["timestamp"])

    except (KeyError, TypeError, ValueError):
        return None

    return {
        "user_id": visitor_id,
        "event_type": event_type,
        "product_id": product_id,
        "session_id": visitor_id % 10_000,
        "original_ts_ms": original_ts_ms,
    }


def load_events(csv_path: str) -> list[dict]:
    """Load valid events from the RetailRocket CSV file."""

    log.info("Loading RetailRocket dataset: %s", csv_path)

    events = []
    skipped = 0

    with open(
        csv_path,
        newline="",
        encoding="utf-8",
    ) as f:
        reader = csv.DictReader(f)

        for row in reader:
            event = parse_row(row)

            if event is None:
                skipped += 1
            else:
                events.append(event)

    if not events:
        raise ValueError("No valid events found in CSV.")

    total = len(events)

    views = sum(
        event["event_type"] == "view"
        for event in events
    )

    carts = sum(
        event["event_type"] == "add_to_cart"
        for event in events
    )

    purchases = sum(
        event["event_type"] == "purchase"
        for event in events
    )

    log.info(
        "Loaded %d events (%d skipped)",
        total,
        skipped,
    )

    log.info(
        "Distribution: view %.2f%% | add_to_cart %.2f%% | purchase %.2f%%",
        views / total * 100,
        carts / total * 100,
        purchases / total * 100,
    )

    return events


def build_message_body(events: list[dict]) -> dict:
    """Create the body for one packed SQS message."""

    return {
        "packing_version": 1,
        "event_count": len(events),
        "events": events,
    }


def serialize_message(events: list[dict]) -> bytes:
    """Convert a packed message to the bytes sent to SQS."""

    body = build_message_body(events)

    return json.dumps(
        body,
        separators=(",", ":"),
    ).encode("utf-8")


def send_pack(
    events: list[dict],
    max_retries: int = 3,
) -> tuple[bool, int]:
    """Send one packed message to SQS."""

    if not events:
        return True, 0

    payload = serialize_message(events)
    payload_bytes = len(payload)

    for attempt in range(1, max_retries + 1):
        try:
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=payload.decode("utf-8"),
            )

            return True, payload_bytes

        except NoCredentialsError:
            log.error(
                "AWS credentials were not found. "
                "Configure AWS credentials before running the replay."
            )
            raise

        except ClientError as exc:
            log.warning(
                "SQS send attempt %d/%d failed: %s",
                attempt,
                max_retries,
                exc,
            )

        if attempt < max_retries:
            time.sleep(2 * attempt)

    return False, payload_bytes


def replay(
    events: list[dict],
    events_per_minute: float,
    max_events: int = 0,
    loop: bool = False,
    max_pack_events: int = 120,
    pack_window: float = 13.0,
    max_payload_bytes: int = 60_000,
    test_run_id: str = "manual_run",
) -> None:
    """Replay events at a controlled rate using bounded packing."""

    if events_per_minute <= 0:
        raise ValueError(
            "events_per_minute must be greater than zero."
        )

    if max_pack_events <= 0:
        raise ValueError(
            "max_pack_events must be greater than zero."
        )

    if pack_window <= 0:
        raise ValueError(
            "pack_window must be greater than zero."
        )

    if max_payload_bytes <= 0:
        raise ValueError(
            "max_payload_bytes must be greater than zero."
        )

    if max_events < 0:
        raise ValueError(
            "max_events cannot be negative."
        )

    interval = 60.0 / events_per_minute

    generated_events = 0
    successful_events = 0
    failed_events = 0

    successful_sqs_messages = 0
    failed_sqs_messages = 0

    pack = []
    pack_started_mono = None
    dataset_pass = 0

    next_event_time = time.monotonic()

    log.info("Run ID: %s", test_run_id)

    log.info(
        "Replay rate: %.2f events/min (%.4f s/event)",
        events_per_minute,
        interval,
    )

    log.info(
        "Packing limits: %d events, %.1f seconds, %d bytes",
        max_pack_events,
        pack_window,
        max_payload_bytes,
    )

    def event_stream():
        nonlocal dataset_pass

        while True:
            dataset_pass += 1
            log.info("Dataset pass %d", dataset_pass)

            for original_event in events:
                yield dict(original_event)

            if not loop:
                break

    stream = event_stream()

    def flush_pack(reason: str) -> None:
        nonlocal pack
        nonlocal pack_started_mono
        nonlocal successful_events
        nonlocal failed_events
        nonlocal successful_sqs_messages
        nonlocal failed_sqs_messages

        if not pack:
            return

        current_pack = pack
        pack = []
        pack_started_mono = None

        success, payload_bytes = send_pack(
            current_pack
        )

        if success:
            successful_events += len(current_pack)
            successful_sqs_messages += 1

            log.info(
                "SQS message sent | reason=%s | events=%d | bytes=%d",
                reason,
                len(current_pack),
                payload_bytes,
            )

        else:
            failed_events += len(current_pack)
            failed_sqs_messages += 1

            log.error(
                "SQS message failed | reason=%s | events=%d | bytes=%d",
                reason,
                len(current_pack),
                payload_bytes,
            )

    try:
        while True:
            if (
                max_events > 0
                and generated_events >= max_events
            ):
                break

            # Send the pack if its waiting time ends
            # before the next event is due.
            if pack and pack_started_mono is not None:
                pack_deadline = (
                    pack_started_mono
                    + pack_window
                )

                if pack_deadline <= next_event_time:
                    delay = (
                        pack_deadline
                        - time.monotonic()
                    )

                    if delay > 0:
                        time.sleep(delay)

                    flush_pack("window_expired")
                    continue

            delay = (
                next_event_time
                - time.monotonic()
            )

            if delay > 0:
                time.sleep(delay)

            try:
                event = next(stream)

            except StopIteration:
                break

            # Record the event arrival time before packing.
            now_ns = time.time_ns()
            now = now_ns / 1_000_000_000
            producer_entered_ms = (
                now_ns // 1_000_000
            )

            event["test_run_id"] = test_run_id
            event["event_id"] = str(
                uuid.uuid4()
            )

            # Keep these fields for compatibility
            # with earlier runs and analysis scripts.
            event["timestamp"] = now
            event["sent_at"] = now

            event[
                "producer_entered_ms"
            ] = producer_entered_ms

            generated_events += 1

            # Send the current pack before adding an
            # event that would exceed the payload limit.
            if pack:
                candidate_payload_bytes = len(
                    serialize_message(
                        pack + [event]
                    )
                )

                if (
                    candidate_payload_bytes
                    > max_payload_bytes
                ):
                    flush_pack(
                        "payload_limit"
                    )

            if not pack:
                pack_started_mono = (
                    time.monotonic()
                )

            pack.append(event)

            current_payload_bytes = len(
                serialize_message(pack)
            )

            # A single large event is sent immediately
            # rather than blocking the producer.
            if (
                current_payload_bytes
                > max_payload_bytes
            ):
                log.warning(
                    "Pack exceeds payload guard: %d > %d bytes",
                    current_payload_bytes,
                    max_payload_bytes,
                )

                flush_pack(
                    "payload_guard_exceeded"
                )

            elif len(pack) >= max_pack_events:
                flush_pack(
                    "count_limit"
                )

            next_event_time += interval

            # Avoid catch-up bursts if processing
            # falls behind the planned schedule.
            if (
                next_event_time
                < time.monotonic()
            ):
                next_event_time = (
                    time.monotonic()
                )

            if (
                generated_events > 0
                and generated_events % 100 == 0
            ):
                log.info(
                    "Progress: generated=%d successful=%d failed=%d "
                    "sqs_messages=%d",
                    generated_events,
                    successful_events,
                    failed_events,
                    successful_sqs_messages,
                )

    except KeyboardInterrupt:
        log.info("Replay stopped by user.")

    finally:
        if pack:
            flush_pack(
                "final_partial"
            )

        log.info(
            "Summary: generated=%d successful=%d failed=%d "
            "sqs_messages=%d failed_messages=%d passes=%d run_id=%s",
            generated_events,
            successful_events,
            failed_events,
            successful_sqs_messages,
            failed_sqs_messages,
            dataset_pass,
            test_run_id,
        )


SCENARIOS = {
    "baseline": 10.0,
    "peak": 20.0,
    "high": 50.0,
    "stress": 500.0,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Replay RetailRocket events using "
            "the fixed bounded-packing configuration."
        )
    )

    parser.add_argument(
        "--csv",
        default=DEFAULT_CSV,
        help="Path to the RetailRocket CSV file.",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Event arrival rate in events per minute.",
    )

    parser.add_argument(
        "--scenario",
        choices=list(
            SCENARIOS.keys()
        ),
        help="Use one of the predefined workload rates.",
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help=(
            "Maximum number of events to replay. "
            "Use 0 to replay the dataset once."
        ),
    )

    parser.add_argument(
        "--loop",
        action="store_true",
        help=(
            "Repeat the dataset until stopped "
            "or max-events is reached."
        ),
    )

    parser.add_argument(
        "--max-pack-events",
        "--pack-size",
        dest="max_pack_events",
        type=int,
        default=120,
        help=(
            "Maximum events in one SQS message. "
            "Default: 120."
        ),
    )

    parser.add_argument(
        "--pack-window",
        type=float,
        default=13.0,
        help=(
            "Maximum producer packing time in seconds. "
            "Default: 13."
        ),
    )

    parser.add_argument(
        "--max-payload-bytes",
        type=int,
        default=60_000,
        help=(
            "Maximum preferred packed message size. "
            "Default: 60000 bytes."
        ),
    )

    parser.add_argument(
        "--run-id",
        default="manual_run",
        help=(
            "Experiment run ID, "
            "for example prov_e2e_10epm."
        ),
    )

    args = parser.parse_args()

    if not os.path.isfile(args.csv):
        log.error(
            "CSV file not found: %s",
            args.csv,
        )
        sys.exit(1)

    if (
        args.rate is not None
        and args.rate <= 0
    ):
        log.error(
            "--rate must be greater than zero."
        )
        sys.exit(1)

    if args.max_events < 0:
        log.error(
            "--max-events cannot be negative."
        )
        sys.exit(1)

    if args.max_pack_events <= 0:
        log.error(
            "--max-pack-events must be greater than zero."
        )
        sys.exit(1)

    if args.pack_window <= 0:
        log.error(
            "--pack-window must be greater than zero."
        )
        sys.exit(1)

    if args.max_payload_bytes <= 0:
        log.error(
            "--max-payload-bytes must be greater than zero."
        )
        sys.exit(1)

    if args.rate is not None:
        rate = args.rate

    elif args.scenario:
        rate = SCENARIOS[
            args.scenario
        ]

    else:
        rate = 10.0

    try:
        events = load_events(
            args.csv
        )

        replay(
            events=events,
            events_per_minute=rate,
            max_events=args.max_events,
            loop=args.loop,
            max_pack_events=args.max_pack_events,
            pack_window=args.pack_window,
            max_payload_bytes=args.max_payload_bytes,
            test_run_id=args.run_id,
        )

    except (
        OSError,
        ValueError,
        ClientError,
    ) as exc:
        log.error(
            "Replay failed: %s",
            exc,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
