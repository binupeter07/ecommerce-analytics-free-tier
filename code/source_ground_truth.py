import csv


CSV_FILE = "retailrocket/events.csv"

TARGETS = {
    1200: "prov_e2e_10epm",
    2400: "prov_e2e_20epm_final",
    6000: "prov_e2e_50epm",
    12000: "prov_e2e_100epm",
    60000: "prov_e2e_500epm",
}


def calculate_source_metrics():
    valid_count = 0
    views = 0
    carts = 0
    purchases = 0

    results = {}

    with open(
        CSV_FILE,
        "r",
        encoding="utf-8",
        newline="",
    ) as f:
        reader = csv.DictReader(f)

        for row in reader:
            event_type = row.get("event")

            if event_type not in (
                "view",
                "addtocart",
                "transaction",
            ):
                continue

            try:
                int(row["visitorid"])
                int(row["itemid"])
                int(row["timestamp"])
            except (ValueError, TypeError, KeyError):
                continue

            valid_count += 1

            if event_type == "view":
                views += 1
            elif event_type == "addtocart":
                carts += 1
            elif event_type == "transaction":
                purchases += 1

            if valid_count in TARGETS:
                conversion = (
                    purchases / views * 100
                    if views
                    else 0.0
                )

                abandonment = (
                    (carts - purchases) / carts * 100
                    if carts
                    else 0.0
                )

                results[valid_count] = {
                    "run_id": TARGETS[valid_count],
                    "total_events": valid_count,
                    "views": views,
                    "carts": carts,
                    "purchases": purchases,
                    "conversion_rate_pct": conversion,
                    "abandonment_rate_pct": abandonment,
                }

            if valid_count >= max(TARGETS):
                break

    return results


def print_results(results):
    print("SOURCE GROUND-TRUTH METRICS")
    print("=" * 92)

    print(
        f"{'run_id':<24}"
        f"{'total':>8}"
        f"{'views':>10}"
        f"{'carts':>8}"
        f"{'purch':>8}"
        f"{'conversion %':>16}"
        f"{'abandonment %':>18}"
    )

    print("-" * 92)

    for target in TARGETS:
        result = results.get(target)

        if result is None:
            print(
                f"Missing source result "
                f"for {target:,} valid events."
            )
            continue

        print(
            f"{result['run_id']:<24}"
            f"{result['total_events']:>8}"
            f"{result['views']:>10}"
            f"{result['carts']:>8}"
            f"{result['purchases']:>8}"
            f"{result['conversion_rate_pct']:>16.6f}"
            f"{result['abandonment_rate_pct']:>18.6f}"
        )


def main():
    results = calculate_source_metrics()
    print_results(results)


if __name__ == "__main__":
    main()