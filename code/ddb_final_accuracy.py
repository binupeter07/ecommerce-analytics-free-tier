import boto3
from boto3.dynamodb.conditions import Attr


REGION = "eu-west-1"
TABLE_NAME = "analytics-table"

SOURCE = {
    "prov_e2e_10epm": {
        "total": 1200,
        "views": 1161,
        "carts": 34,
        "purchases": 5,
    },
    "prov_e2e_20epm_final": {
        "total": 2400,
        "views": 2323,
        "carts": 57,
        "purchases": 20,
    },
    "prov_e2e_50epm": {
        "total": 6000,
        "views": 5791,
        "carts": 156,
        "purchases": 53,
    },
    "prov_e2e_100epm": {
        "total": 12000,
        "views": 11595,
        "carts": 294,
        "purchases": 111,
    },
    "prov_e2e_500epm": {
        "total": 60000,
        "views": 58060,
        "carts": 1408,
        "purchases": 532,
    },
}

RUN_IDS = list(SOURCE.keys())


def relative_error(pipeline_value, source_value):
    if source_value == 0:
        return 0.0 if pipeline_value == 0 else float("inf")

    return abs(pipeline_value - source_value) / source_value * 100


def read_dynamodb_results(table):
    items = []

    kwargs = {
        "FilterExpression": Attr("test_run_id").is_in(RUN_IDS)
    }

    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))

        if "LastEvaluatedKey" not in response:
            break

        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return items


def combine_results(items):
    results = {
        run_id: {
            "records": 0,
            "total": 0,
            "views": 0,
            "carts": 0,
            "purchases": 0,
        }
        for run_id in RUN_IDS
    }

    for item in items:
        run_id = item.get("test_run_id")

        if run_id not in results:
            continue

        results[run_id]["records"] += 1
        results[run_id]["total"] += int(item.get("total", 0))
        results[run_id]["views"] += int(item.get("views", 0))
        results[run_id]["carts"] += int(item.get("carts", 0))
        results[run_id]["purchases"] += int(
            item.get("purchases", 0)
        )

    return results


def main():
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=REGION,
    )
    table = dynamodb.Table(TABLE_NAME)

    items = read_dynamodb_results(table)
    results = combine_results(items)

    print("=" * 115)
    print("DYNAMODB ANALYTICAL ACCURACY")
    print("=" * 115)

    print(
        f"{'run_id':<24}"
        f"{'records':>9}"
        f"{'total':>9}"
        f"{'views':>10}"
        f"{'carts':>8}"
        f"{'purch':>8}"
        f"{'conversion %':>15}"
        f"{'abandonment %':>17}"
        f"{'max error %':>14}"
    )

    print("-" * 115)

    for run_id in RUN_IDS:
        result = results[run_id]
        source = SOURCE[run_id]

        conversion = (
            result["purchases"] / result["views"] * 100
            if result["views"]
            else 0.0
        )

        abandonment = (
            (
                result["carts"] - result["purchases"]
            )
            / result["carts"]
            * 100
            if result["carts"]
            else 0.0
        )

        source_conversion = (
            source["purchases"] / source["views"] * 100
            if source["views"]
            else 0.0
        )

        source_abandonment = (
            (
                source["carts"] - source["purchases"]
            )
            / source["carts"]
            * 100
            if source["carts"]
            else 0.0
        )

        errors = [
            relative_error(
                result["total"],
                source["total"],
            ),
            relative_error(
                result["views"],
                source["views"],
            ),
            relative_error(
                result["carts"],
                source["carts"],
            ),
            relative_error(
                result["purchases"],
                source["purchases"],
            ),
            relative_error(
                conversion,
                source_conversion,
            ),
            relative_error(
                abandonment,
                source_abandonment,
            ),
        ]

        max_error = max(errors)

        print(
            f"{run_id:<24}"
            f"{result['records']:>9}"
            f"{result['total']:>9}"
            f"{result['views']:>10}"
            f"{result['carts']:>8}"
            f"{result['purchases']:>8}"
            f"{conversion:>15.6f}"
            f"{abandonment:>17.6f}"
            f"{max_error:>14.6f}"
        )

    print()
    print("Accuracy check complete.")


if __name__ == "__main__":
    main()