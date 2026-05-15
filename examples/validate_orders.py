"""End-to-end example: validate the sample orders CSV with a YAML config.

Run from the repo root after ``pip install -e .``::

    python examples/validate_orders.py

This script intentionally uses local Spark + a small CSV so it can be run
on a developer laptop with no external services. The sample data contains
a handful of seeded quality issues (null customer_ids, a negative amount,
unknown payment method, invalid status, duplicate order_id), so the run is
expected to fail — that's the point. Replace the input file with real data
to run it for real.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from dq_framework import (
    ConsoleReporter,
    DataQualityValidator,
    JsonReporter,
)
from dq_framework.config_loader import build_expectations, load_config
from dq_framework.severity import CriticalDataQualityError


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
CONFIG_PATH = REPO_ROOT / "configs" / "orders.yaml"
DATA_PATH = HERE / "data" / "orders_sample.csv"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = (
        SparkSession.builder.appName("dq_framework-example-orders")
        .master("local[2]")
        .getOrCreate()
    )
    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(DATA_PATH))
        )
        print(f"Loaded {df.count()} rows from {DATA_PATH}")

        # Loosen the orders config slightly for the example: we don't want a
        # CRITICAL on the seeded duplicate to abort the run before we see
        # the rest of the failures.
        config = load_config(CONFIG_PATH)
        for item in config["expectations"]:
            if item.get("type") == "uniqueness":
                item["severity"] = "ERROR"

        expectations = build_expectations(config)
        validator = DataQualityValidator(
            expectations,
            dataset_name="orders_sample",
            raise_on_critical=False,
        )
        report = validator.run(df, metadata={"source": str(DATA_PATH)})

        ConsoleReporter().report(report)
        JsonReporter(str(REPO_ROOT / "examples" / "last_report.json")).report(report)

        return 0 if report.success else 1
    except CriticalDataQualityError as exc:
        print(f"CRITICAL DQ failure: {exc}", file=sys.stderr)
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
