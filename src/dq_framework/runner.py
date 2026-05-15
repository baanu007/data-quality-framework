"""CLI entry point: ``python -m dq_framework run --config ... --table ...``.

Example
-------
::

    python -m dq_framework run \\
        --config configs/orders.yaml \\
        --table data/orders.csv \\
        --format csv \\
        --report-json out/orders_report.json
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional, Sequence

from dq_framework.config_loader import build_expectations, load_config
from dq_framework.reporters import (
    ConsoleReporter,
    JsonReporter,
    Reporter,
    S3Reporter,
    SlackReporter,
)
from dq_framework.severity import CriticalDataQualityError, Severity
from dq_framework.validator import DataQualityValidator, ValidationReport

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dq_framework",
        description="Run a dq_framework rule set against a table.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run validation against a table")
    run.add_argument("--config", required=True, help="Path to YAML rule set")
    run.add_argument("--table", required=True, help="Input path or table URI")
    run.add_argument(
        "--format",
        default="parquet",
        help="Spark format for --table (parquet, csv, json, delta, ...). " "Defaults to parquet.",
    )
    run.add_argument(
        "--header",
        action="store_true",
        help="When --format=csv, treat the first line as a header.",
    )
    run.add_argument(
        "--infer-schema",
        action="store_true",
        help="When --format=csv, infer column types.",
    )
    run.add_argument(
        "--dataset",
        default=None,
        help="Override dataset name (defaults to value from config or " "the table basename).",
    )
    run.add_argument(
        "--report-json",
        default=None,
        help="Write JSON report to this path.",
    )
    run.add_argument(
        "--report-s3",
        default=None,
        help="Upload JSON report to this s3:// URI.",
    )
    run.add_argument(
        "--slack",
        action="store_true",
        help="Post a summary to Slack (uses $SLACK_WEBHOOK_URL).",
    )
    run.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def _load_dataframe(spark, args: argparse.Namespace):
    reader = spark.read.format(args.format)
    if args.format.lower() == "csv":
        reader = reader.option("header", str(args.header).lower())
        reader = reader.option("inferSchema", str(args.infer_schema).lower())
    return reader.load(args.table)


def _build_reporters(args: argparse.Namespace) -> List[Reporter]:
    reporters: List[Reporter] = [ConsoleReporter()]
    if args.report_json:
        reporters.append(JsonReporter(args.report_json))
    if args.report_s3:
        reporters.append(S3Reporter(args.report_s3))
    if args.slack:
        reporters.append(SlackReporter())
    return reporters


def _resolve_dataset_name(args: argparse.Namespace, config: dict) -> str:
    if args.dataset:
        return args.dataset
    if config.get("dataset"):
        return str(config["dataset"])
    # Fall back to basename of --table.
    return args.table.rstrip("/").split("/")[-1] or "dataset"


def _resolve_validator_kwargs(config: dict) -> dict:
    kwargs: dict = {}
    if "fail_on" in config:
        fail_on = config["fail_on"]
        if not isinstance(fail_on, list):
            raise ValueError("config['fail_on'] must be a list")
        kwargs["fail_on"] = [Severity.from_str(s) for s in fail_on]
    if "raise_on_critical" in config:
        kwargs["raise_on_critical"] = bool(config["raise_on_critical"])
    return kwargs


def run(args: argparse.Namespace) -> ValidationReport:
    """Programmatic equivalent of the ``run`` subcommand."""
    # Lazy import so `--help` works without a Spark install.
    from pyspark.sql import SparkSession

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config(args.config)
    expectations = build_expectations(config)
    dataset = _resolve_dataset_name(args, config)
    validator_kwargs = _resolve_validator_kwargs(config)

    spark = SparkSession.builder.appName(f"dq_framework:{dataset}").getOrCreate()
    try:
        df = _load_dataframe(spark, args)
        validator = DataQualityValidator(expectations, dataset_name=dataset, **validator_kwargs)
        report = validator.run(df, metadata={"source": args.table, "format": args.format})
        for reporter in _build_reporters(args):
            try:
                reporter.report(report)
            except Exception:  # noqa: BLE001
                logger.exception("Reporter %s failed; continuing.", type(reporter).__name__)
        return report
    finally:
        spark.stop()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command != "run":  # pragma: no cover - argparse enforces this
        parser.print_help()
        return 2
    try:
        report = run(args)
    except CriticalDataQualityError as exc:
        logger.error("CRITICAL DQ failure: %s", exc)
        return 2
    return 0 if report.success else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
