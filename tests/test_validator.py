"""Tests for DataQualityValidator and ValidationReport."""

from __future__ import annotations

import pytest

from dq_framework.expectations import (
    NullRateExpectation,
    RowCountExpectation,
    UniquenessExpectation,
)
from dq_framework.severity import CriticalDataQualityError, Severity
from dq_framework.validator import DataQualityValidator


def test_validator_runs_all_expectations(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    validator = DataQualityValidator(
        [
            RowCountExpectation(min_rows=1),
            NullRateExpectation("id", max_null_rate=0.0),
            UniquenessExpectation("id"),
        ],
        dataset_name="ids",
    )
    report = validator.run(df)
    assert report.success
    assert report.total == 3
    assert report.passed == 3
    assert report.failed == 0


def test_validator_marks_run_failed_on_error(spark):
    df = spark.createDataFrame([(1,), (1,)], ["id"])
    validator = DataQualityValidator(
        [UniquenessExpectation("id", severity=Severity.ERROR)],
        dataset_name="ids",
    )
    report = validator.run(df)
    assert not report.success
    assert report.failed == 1


def test_validator_raises_on_critical(spark):
    df = spark.createDataFrame([(1,), (None,)], "id INT")
    validator = DataQualityValidator(
        [NullRateExpectation("id", max_null_rate=0.0, severity=Severity.CRITICAL)],
        dataset_name="ids",
    )
    with pytest.raises(CriticalDataQualityError):
        validator.run(df)


def test_validator_does_not_raise_when_configured(spark):
    df = spark.createDataFrame([(1,), (None,)], "id INT")
    validator = DataQualityValidator(
        [NullRateExpectation("id", max_null_rate=0.0, severity=Severity.CRITICAL)],
        dataset_name="ids",
        raise_on_critical=False,
    )
    report = validator.run(df)
    assert not report.success


def test_validator_captures_expectation_exceptions(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    # Reference a non-existent column to force an exception inside validate().
    validator = DataQualityValidator(
        [NullRateExpectation("does_not_exist", severity=Severity.ERROR)],
        dataset_name="ids",
    )
    report = validator.run(df)
    assert not report.success
    assert report.results[0].success is False
    assert "error" in report.results[0].observed


def test_validator_info_severity_does_not_fail_run(spark):
    df = spark.createDataFrame([(1,), (1,)], ["id"])
    validator = DataQualityValidator(
        [UniquenessExpectation("id", severity=Severity.INFO)],
        dataset_name="ids",
    )
    report = validator.run(df)
    # INFO is not in default fail_on set
    assert report.success
    assert report.failed == 1  # the expectation still failed, but doesn't fail the run


def test_validator_to_dict_is_json_serializable(spark):
    import json

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    validator = DataQualityValidator(
        [RowCountExpectation(min_rows=1)],
        dataset_name="ids",
    )
    report = validator.run(df)
    encoded = json.dumps(report.to_dict(), default=str)
    decoded = json.loads(encoded)
    assert decoded["dataset"] == "ids"
    assert decoded["totals"]["total"] == 1


def test_validator_requires_expectations():
    with pytest.raises(ValueError):
        DataQualityValidator([])


def test_validator_rejects_non_expectations():
    with pytest.raises(TypeError):
        DataQualityValidator(["not an expectation"])  # type: ignore[list-item]
