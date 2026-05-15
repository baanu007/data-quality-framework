"""Unit tests for each Expectation subclass."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dq_framework.expectations import (
    FreshnessExpectation,
    NullRateExpectation,
    ReferentialIntegrityExpectation,
    RegexExpectation,
    RowCountExpectation,
    UniquenessExpectation,
    ValueRangeExpectation,
)
from dq_framework.severity import Severity

# ---------------------------------------------------------------------- #
# RowCountExpectation                                                    #
# ---------------------------------------------------------------------- #


def test_row_count_pass(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    result = RowCountExpectation(min_rows=1, max_rows=10).validate(df)
    assert result.success
    assert result.observed["row_count"] == 3
    assert result.severity == Severity.ERROR


def test_row_count_below_min(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    result = RowCountExpectation(min_rows=5).validate(df)
    assert not result.success
    assert result.observed["row_count"] == 1


def test_row_count_requires_a_bound():
    with pytest.raises(ValueError):
        RowCountExpectation()


# ---------------------------------------------------------------------- #
# NullRateExpectation                                                    #
# ---------------------------------------------------------------------- #


def test_null_rate_pass(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    result = NullRateExpectation("id", max_null_rate=0.0).validate(df)
    assert result.success
    assert result.observed["null_count"] == 0


def test_null_rate_fail_with_sample(spark):
    df = spark.createDataFrame([(1,), (None,), (None,), (4,)], ["id"])
    result = NullRateExpectation("id", max_null_rate=0.1).validate(df)
    assert not result.success
    assert result.observed["null_count"] == 2
    assert pytest.approx(result.observed["null_rate"], rel=1e-6) == 0.5
    assert result.failed_sample, "failed_sample should be populated"


def test_null_rate_empty_df_is_vacuously_true(spark):
    df = spark.createDataFrame([], "id INT")
    result = NullRateExpectation("id").validate(df)
    assert result.success
    assert result.observed["row_count"] == 0


def test_null_rate_unknown_column_raises_value_error(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="not found"):
        NullRateExpectation("nope").validate(df)


def test_null_rate_invalid_bounds():
    with pytest.raises(ValueError):
        NullRateExpectation("c", max_null_rate=1.5)


# ---------------------------------------------------------------------- #
# UniquenessExpectation                                                  #
# ---------------------------------------------------------------------- #


def test_uniqueness_pass(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    result = UniquenessExpectation("id").validate(df)
    assert result.success


def test_uniqueness_detects_duplicates(spark):
    df = spark.createDataFrame([(1,), (2,), (2,), (3,), (3,)], ["id"])
    result = UniquenessExpectation("id").validate(df)
    assert not result.success
    assert result.observed["duplicate_key_count"] == 2  # two duplicate keys
    # Two duplicated keys (2 and 3) each appear twice -> 1 extra row per key,
    # for a total of 2 duplicate rows beyond the originals.
    assert result.observed["duplicate_row_count"] == 2


def test_uniqueness_three_rows_same_key(spark):
    # A single key appearing three times should count as 2 duplicate rows
    # (the first occurrence is the "original").
    df = spark.createDataFrame([(1,), (1,), (1,), (2,)], ["id"])
    result = UniquenessExpectation("id").validate(df)
    assert not result.success
    assert result.observed["duplicate_key_count"] == 1
    assert result.observed["duplicate_row_count"] == 2


def test_uniqueness_compound_key(spark):
    df = spark.createDataFrame(
        [(1, "a"), (1, "b"), (2, "a"), (1, "a")],
        ["id", "k"],
    )
    result = UniquenessExpectation(["id", "k"]).validate(df)
    assert not result.success
    assert result.observed["duplicate_key_count"] == 1


def test_uniqueness_ignore_nulls(spark):
    df = spark.createDataFrame([(None,), (None,), (1,)], "id INT")
    result = UniquenessExpectation("id", ignore_nulls=True).validate(df)
    assert result.success


# ---------------------------------------------------------------------- #
# ValueRangeExpectation                                                  #
# ---------------------------------------------------------------------- #


def test_value_range_pass(spark):
    df = spark.createDataFrame([(1.0,), (5.0,), (9.9,)], ["x"])
    result = ValueRangeExpectation("x", min_value=0, max_value=10).validate(df)
    assert result.success


def test_value_range_fail(spark):
    df = spark.createDataFrame([(-1.0,), (5.0,), (11.0,)], ["x"])
    result = ValueRangeExpectation("x", min_value=0, max_value=10).validate(df)
    assert not result.success
    assert result.observed["out_of_range_count"] == 2


def test_value_range_disallow_nulls(spark):
    df = spark.createDataFrame([(1.0,), (None,)], "x DOUBLE")
    result = ValueRangeExpectation("x", min_value=0, allow_nulls=False).validate(df)
    assert not result.success


# ---------------------------------------------------------------------- #
# RegexExpectation                                                       #
# ---------------------------------------------------------------------- #


def test_regex_pass(spark):
    df = spark.createDataFrame([("a@b.com",), ("x@y.io",)], ["email"])
    result = RegexExpectation("email", r"^[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}$").validate(df)
    assert result.success


def test_regex_fail(spark):
    df = spark.createDataFrame([("a@b.com",), ("not-an-email",)], ["email"])
    result = RegexExpectation("email", r"^[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}$").validate(df)
    assert not result.success
    assert result.observed["mismatch_count"] == 1


def test_regex_invalid_pattern_blows_up_early():
    with pytest.raises(Exception):
        RegexExpectation("c", "([unclosed")


# ---------------------------------------------------------------------- #
# ReferentialIntegrityExpectation                                        #
# ---------------------------------------------------------------------- #


def test_referential_integrity_pass(spark):
    orders = spark.createDataFrame([(1, "C1"), (2, "C2"), (3, "C1")], ["order_id", "customer_id"])
    customers = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])
    result = ReferentialIntegrityExpectation("customer_id", customers, "customer_id").validate(
        orders
    )
    assert result.success


def test_referential_integrity_detects_orphans(spark):
    orders = spark.createDataFrame([(1, "C1"), (2, "C99"), (3, "C1")], ["order_id", "customer_id"])
    customers = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])
    result = ReferentialIntegrityExpectation("customer_id", customers, "customer_id").validate(
        orders
    )
    assert not result.success
    assert result.observed["orphan_count"] == 1
    assert result.failed_sample[0]["customer_id"] == "C99"


# ---------------------------------------------------------------------- #
# FreshnessExpectation                                                   #
# ---------------------------------------------------------------------- #


def test_freshness_pass(spark):
    now = datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [(datetime(2024, 1, 10, 11, 0),)],
        "ts TIMESTAMP",
    )
    result = FreshnessExpectation("ts", max_age=timedelta(hours=2), now=now).validate(df)
    assert result.success


def test_freshness_stale(spark):
    now = datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [(datetime(2024, 1, 9, 0, 0),)],
        "ts TIMESTAMP",
    )
    result = FreshnessExpectation("ts", max_age=timedelta(hours=2), now=now).validate(df)
    assert not result.success


def test_freshness_no_data(spark):
    now = datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
    df = spark.createDataFrame([(None,)], "ts TIMESTAMP")
    result = FreshnessExpectation("ts", max_age=timedelta(hours=2), now=now).validate(df)
    assert not result.success
    assert result.observed["max_timestamp"] is None


def test_freshness_rejects_negative_max_age():
    with pytest.raises(ValueError):
        FreshnessExpectation("ts", max_age=timedelta(seconds=-1))
