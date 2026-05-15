"""Tests for the Great Expectations adapter."""

from __future__ import annotations

import json

from dq_framework.expectations import NullRateExpectation
from dq_framework.ge_adapter import from_great_expectations


def _write_suite(tmp_path, expectations):
    suite = {"expectation_suite_name": "test", "expectations": expectations}
    p = tmp_path / "suite.json"
    p.write_text(json.dumps(suite), encoding="utf-8")
    return p


def test_translates_known_types(tmp_path):
    path = _write_suite(
        tmp_path,
        [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": 1000},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "id"},
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "id"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "amount", "min_value": 0, "max_value": 1000},
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": "email", "regex": r"^.+@.+$"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "status", "value_set": ["A", "B", "C"]},
            },
        ],
    )
    expectations = from_great_expectations(path)
    types = [type(e).__name__ for e in expectations]
    assert types == [
        "RowCountExpectation",
        "NullRateExpectation",
        "UniquenessExpectation",
        "ValueRangeExpectation",
        "RegexExpectation",
        "RegexExpectation",
    ]


def test_skips_unsupported_types(tmp_path, caplog):
    path = _write_suite(
        tmp_path,
        [
            {
                "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                "kwargs": {"column": "x"},
            },
        ],
    )
    out = from_great_expectations(path)
    assert out == []


def test_not_null_with_mostly_translates_to_null_rate(tmp_path):
    path = _write_suite(
        tmp_path,
        [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "id", "mostly": 0.95},
            },
        ],
    )
    out = from_great_expectations(path)
    assert isinstance(out[0], NullRateExpectation)
    assert abs(out[0].max_null_rate - 0.05) < 1e-9
