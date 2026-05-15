"""Tests for the YAML config loader and builder."""

from __future__ import annotations

from datetime import timedelta

import pytest

from dq_framework.config_loader import build_expectations, load_config
from dq_framework.expectations import (
    FreshnessExpectation,
    NullRateExpectation,
    RegexExpectation,
    RowCountExpectation,
    UniquenessExpectation,
    ValueRangeExpectation,
)
from dq_framework.severity import Severity


def _write(tmp_path, body: str):
    p = tmp_path / "rules.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def test_load_config_and_build_all_types(tmp_path):
    path = _write(
        tmp_path,
        """
dataset: orders
fail_on: [CRITICAL, ERROR]
expectations:
  - type: row_count
    min_rows: 1
    severity: CRITICAL
  - type: null_rate
    column: id
    max_null_rate: 0.0
  - type: uniqueness
    columns: [id]
  - type: value_range
    column: amount
    min_value: 0
  - type: regex
    column: status
    pattern: "^[A-Z_]+$"
  - type: freshness
    column: ts
    max_age_hours: 24
""",
    )
    config = load_config(path)
    assert config["dataset"] == "orders"
    expectations = build_expectations(config)
    assert len(expectations) == 6
    assert isinstance(expectations[0], RowCountExpectation)
    assert isinstance(expectations[1], NullRateExpectation)
    assert isinstance(expectations[2], UniquenessExpectation)
    assert isinstance(expectations[3], ValueRangeExpectation)
    assert isinstance(expectations[4], RegexExpectation)
    assert isinstance(expectations[5], FreshnessExpectation)
    assert expectations[0].severity == Severity.CRITICAL
    assert expectations[5].max_age == timedelta(hours=24)


def test_missing_type_raises(tmp_path):
    path = _write(
        tmp_path,
        """
expectations:
  - column: id
""",
    )
    with pytest.raises(ValueError, match="missing required key 'type'"):
        build_expectations(load_config(path))


def test_unknown_type_raises(tmp_path):
    path = _write(
        tmp_path,
        """
expectations:
  - type: nonsense
""",
    )
    with pytest.raises(ValueError, match="unknown type"):
        build_expectations(load_config(path))


def test_empty_expectations_raises(tmp_path):
    path = _write(
        tmp_path,
        """
expectations: []
""",
    )
    with pytest.raises(ValueError, match="non-empty list"):
        build_expectations(load_config(path))


def test_freshness_requires_exactly_one_duration(tmp_path):
    path = _write(
        tmp_path,
        """
expectations:
  - type: freshness
    column: ts
""",
    )
    with pytest.raises(ValueError, match="exactly one of"):
        build_expectations(load_config(path))


def test_uniqueness_accepts_singular_column(tmp_path):
    path = _write(
        tmp_path,
        """
expectations:
  - type: uniqueness
    column: id
""",
    )
    expectations = build_expectations(load_config(path))
    assert expectations[0].columns == ["id"]


def test_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nope.yaml")


def test_empty_file_raises(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        load_config(p)
