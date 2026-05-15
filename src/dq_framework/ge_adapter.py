"""Bridge a (subset of) Great Expectations suites into ``dq_framework``.

This adapter is intentionally narrow. Great Expectations is a large library
and faithfully replicating every expectation is out of scope. Instead, we
translate the most common expectation types in a GE expectation suite into
their ``dq_framework`` equivalents. Anything we don't recognise is skipped
with a warning, so a partial suite still produces useful validation.

Supported GE expectation types
------------------------------
* ``expect_table_row_count_to_be_between``
* ``expect_column_values_to_not_be_null``      → ``NullRateExpectation(0.0)``
* ``expect_column_values_to_be_unique``        → ``UniquenessExpectation``
* ``expect_column_values_to_be_between``       → ``ValueRangeExpectation``
* ``expect_column_values_to_match_regex``      → ``RegexExpectation``
* ``expect_column_values_to_be_in_set``        → ``RegexExpectation`` over the set

The factory :func:`from_great_expectations` accepts the path to a JSON
expectation suite (the format GE writes to disk) and returns a list of
``Expectation`` objects.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from dq_framework.expectations import (
    Expectation,
    NullRateExpectation,
    RegexExpectation,
    RowCountExpectation,
    UniquenessExpectation,
    ValueRangeExpectation,
)
from dq_framework.severity import Severity

logger = logging.getLogger(__name__)


def _translate_expectation(
    expectation_type: str,
    kwargs: Dict[str, Any],
    default_severity: Severity,
) -> Optional[Expectation]:
    """Translate a single GE expectation into a dq_framework Expectation."""
    if expectation_type == "expect_table_row_count_to_be_between":
        return RowCountExpectation(
            min_rows=kwargs.get("min_value"),
            max_rows=kwargs.get("max_value"),
            severity=default_severity,
            description=f"GE: {expectation_type}",
        )
    if expectation_type == "expect_column_values_to_not_be_null":
        column = kwargs["column"]
        mostly = float(kwargs.get("mostly", 1.0))
        # "mostly=0.95" means 95% must be non-null → null rate ≤ 5%.
        max_null_rate = max(0.0, min(1.0, 1.0 - mostly))
        return NullRateExpectation(
            column,
            max_null_rate=max_null_rate,
            severity=default_severity,
            description=f"GE: {expectation_type}({column}, mostly={mostly})",
        )
    if expectation_type == "expect_column_values_to_be_unique":
        return UniquenessExpectation(
            kwargs["column"],
            severity=default_severity,
            description=f"GE: {expectation_type}({kwargs['column']})",
        )
    if expectation_type == "expect_column_values_to_be_between":
        return ValueRangeExpectation(
            kwargs["column"],
            min_value=kwargs.get("min_value"),
            max_value=kwargs.get("max_value"),
            severity=default_severity,
            description=f"GE: {expectation_type}({kwargs['column']})",
        )
    if expectation_type == "expect_column_values_to_match_regex":
        return RegexExpectation(
            kwargs["column"],
            kwargs["regex"],
            severity=default_severity,
            description=f"GE: {expectation_type}({kwargs['column']})",
        )
    if expectation_type == "expect_column_values_to_be_in_set":
        value_set = kwargs.get("value_set") or []
        if not value_set:
            return None
        pattern = "^(?:" + "|".join(re.escape(str(v)) for v in value_set) + ")$"
        return RegexExpectation(
            kwargs["column"],
            pattern,
            severity=default_severity,
            description=(f"GE: {expectation_type}({kwargs['column']}, " f"value_set={value_set})"),
        )
    return None


def from_great_expectations(
    suite_path: str | Path,
    *,
    default_severity: Severity | str = Severity.ERROR,
) -> List[Expectation]:
    """Build a list of :class:`Expectation` objects from a GE suite JSON file.

    Parameters
    ----------
    suite_path:
        Path to a Great Expectations expectation suite file (JSON).
    default_severity:
        Severity to assign to every translated expectation. GE doesn't have
        an exact equivalent so we apply a single default.

    Unsupported expectation types are skipped with a warning rather than
    raising — partial coverage is more useful than nothing.
    """
    severity = Severity.from_str(default_severity)
    path = Path(suite_path)
    with path.open("r", encoding="utf-8") as fh:
        suite = json.load(fh)
    raw_expectations = suite.get("expectations") or []
    out: List[Expectation] = []
    for entry in raw_expectations:
        etype = entry.get("expectation_type")
        ekwargs = entry.get("kwargs") or {}
        if not etype:
            continue
        try:
            translated = _translate_expectation(etype, ekwargs, severity)
        except (KeyError, ValueError) as exc:
            logger.warning("Skipping GE expectation %r (%s): %s", etype, ekwargs, exc)
            continue
        if translated is None:
            logger.warning(
                "GE expectation %r is not supported by dq_framework; skipping.",
                etype,
            )
            continue
        out.append(translated)
    return out
