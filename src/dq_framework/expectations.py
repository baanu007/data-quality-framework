"""Class-based data quality expectations for PySpark DataFrames.

Each expectation subclass implements :meth:`Expectation.validate`, returning
a :class:`ValidationResult`. Expectations are intentionally small, composable
objects so they can be:

* instantiated programmatically,
* serialized from YAML configs via :mod:`dq_framework.config_loader`,
* or bridged from Great Expectations suites via :mod:`dq_framework.ge_adapter`.

Design notes
------------
We deliberately favor a class hierarchy (rather than a function registry)
because:

1. Each rule carries its own typed configuration (column, threshold, etc.),
   which is easier to validate at construction time than at run time.
2. Inheriting from :class:`Expectation` lets us share a uniform contract:
   ``name``, ``severity``, ``description``, ``validate`` — and lets reporters
   render results without caring about which rule produced them.
3. Adding a new rule means subclassing one class and implementing one method,
   which is a small surface area for code review.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_framework.severity import Severity


@dataclass
class ValidationResult:
    """Outcome of a single :class:`Expectation` run against a DataFrame.

    Attributes
    ----------
    expectation_name:
        Class name of the expectation (e.g. ``"NullRateExpectation"``).
    description:
        Human-readable description of the rule.
    success:
        ``True`` when the rule passed.
    severity:
        Severity declared on the expectation.
    observed:
        Free-form dict of observed metrics (counts, rates, etc.).
    expected:
        Free-form dict of the rule's threshold/expected configuration.
    failed_sample:
        Up to N rows that violated the rule (as plain dicts).
    column:
        Column the rule targets, if any.
    message:
        Optional summary string suitable for logs/reports.
    """

    expectation_name: str
    description: str
    success: bool
    severity: Severity
    observed: Dict[str, Any] = field(default_factory=dict)
    expected: Dict[str, Any] = field(default_factory=dict)
    failed_sample: List[Dict[str, Any]] = field(default_factory=list)
    column: Optional[str] = None
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-friendly dict."""
        return {
            "expectation_name": self.expectation_name,
            "description": self.description,
            "success": self.success,
            "severity": self.severity.value,
            "observed": self.observed,
            "expected": self.expected,
            "failed_sample": self.failed_sample,
            "column": self.column,
            "message": self.message,
        }


class Expectation(ABC):
    """Abstract base class for a single data quality rule.

    Subclasses MUST implement :meth:`validate` and SHOULD override
    :attr:`description`.
    """

    #: Default severity when none is supplied at construction time.
    default_severity: Severity = Severity.ERROR

    def __init__(
        self,
        *,
        severity: Severity | str | None = None,
        description: Optional[str] = None,
        failed_sample_limit: int = 10,
    ) -> None:
        self.severity: Severity = (
            Severity.from_str(severity) if severity is not None else self.default_severity
        )
        self._description_override = description
        if failed_sample_limit < 0:
            raise ValueError("failed_sample_limit must be >= 0")
        self.failed_sample_limit = failed_sample_limit

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #

    @property
    def name(self) -> str:
        """Class name of the expectation (used in reports)."""
        return type(self).__name__

    @property
    def description(self) -> str:
        """Human-readable description of the rule."""
        return self._description_override or self._default_description()

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResult:
        """Run the rule against ``df`` and return a :class:`ValidationResult`."""

    # ------------------------------------------------------------------ #
    # Helpers for subclasses                                             #
    # ------------------------------------------------------------------ #

    def _default_description(self) -> str:
        return f"{self.name} (no description provided)"

    def _collect_failed_sample(self, failed_df: DataFrame) -> List[Dict[str, Any]]:
        if self.failed_sample_limit == 0:
            return []
        rows = failed_df.limit(self.failed_sample_limit).collect()
        return [row.asDict(recursive=True) for row in rows]

    def _require_column(self, df: DataFrame, column: str) -> None:
        if column not in df.columns:
            raise ValueError(
                f"{self.name}: column {column!r} not found in DataFrame. "
                f"Available columns: {df.columns}"
            )

    def _result(
        self,
        *,
        success: bool,
        observed: Dict[str, Any],
        expected: Dict[str, Any],
        column: Optional[str] = None,
        failed_sample: Optional[List[Dict[str, Any]]] = None,
        message: Optional[str] = None,
    ) -> ValidationResult:
        return ValidationResult(
            expectation_name=self.name,
            description=self.description,
            success=success,
            severity=self.severity,
            observed=observed,
            expected=expected,
            failed_sample=failed_sample or [],
            column=column,
            message=message,
        )


# ---------------------------------------------------------------------- #
# Concrete expectations                                                  #
# ---------------------------------------------------------------------- #


class RowCountExpectation(Expectation):
    """Assert that the DataFrame has a row count within bounds.

    Parameters
    ----------
    min_rows, max_rows:
        Inclusive bounds. Use ``None`` to leave a side open.
    """

    def __init__(
        self,
        *,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        if min_rows is None and max_rows is None:
            raise ValueError("RowCountExpectation requires min_rows or max_rows")
        if min_rows is not None and min_rows < 0:
            raise ValueError("min_rows must be >= 0")
        if max_rows is not None and max_rows < 0:
            raise ValueError("max_rows must be >= 0")
        if min_rows is not None and max_rows is not None and min_rows > max_rows:
            raise ValueError("min_rows cannot exceed max_rows")
        super().__init__(**kwargs)
        self.min_rows = min_rows
        self.max_rows = max_rows

    def _default_description(self) -> str:
        return f"Row count must be between {self.min_rows} and {self.max_rows}"

    def validate(self, df: DataFrame) -> ValidationResult:
        count = df.count()
        ok_min = self.min_rows is None or count >= self.min_rows
        ok_max = self.max_rows is None or count <= self.max_rows
        success = ok_min and ok_max
        return self._result(
            success=success,
            observed={"row_count": count},
            expected={"min_rows": self.min_rows, "max_rows": self.max_rows},
            message=f"row_count={count}",
        )


class NullRateExpectation(Expectation):
    """Assert that a column's null rate is at most ``max_null_rate``.

    Parameters
    ----------
    column:
        Name of the column to check.
    max_null_rate:
        Maximum acceptable null rate, in ``[0.0, 1.0]``. Default ``0.0``.
    """

    def __init__(
        self,
        column: str,
        *,
        max_null_rate: float = 0.0,
        **kwargs: Any,
    ) -> None:
        if not 0.0 <= max_null_rate <= 1.0:
            raise ValueError("max_null_rate must be in [0.0, 1.0]")
        super().__init__(**kwargs)
        self.column = column
        self.max_null_rate = max_null_rate

    def _default_description(self) -> str:
        return f"Null rate of {self.column!r} must be <= {self.max_null_rate}"

    def validate(self, df: DataFrame) -> ValidationResult:
        self._require_column(df, self.column)
        total = df.count()
        if total == 0:
            return self._result(
                success=True,
                observed={"row_count": 0, "null_count": 0, "null_rate": 0.0},
                expected={"max_null_rate": self.max_null_rate},
                column=self.column,
                message="empty DataFrame; null rate vacuously 0",
            )
        null_count = df.filter(F.col(self.column).isNull()).count()
        null_rate = null_count / total
        success = null_rate <= self.max_null_rate
        failed_sample: List[Dict[str, Any]] = []
        if not success:
            failed_sample = self._collect_failed_sample(
                df.filter(F.col(self.column).isNull())
            )
        return self._result(
            success=success,
            observed={
                "row_count": total,
                "null_count": null_count,
                "null_rate": null_rate,
            },
            expected={"max_null_rate": self.max_null_rate},
            column=self.column,
            failed_sample=failed_sample,
            message=f"null_rate={null_rate:.4f} (nulls={null_count}/{total})",
        )


class UniquenessExpectation(Expectation):
    """Assert that values in one or more columns are unique.

    Parameters
    ----------
    columns:
        A single column name or a list of column names forming a
        compound key.
    ignore_nulls:
        When ``True`` (default), rows where any key column is null are
        excluded from the uniqueness check.
    """

    def __init__(
        self,
        columns: str | Sequence[str],
        *,
        ignore_nulls: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if isinstance(columns, str):
            self.columns: List[str] = [columns]
        else:
            self.columns = list(columns)
        if not self.columns:
            raise ValueError("UniquenessExpectation requires at least one column")
        self.ignore_nulls = ignore_nulls

    def _default_description(self) -> str:
        cols = ", ".join(self.columns)
        return f"Values in ({cols}) must be unique"

    def validate(self, df: DataFrame) -> ValidationResult:
        for c in self.columns:
            self._require_column(df, c)
        working = df.select(*self.columns)
        if self.ignore_nulls:
            for c in self.columns:
                working = working.filter(F.col(c).isNotNull())
        total = working.count()
        grouped = working.groupBy(*self.columns).count().filter(F.col("count") > 1)
        duplicate_keys = grouped.count()
        duplicate_rows = (
            grouped.agg(F.sum("count")).collect()[0][0] if duplicate_keys else 0
        ) or 0
        success = duplicate_keys == 0
        failed_sample: List[Dict[str, Any]] = []
        if not success:
            failed_sample = self._collect_failed_sample(grouped)
        return self._result(
            success=success,
            observed={
                "row_count": total,
                "duplicate_key_count": duplicate_keys,
                "duplicate_row_count": duplicate_rows,
            },
            expected={"columns": self.columns, "ignore_nulls": self.ignore_nulls},
            column=self.columns[0] if len(self.columns) == 1 else None,
            failed_sample=failed_sample,
            message=(
                f"duplicates={duplicate_keys} key(s) covering "
                f"{duplicate_rows} row(s)"
            ),
        )


class ValueRangeExpectation(Expectation):
    """Assert that a numeric column's values fall within an inclusive range.

    Parameters
    ----------
    column:
        Column to check.
    min_value, max_value:
        Inclusive bounds. Use ``None`` to leave a side open.
    allow_nulls:
        If ``True`` (default), null values are not counted as failures.
    """

    def __init__(
        self,
        column: str,
        *,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        allow_nulls: bool = True,
        **kwargs: Any,
    ) -> None:
        if min_value is None and max_value is None:
            raise ValueError(
                "ValueRangeExpectation requires min_value or max_value"
            )
        if (
            min_value is not None
            and max_value is not None
            and min_value > max_value
        ):
            raise ValueError("min_value cannot exceed max_value")
        super().__init__(**kwargs)
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.allow_nulls = allow_nulls

    def _default_description(self) -> str:
        return (
            f"Values of {self.column!r} must be in "
            f"[{self.min_value}, {self.max_value}]"
        )

    def validate(self, df: DataFrame) -> ValidationResult:
        self._require_column(df, self.column)
        col = F.col(self.column)
        conditions = []
        if self.min_value is not None:
            conditions.append(col < F.lit(self.min_value))
        if self.max_value is not None:
            conditions.append(col > F.lit(self.max_value))
        if not self.allow_nulls:
            conditions.append(col.isNull())

        if not conditions:  # pragma: no cover - guarded by __init__
            raise RuntimeError("ValueRangeExpectation has no conditions")

        out_of_range = conditions[0]
        for cond in conditions[1:]:
            out_of_range = out_of_range | cond

        failed_df = df.filter(out_of_range)
        failed_count = failed_df.count()
        total = df.count()
        success = failed_count == 0
        failed_sample = (
            self._collect_failed_sample(failed_df) if not success else []
        )
        return self._result(
            success=success,
            observed={"row_count": total, "out_of_range_count": failed_count},
            expected={
                "min_value": self.min_value,
                "max_value": self.max_value,
                "allow_nulls": self.allow_nulls,
            },
            column=self.column,
            failed_sample=failed_sample,
            message=f"out_of_range={failed_count}/{total}",
        )


class RegexExpectation(Expectation):
    """Assert that all (non-null) values in a column match a regex.

    Parameters
    ----------
    column:
        Column to check.
    pattern:
        Python-style regex pattern. Anchored matching is the caller's
        responsibility (e.g. include ``^`` and ``$``).
    allow_nulls:
        If ``True`` (default), null values are not counted as failures.
    """

    def __init__(
        self,
        column: str,
        pattern: str,
        *,
        allow_nulls: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # Compile up front so bad patterns blow up at construction.
        re.compile(pattern)
        self.column = column
        self.pattern = pattern
        self.allow_nulls = allow_nulls

    def _default_description(self) -> str:
        return f"Values of {self.column!r} must match regex {self.pattern!r}"

    def validate(self, df: DataFrame) -> ValidationResult:
        self._require_column(df, self.column)
        col = F.col(self.column).cast("string")
        match_expr = col.rlike(self.pattern)
        if self.allow_nulls:
            bad_expr = col.isNotNull() & ~match_expr
        else:
            bad_expr = col.isNull() | ~match_expr
        failed_df = df.filter(bad_expr)
        failed_count = failed_df.count()
        total = df.count()
        success = failed_count == 0
        failed_sample = (
            self._collect_failed_sample(failed_df) if not success else []
        )
        return self._result(
            success=success,
            observed={"row_count": total, "mismatch_count": failed_count},
            expected={"pattern": self.pattern, "allow_nulls": self.allow_nulls},
            column=self.column,
            failed_sample=failed_sample,
            message=f"mismatches={failed_count}/{total}",
        )


class ReferentialIntegrityExpectation(Expectation):
    """Assert that every value in ``column`` exists in a reference column.

    Parameters
    ----------
    column:
        Foreign-key column in the DataFrame under test.
    reference_df:
        DataFrame containing the parent/reference values.
    reference_column:
        Column in ``reference_df`` to look up against.
    allow_nulls:
        If ``True`` (default), null FK values are not counted as failures.
    """

    def __init__(
        self,
        column: str,
        reference_df: DataFrame,
        reference_column: str,
        *,
        allow_nulls: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if reference_df is None:
            raise ValueError("reference_df is required")
        if reference_column not in reference_df.columns:
            raise ValueError(
                f"reference_column {reference_column!r} not in reference_df "
                f"(columns: {reference_df.columns})"
            )
        self.column = column
        self.reference_df = reference_df
        self.reference_column = reference_column
        self.allow_nulls = allow_nulls

    def _default_description(self) -> str:
        return (
            f"Every value of {self.column!r} must exist in reference "
            f"{self.reference_column!r}"
        )

    def validate(self, df: DataFrame) -> ValidationResult:
        self._require_column(df, self.column)
        left = df.alias("child")
        right = (
            self.reference_df.select(self.reference_column)
            .distinct()
            .withColumnRenamed(self.reference_column, "_ref_key")
            .alias("ref")
        )
        joined = left.join(
            right,
            F.col(f"child.{self.column}") == F.col("ref._ref_key"),
            how="left",
        )
        if self.allow_nulls:
            bad = joined.filter(
                F.col(f"child.{self.column}").isNotNull()
                & F.col("ref._ref_key").isNull()
            )
        else:
            bad = joined.filter(F.col("ref._ref_key").isNull())
        # Select original columns so failed_sample mirrors the source row.
        failed_df = bad.select("child.*")
        failed_count = failed_df.count()
        total = df.count()
        success = failed_count == 0
        failed_sample = (
            self._collect_failed_sample(failed_df) if not success else []
        )
        return self._result(
            success=success,
            observed={"row_count": total, "orphan_count": failed_count},
            expected={
                "reference_column": self.reference_column,
                "allow_nulls": self.allow_nulls,
            },
            column=self.column,
            failed_sample=failed_sample,
            message=f"orphans={failed_count}/{total}",
        )


class FreshnessExpectation(Expectation):
    """Assert that ``MAX(timestamp_column)`` is within ``max_age`` of ``now``.

    Useful as a "the pipeline ran today" check.

    Parameters
    ----------
    column:
        Timestamp (or date) column to check.
    max_age:
        Maximum allowed age of the most recent record.
    now:
        Reference "current time" — defaults to ``datetime.now(timezone.utc)``.
        Accepting an override makes the rule deterministic for tests.
    """

    def __init__(
        self,
        column: str,
        *,
        max_age: timedelta,
        now: Optional[datetime] = None,
        **kwargs: Any,
    ) -> None:
        if not isinstance(max_age, timedelta):
            raise TypeError("max_age must be a datetime.timedelta")
        if max_age.total_seconds() < 0:
            raise ValueError("max_age must be non-negative")
        super().__init__(**kwargs)
        self.column = column
        self.max_age = max_age
        self._now = now

    @property
    def now(self) -> datetime:
        return self._now or datetime.now(timezone.utc)

    def _default_description(self) -> str:
        return (
            f"MAX({self.column}) must be within {self.max_age} of the "
            "reference time"
        )

    @staticmethod
    def _coerce_to_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        # date objects expose isoformat but lack time; treat as midnight UTC.
        if hasattr(value, "isoformat"):
            try:
                return datetime.fromisoformat(value.isoformat())
            except ValueError:
                return None
        return None

    def validate(self, df: DataFrame) -> ValidationResult:
        self._require_column(df, self.column)
        row = df.agg(F.max(F.col(self.column)).alias("max_ts")).collect()[0]
        latest = self._coerce_to_datetime(row["max_ts"])
        now = self.now
        # Normalize tz-awareness to keep arithmetic safe.
        if latest is not None and latest.tzinfo is None and now.tzinfo is not None:
            latest = latest.replace(tzinfo=now.tzinfo)
        if latest is not None and latest.tzinfo is not None and now.tzinfo is None:
            now = now.replace(tzinfo=latest.tzinfo)
        if latest is None:
            return self._result(
                success=False,
                observed={"max_timestamp": None, "age_seconds": None},
                expected={"max_age_seconds": self.max_age.total_seconds()},
                column=self.column,
                message="no non-null timestamps found",
            )
        age = now - latest
        age_seconds = age.total_seconds()
        success = age <= self.max_age
        return self._result(
            success=success,
            observed={
                "max_timestamp": latest.isoformat(),
                "age_seconds": age_seconds,
            },
            expected={"max_age_seconds": self.max_age.total_seconds()},
            column=self.column,
            message=f"age_seconds={age_seconds:.2f}",
        )
