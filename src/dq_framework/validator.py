"""High-level orchestrator that runs expectations and produces a report."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame

from dq_framework.expectations import Expectation, ValidationResult
from dq_framework.severity import CriticalDataQualityError, Severity

logger = logging.getLogger(__name__)


# Severities that, by default, mark a run as failed.
_DEFAULT_FAIL_ON: Tuple[Severity, ...] = (Severity.CRITICAL, Severity.ERROR, Severity.WARNING)


@dataclass
class ValidationReport:
    """Aggregate report produced by :class:`DataQualityValidator`.

    Attributes
    ----------
    dataset:
        Logical name of the dataset being validated.
    started_at, finished_at:
        UTC timestamps bracketing the run.
    success:
        Overall pass/fail of the run (computed from results and ``fail_on``).
    results:
        Per-expectation :class:`ValidationResult` objects.
    """

    dataset: str
    started_at: datetime
    finished_at: datetime
    success: bool
    results: List[ValidationResult] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    # Convenience aggregates                                              #
    # ------------------------------------------------------------------ #

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.success)

    def failures_by_severity(self) -> Dict[str, int]:
        out: Dict[str, int] = {s.value: 0 for s in Severity}
        for r in self.results:
            if not r.success:
                out[r.severity.value] += 1
        return out

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-friendly dict."""
        return {
            "dataset": self.dataset,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": (self.finished_at - self.started_at).total_seconds(),
            "success": self.success,
            "totals": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "by_severity": self.failures_by_severity(),
            },
            "results": [r.to_dict() for r in self.results],
            "metadata": self.metadata,
        }


class DataQualityValidator:
    """Run a list of :class:`Expectation` objects against a Spark DataFrame.

    Parameters
    ----------
    expectations:
        Sequence of expectations to evaluate.
    dataset_name:
        Logical name used in the report and in log messages.
    fail_on:
        Severities that count as failure when computing the report's
        overall ``success``. Defaults to CRITICAL/ERROR/WARNING.
        INFO is informational only.
    raise_on_critical:
        If ``True`` (default), a CRITICAL failure raises
        :class:`CriticalDataQualityError` immediately. Otherwise the run
        continues and the report records the failure.

    Example
    -------
    >>> from dq_framework import (
    ...     DataQualityValidator, RowCountExpectation, NullRateExpectation
    ... )
    >>> validator = DataQualityValidator(
    ...     [
    ...         RowCountExpectation(min_rows=1),
    ...         NullRateExpectation("id", max_null_rate=0.0),
    ...     ],
    ...     dataset_name="orders",
    ... )
    >>> report = validator.run(df)  # doctest: +SKIP
    """

    def __init__(
        self,
        expectations: Sequence[Expectation],
        *,
        dataset_name: str = "dataset",
        fail_on: Iterable[Severity | str] = _DEFAULT_FAIL_ON,
        raise_on_critical: bool = True,
    ) -> None:
        if not expectations:
            raise ValueError("DataQualityValidator requires at least one expectation")
        bad = [e for e in expectations if not isinstance(e, Expectation)]
        if bad:
            raise TypeError(f"All expectations must be Expectation instances, got: {bad!r}")
        self.expectations: List[Expectation] = list(expectations)
        self.dataset_name = dataset_name
        self.fail_on: Tuple[Severity, ...] = tuple(Severity.from_str(s) for s in fail_on)
        self.raise_on_critical = raise_on_critical

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #

    def run(
        self,
        df: DataFrame,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ValidationReport:
        """Evaluate every expectation against ``df`` and return a report."""
        if df is None:
            raise ValueError("df must not be None")

        started = datetime.now(timezone.utc)
        results: List[ValidationResult] = []
        logger.info(
            "Starting DQ run for dataset=%s with %d expectation(s)",
            self.dataset_name,
            len(self.expectations),
        )
        for expectation in self.expectations:
            result = self._safe_validate(expectation, df)
            results.append(result)
            self._log_result(result)
            if (
                not result.success
                and result.severity == Severity.CRITICAL
                and self.raise_on_critical
            ):
                finished = datetime.now(timezone.utc)
                report = ValidationReport(
                    dataset=self.dataset_name,
                    started_at=started,
                    finished_at=finished,
                    success=False,
                    results=results,
                    metadata=metadata or {},
                )
                raise CriticalDataQualityError(
                    f"CRITICAL DQ failure in {self.dataset_name!r}: "
                    f"{result.expectation_name} — {result.message}"
                ) from None

        finished = datetime.now(timezone.utc)
        success = self._compute_overall_success(results)
        return ValidationReport(
            dataset=self.dataset_name,
            started_at=started,
            finished_at=finished,
            success=success,
            results=results,
            metadata=metadata or {},
        )

    # ------------------------------------------------------------------ #
    # Internals                                                          #
    # ------------------------------------------------------------------ #

    def _safe_validate(self, expectation: Expectation, df: DataFrame) -> ValidationResult:
        try:
            return expectation.validate(df)
        except Exception as exc:  # noqa: BLE001 - we want to capture anything
            logger.exception("Expectation %s raised; recording as failure", expectation.name)
            return ValidationResult(
                expectation_name=expectation.name,
                description=expectation.description,
                success=False,
                severity=expectation.severity,
                observed={"error": repr(exc)},
                expected={},
                failed_sample=[],
                column=getattr(expectation, "column", None),
                message=f"expectation raised: {exc}",
            )

    def _log_result(self, result: ValidationResult) -> None:
        if result.success:
            logger.info(
                "[PASS][%s] %s — %s",
                result.severity.value,
                result.expectation_name,
                result.message or "",
            )
            return
        log_fn = {
            Severity.CRITICAL: logger.error,
            Severity.ERROR: logger.error,
            Severity.WARNING: logger.warning,
            Severity.INFO: logger.info,
        }[result.severity]
        log_fn(
            "[FAIL][%s] %s — %s",
            result.severity.value,
            result.expectation_name,
            result.message or "",
        )

    def _compute_overall_success(self, results: List[ValidationResult]) -> bool:
        fail_levels = {s for s in self.fail_on}
        return not any((not r.success) and (r.severity in fail_levels) for r in results)
