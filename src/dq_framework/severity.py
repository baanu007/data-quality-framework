"""Severity levels for data quality expectations."""

from __future__ import annotations

from enum import Enum


class Severity(str, Enum):
    """Severity of a data quality expectation.

    The values are strings so they serialize cleanly to JSON and YAML.

    Order (most -> least severe):

    * ``CRITICAL`` — failure raises :class:`CriticalDataQualityError`
      and aborts the validation run.
    * ``ERROR`` — failure marks the report as failed but the run continues.
    * ``WARNING`` — failure is logged at WARNING level; report still marked
      as failed unless ``fail_on=("CRITICAL", "ERROR")`` is configured.
    * ``INFO`` — purely informational; never marks the report as failed.
    """

    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"

    @classmethod
    def from_str(cls, value: str | "Severity") -> "Severity":
        """Parse a severity from a case-insensitive string."""
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise TypeError(f"Severity must be a string or Severity, got {type(value)!r}")
        try:
            return cls[value.strip().upper()]
        except KeyError as exc:
            valid = ", ".join(s.value for s in cls)
            raise ValueError(
                f"Unknown severity {value!r}. Valid values: {valid}."
            ) from exc

    @property
    def rank(self) -> int:
        """Numeric rank where higher == more severe (CRITICAL=3, INFO=0)."""
        return {
            Severity.INFO: 0,
            Severity.WARNING: 1,
            Severity.ERROR: 2,
            Severity.CRITICAL: 3,
        }[self]


class CriticalDataQualityError(RuntimeError):
    """Raised when a CRITICAL expectation fails during validation."""
