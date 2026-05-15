"""
dq_framework
============

A reusable PySpark-based data quality validation framework with
class-based expectations, pluggable reporters, and YAML-driven
rule sets. Optionally bridges to Great Expectations expectation
suites via :mod:`dq_framework.ge_adapter`.

Public API:

    >>> from dq_framework import (
    ...     DataQualityValidator,
    ...     ValidationReport,
    ...     ValidationResult,
    ...     Severity,
    ...     RowCountExpectation,
    ...     NullRateExpectation,
    ...     UniquenessExpectation,
    ...     ValueRangeExpectation,
    ...     RegexExpectation,
    ...     ReferentialIntegrityExpectation,
    ...     FreshnessExpectation,
    ... )
"""

from dq_framework.severity import Severity
from dq_framework.expectations import (
    Expectation,
    ValidationResult,
    RowCountExpectation,
    NullRateExpectation,
    UniquenessExpectation,
    ValueRangeExpectation,
    RegexExpectation,
    ReferentialIntegrityExpectation,
    FreshnessExpectation,
)
from dq_framework.validator import DataQualityValidator, ValidationReport
from dq_framework.reporters import (
    Reporter,
    ConsoleReporter,
    JsonReporter,
    S3Reporter,
    SlackReporter,
)
from dq_framework.config_loader import load_config, build_expectations

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # severity
    "Severity",
    # expectations
    "Expectation",
    "ValidationResult",
    "RowCountExpectation",
    "NullRateExpectation",
    "UniquenessExpectation",
    "ValueRangeExpectation",
    "RegexExpectation",
    "ReferentialIntegrityExpectation",
    "FreshnessExpectation",
    # validator
    "DataQualityValidator",
    "ValidationReport",
    # reporters
    "Reporter",
    "ConsoleReporter",
    "JsonReporter",
    "S3Reporter",
    "SlackReporter",
    # config
    "load_config",
    "build_expectations",
]
