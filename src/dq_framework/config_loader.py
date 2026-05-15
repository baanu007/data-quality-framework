"""Load DQ rule sets from YAML and build :class:`Expectation` objects.

YAML schema (informal)::

    dataset: orders
    fail_on: [CRITICAL, ERROR]          # optional
    raise_on_critical: true             # optional
    expectations:
      - type: row_count
        min_rows: 1
        severity: CRITICAL
        description: "orders table must not be empty"
      - type: null_rate
        column: order_id
        max_null_rate: 0.0
        severity: CRITICAL
      - type: uniqueness
        columns: [order_id]
        severity: ERROR
      - type: value_range
        column: amount
        min_value: 0
        severity: ERROR
      - type: regex
        column: status
        pattern: "^(PENDING|SHIPPED|DELIVERED|COMPLETED|RETURNED)$"
        severity: WARNING
      - type: freshness
        column: order_date
        max_age_hours: 48
        severity: WARNING
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Type

import yaml

from dq_framework.expectations import (
    Expectation,
    FreshnessExpectation,
    NullRateExpectation,
    RegexExpectation,
    RowCountExpectation,
    UniquenessExpectation,
    ValueRangeExpectation,
)

# Mapping of YAML "type" → Expectation class.
_TYPE_REGISTRY: Dict[str, Type[Expectation]] = {
    "row_count": RowCountExpectation,
    "null_rate": NullRateExpectation,
    "uniqueness": UniquenessExpectation,
    "value_range": ValueRangeExpectation,
    "regex": RegexExpectation,
    "freshness": FreshnessExpectation,
}


def load_config(path: str | Path) -> Dict[str, Any]:
    """Load a YAML DQ config and return the parsed dict.

    Raises
    ------
    FileNotFoundError
        If ``path`` does not exist.
    ValueError
        If the file is empty or the top-level value is not a mapping.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"DQ config not found: {p}")
    with p.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if data is None:
        raise ValueError(f"DQ config {p} is empty")
    if not isinstance(data, dict):
        raise ValueError(f"DQ config {p} must be a mapping at the top level, got {type(data)!r}")
    return data


def build_expectations(config: Dict[str, Any]) -> List[Expectation]:
    """Translate a parsed config dict into a list of :class:`Expectation`."""
    raw = config.get("expectations")
    if not isinstance(raw, list) or not raw:
        raise ValueError("config['expectations'] must be a non-empty list")

    expectations: List[Expectation] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"expectations[{idx}] must be a mapping, got {type(item)!r}")
        # Copy so we can pop keys without mutating the caller's data.
        spec = dict(item)
        type_key = spec.pop("type", None)
        if not type_key:
            raise ValueError(f"expectations[{idx}] is missing required key 'type'")
        if type_key not in _TYPE_REGISTRY:
            valid = ", ".join(sorted(_TYPE_REGISTRY))
            raise ValueError(f"expectations[{idx}]: unknown type {type_key!r}. Valid: {valid}")
        try:
            expectations.append(_build_one(type_key, spec))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"expectations[{idx}] ({type_key}): {exc}") from exc
    return expectations


# ---------------------------------------------------------------------- #
# Per-type builders                                                      #
# ---------------------------------------------------------------------- #


def _build_one(type_key: str, spec: Dict[str, Any]) -> Expectation:
    """Dispatch to the right per-type builder.

    Most expectations can be constructed by simply forwarding kwargs, but
    a few (notably ``freshness``) need translation (e.g. ``max_age_hours``
    → ``timedelta(hours=...)``).
    """
    cls = _TYPE_REGISTRY[type_key]
    if type_key == "freshness":
        return _build_freshness(spec)
    if type_key == "uniqueness":
        # Allow `column` (singular) or `columns` (list)
        if "column" in spec and "columns" not in spec:
            spec["columns"] = spec.pop("column")
        return cls(**spec)
    return cls(**spec)


def _build_freshness(spec: Dict[str, Any]) -> FreshnessExpectation:
    # Accept either max_age_hours/minutes/seconds/days or raw max_age_seconds.
    duration_keys = {
        "max_age_seconds": "seconds",
        "max_age_minutes": "minutes",
        "max_age_hours": "hours",
        "max_age_days": "days",
    }
    found = [k for k in duration_keys if k in spec]
    if len(found) != 1:
        raise ValueError(
            "freshness expectation requires exactly one of " "max_age_seconds/minutes/hours/days"
        )
    key = found[0]
    value = spec.pop(key)
    if not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric, got {type(value)!r}")
    spec["max_age"] = timedelta(**{duration_keys[key]: value})
    return FreshnessExpectation(**spec)
