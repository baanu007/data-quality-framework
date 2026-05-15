"""Tests for the Severity enum."""

from __future__ import annotations

import pytest

from dq_framework.severity import Severity


def test_from_str_case_insensitive():
    assert Severity.from_str("critical") == Severity.CRITICAL
    assert Severity.from_str("Error") == Severity.ERROR
    assert Severity.from_str(Severity.WARNING) == Severity.WARNING


def test_from_str_invalid():
    with pytest.raises(ValueError):
        Severity.from_str("nope")


def test_rank_order():
    assert Severity.CRITICAL.rank > Severity.ERROR.rank > Severity.WARNING.rank > Severity.INFO.rank


def test_from_str_rejects_non_string():
    with pytest.raises(TypeError):
        Severity.from_str(42)  # type: ignore[arg-type]
