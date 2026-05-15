"""Tests for the reporter classes."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from dq_framework.expectations import ValidationResult
from dq_framework.reporters import (
    ConsoleReporter,
    JsonReporter,
    S3Reporter,
    SlackReporter,
)
from dq_framework.severity import Severity
from dq_framework.validator import ValidationReport


def _sample_report(success: bool = False) -> ValidationReport:
    started = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    finished = datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    results = [
        ValidationResult(
            expectation_name="RowCountExpectation",
            description="rows >= 1",
            success=True,
            severity=Severity.CRITICAL,
            observed={"row_count": 5},
            expected={"min_rows": 1, "max_rows": None},
            message="row_count=5",
        ),
        ValidationResult(
            expectation_name="NullRateExpectation",
            description="no nulls in id",
            success=success,
            severity=Severity.ERROR,
            observed={
                "null_count": 0 if success else 2,
                "row_count": 5,
                "null_rate": 0.0 if success else 0.4,
            },
            expected={"max_null_rate": 0.0},
            failed_sample=[] if success else [{"id": None}],
            column="id",
            message="null_rate=0.4",
        ),
    ]
    return ValidationReport(
        dataset="orders",
        started_at=started,
        finished_at=finished,
        success=success,
        results=results,
    )


def test_console_reporter_renders(capsys):
    report = _sample_report(success=False)
    reporter = ConsoleReporter()
    reporter.report(report)
    captured = capsys.readouterr().out
    assert "FAIL" in captured
    assert "RowCountExpectation" in captured
    assert "NullRateExpectation" in captured
    assert "failed sample" in captured


def test_json_reporter_writes_file(tmp_path):
    report = _sample_report(success=True)
    out = tmp_path / "report.json"
    JsonReporter(str(out)).report(report)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["dataset"] == "orders"
    assert data["success"] is True
    assert data["totals"]["total"] == 2


def test_json_reporter_serialize_string():
    report = _sample_report(success=True)
    payload = JsonReporter().serialize(report)
    parsed = json.loads(payload)
    assert parsed["dataset"] == "orders"


def test_s3_reporter_uses_injected_client():
    calls = {}

    class FakeClient:
        def put_object(self, **kwargs):
            calls.update(kwargs)

    reporter = S3Reporter("s3://my-bucket/path/report.json", boto3_client=FakeClient())
    reporter.report(_sample_report(success=True))
    assert calls["Bucket"] == "my-bucket"
    assert calls["Key"] == "path/report.json"
    body = json.loads(calls["Body"].decode("utf-8"))
    assert body["dataset"] == "orders"


def test_s3_reporter_rejects_bad_uri():
    with pytest.raises(ValueError):
        S3Reporter("not-an-s3-uri")
    with pytest.raises(ValueError):
        S3Reporter("s3://bucket-without-key")


def test_slack_reporter_skips_when_env_missing(monkeypatch, caplog):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    called = {"count": 0}

    def fake_post(url, payload):
        called["count"] += 1

    reporter = SlackReporter(http_post=fake_post)
    reporter.report(_sample_report(success=True))
    assert called["count"] == 0


def test_slack_reporter_posts_when_env_set(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.com/webhook")
    posted = {}

    def fake_post(url, payload):
        posted["url"] = url
        posted["payload"] = payload

    reporter = SlackReporter(http_post=fake_post, channel="#dq")
    reporter.report(_sample_report(success=False))
    assert posted["url"] == "https://example.com/webhook"
    assert "Data Quality" in posted["payload"]["text"]
    assert posted["payload"]["channel"] == "#dq"


def test_slack_payload_truncates_at_ten_failures():
    started = datetime(2024, 1, 1, tzinfo=timezone.utc)
    failures = [
        ValidationResult(
            expectation_name=f"Exp{i}",
            description="d",
            success=False,
            severity=Severity.ERROR,
            message=f"m{i}",
        )
        for i in range(15)
    ]
    report = ValidationReport(
        dataset="ds",
        started_at=started,
        finished_at=started,
        success=False,
        results=failures,
    )
    payload = SlackReporter().build_payload(report)
    assert "and 5 more failure(s)" in payload["text"]
