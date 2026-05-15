"""Pluggable reporters for :class:`dq_framework.ValidationReport`.

Reporters are deliberately decoupled from validation so the same report can
be sent to multiple destinations (stdout, JSON file, S3, Slack, ...). All
external integrations read their credentials from environment variables; no
secrets are baked into this code.
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from dq_framework.validator import ValidationReport

logger = logging.getLogger(__name__)


class Reporter(ABC):
    """Abstract reporter contract."""

    @abstractmethod
    def report(self, report: ValidationReport) -> None:
        """Deliver ``report`` to this reporter's destination."""


# ---------------------------------------------------------------------- #
# Console                                                                #
# ---------------------------------------------------------------------- #


class ConsoleReporter(Reporter):
    """Pretty-print a report to stdout. Useful for local runs and CI logs."""

    def __init__(self, *, show_failed_sample: bool = True, sample_rows: int = 3) -> None:
        self.show_failed_sample = show_failed_sample
        self.sample_rows = sample_rows

    def report(self, report: ValidationReport) -> None:
        print(self.render(report))

    def render(self, report: ValidationReport) -> str:
        lines = []
        status = "✅ PASS" if report.success else "❌ FAIL"
        lines.append(f"{status} — dataset={report.dataset!r}")
        lines.append(
            f"  ran {report.total} expectation(s): "
            f"{report.passed} passed, {report.failed} failed"
        )
        lines.append(f"  duration={(report.finished_at - report.started_at).total_seconds():.3f}s")
        by_sev = report.failures_by_severity()
        lines.append("  failures by severity: " + ", ".join(f"{k}={v}" for k, v in by_sev.items()))
        lines.append("")
        for r in report.results:
            mark = "PASS" if r.success else "FAIL"
            head = f"  [{mark}][{r.severity.value}] {r.expectation_name}"
            if r.column:
                head += f" (column={r.column})"
            lines.append(head)
            lines.append(f"      {r.description}")
            if r.message:
                lines.append(f"      observed: {r.message}")
            if not r.success and self.show_failed_sample and r.failed_sample:
                lines.append("      failed sample:")
                for row in r.failed_sample[: self.sample_rows]:
                    lines.append(f"        - {row}")
        return "\n".join(lines)


# ---------------------------------------------------------------------- #
# JSON file                                                              #
# ---------------------------------------------------------------------- #


class JsonReporter(Reporter):
    """Write the report as JSON to ``output_path`` (or return it as a string)."""

    def __init__(self, output_path: Optional[str] = None, *, indent: int = 2) -> None:
        self.output_path = output_path
        self.indent = indent

    def report(self, report: ValidationReport) -> None:
        payload = self.serialize(report)
        if self.output_path is None:
            print(payload)
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.output_path)) or ".", exist_ok=True)
        with open(self.output_path, "w", encoding="utf-8") as fh:
            fh.write(payload)
        logger.info("Wrote DQ report to %s", self.output_path)

    def serialize(self, report: ValidationReport) -> str:
        return json.dumps(report.to_dict(), indent=self.indent, default=str)


# ---------------------------------------------------------------------- #
# S3                                                                     #
# ---------------------------------------------------------------------- #


class S3Reporter(Reporter):
    """Upload the JSON report to ``s3://bucket/key``.

    boto3 is imported lazily so it is only required when this reporter is
    actually used. Credentials are picked up from the standard AWS chain
    (env vars, instance profile, etc.) — nothing is hardcoded here.
    """

    def __init__(
        self,
        s3_uri: str,
        *,
        boto3_client: Any = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        parsed = urlparse(s3_uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
            raise ValueError(
                f"S3Reporter requires a fully qualified s3://bucket/key URI, " f"got {s3_uri!r}"
            )
        self.bucket = parsed.netloc
        self.key = parsed.path.lstrip("/")
        self._client = boto3_client
        self.extra_args = extra_args or {}

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import boto3  # type: ignore
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise RuntimeError(
                "S3Reporter requires boto3; install with `pip install boto3`."
            ) from exc
        return boto3.client("s3")

    def report(self, report: ValidationReport) -> None:
        body = json.dumps(report.to_dict(), default=str).encode("utf-8")
        client = self._get_client()
        client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=body,
            ContentType="application/json",
            **self.extra_args,
        )
        logger.info("Uploaded DQ report to s3://%s/%s", self.bucket, self.key)


# ---------------------------------------------------------------------- #
# Slack                                                                  #
# ---------------------------------------------------------------------- #


class SlackReporter(Reporter):
    """Post a short summary to a Slack incoming webhook.

    The webhook URL is read from the environment variable named by
    ``webhook_env_var`` (default ``SLACK_WEBHOOK_URL``). If the variable is
    unset, the reporter logs a warning and skips delivery — failing-open is
    preferable to crashing the pipeline over a missing notifier.
    """

    DEFAULT_ENV = "SLACK_WEBHOOK_URL"

    def __init__(
        self,
        *,
        webhook_env_var: str = DEFAULT_ENV,
        channel: Optional[str] = None,
        http_post: Any = None,
    ) -> None:
        self.webhook_env_var = webhook_env_var
        self.channel = channel
        self._http_post = http_post

    def _post(self, url: str, payload: Dict[str, Any]) -> None:
        if self._http_post is not None:
            self._http_post(url, payload)
            return
        try:
            import requests  # type: ignore
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise RuntimeError(
                "SlackReporter requires `requests`; install with `pip install requests`."
            ) from exc
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()

    def build_payload(self, report: ValidationReport) -> Dict[str, Any]:
        emoji = "✅" if report.success else "❌"
        header = (
            f"{emoji} *Data Quality — {report.dataset}* " f"({report.passed}/{report.total} passed)"
        )
        lines = [header]
        if not report.success:
            failed = [r for r in report.results if not r.success]
            for r in failed[:10]:  # keep Slack messages compact
                lines.append(
                    f"• [{r.severity.value}] {r.expectation_name}"
                    + (f" (column={r.column})" if r.column else "")
                    + (f" — {r.message}" if r.message else "")
                )
            if len(failed) > 10:
                lines.append(f"…and {len(failed) - 10} more failure(s)")
        payload: Dict[str, Any] = {"text": "\n".join(lines)}
        if self.channel:
            payload["channel"] = self.channel
        return payload

    def report(self, report: ValidationReport) -> None:
        url = os.environ.get(self.webhook_env_var)
        if not url:
            logger.warning(
                "SlackReporter: %s is not set; skipping Slack notification.",
                self.webhook_env_var,
            )
            return
        payload = self.build_payload(report)
        self._post(url, payload)
        logger.info("Posted DQ summary to Slack")
