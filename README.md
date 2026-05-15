# 🔍 Data Quality Framework

A reusable **PySpark** data quality validation framework with class-based
expectations, YAML-driven rule sets, pluggable reporters (console, JSON, S3,
Slack), and an optional **Great Expectations** adapter for importing existing
expectation suites.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6F00?style=for-the-badge)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)

---

## ✨ What it does

* Define data quality rules as small, typed **Python objects** or in **YAML**.
* Run them against any **PySpark DataFrame** (local, EMR, Databricks,
  Glue — anywhere Spark runs).
* Get a structured **`ValidationReport`** — pass/fail counts, observed
  metrics, failed-row samples, and per-rule severity.
* Ship the report to one or more **reporters**: `ConsoleReporter`,
  `JsonReporter`, `S3Reporter`, `SlackReporter`.
* Optionally **import existing Great Expectations suites** via
  `dq_framework.ge_adapter.from_great_expectations(...)`.

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                       INPUT (Spark DataFrame)                         │
└───────────────────────────────────┬──────────────────────────────────┘
                                    │
                                    ▼
        ┌───────────────────────────────────────────────────────┐
        │              Expectation classes (rules)               │
        │  RowCount · NullRate · Uniqueness · ValueRange         │
        │  Regex · ReferentialIntegrity · Freshness              │
        │  …each yields a ValidationResult                       │
        └───────────────────────────────────┬───────────────────┘
                                            │
                                            ▼
        ┌───────────────────────────────────────────────────────┐
        │            DataQualityValidator (orchestrator)        │
        │  • Runs every expectation                             │
        │  • Honours severity (CRITICAL/ERROR/WARNING/INFO)     │
        │  • Aggregates into a ValidationReport                 │
        └───────────────────────────────────┬───────────────────┘
                                            │
                ┌───────────────────────────┼───────────────────────────┐
                ▼                           ▼                           ▼
       ┌────────────────┐         ┌────────────────┐         ┌────────────────┐
       │ ConsoleReporter│         │ JsonReporter   │         │ S3Reporter /   │
       │  (stdout)      │         │ (file / stdout)│         │ SlackReporter  │
       └────────────────┘         └────────────────┘         └────────────────┘
```

## 📦 Project layout

```
data-quality-framework/
├── src/dq_framework/
│   ├── __init__.py             # public API
│   ├── severity.py             # Severity enum + CriticalDataQualityError
│   ├── expectations.py         # Expectation base + concrete rules
│   ├── validator.py            # DataQualityValidator + ValidationReport
│   ├── reporters.py            # ConsoleReporter, JsonReporter, S3Reporter, SlackReporter
│   ├── config_loader.py        # YAML → list[Expectation]
│   ├── runner.py               # python -m dq_framework run …
│   └── ge_adapter.py           # bridge from Great Expectations suites
├── configs/
│   ├── orders.yaml
│   └── customers.yaml
├── examples/
│   ├── validate_orders.py
│   └── data/orders_sample.csv  # ~100 rows w/ seeded DQ issues
├── tests/                      # pytest suite (Spark fixture in conftest.py)
├── screenshots/sample_report.md
├── dags/dq_validation_dag.py   # legacy Airflow DAG (uses Great Expectations directly)
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── .github/workflows/ci.yml
├── .pre-commit-config.yaml
└── LICENSE
```

## 🚀 Quick start

### Install

```bash
git clone https://github.com/baanu007/data-quality-framework.git
cd data-quality-framework
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
```

Optional extras: `pip install -e ".[s3]"` for `S3Reporter`,
`".[slack]"` for `SlackReporter`, `".[great_expectations]"` for the GE adapter.

### Run the bundled example

```bash
python examples/validate_orders.py
```

That loads `examples/data/orders_sample.csv`, applies the rules from
`configs/orders.yaml`, prints a console report, and writes
`examples/last_report.json`. The sample data has seeded issues
(null `customer_id`, negative `amount`, unknown enum values, a duplicate
`order_id`) so the run is expected to **fail** — that's how you can see
every code path.

### Programmatic use

```python
from pyspark.sql import SparkSession
from dq_framework import (
    DataQualityValidator,
    RowCountExpectation,
    NullRateExpectation,
    UniquenessExpectation,
    ValueRangeExpectation,
    ConsoleReporter,
)
from dq_framework.severity import Severity

spark = SparkSession.builder.master("local[2]").getOrCreate()
df = spark.read.option("header", True).csv("orders.csv")

validator = DataQualityValidator(
    [
        RowCountExpectation(min_rows=1, severity=Severity.CRITICAL),
        NullRateExpectation("order_id", max_null_rate=0.0, severity=Severity.CRITICAL),
        UniquenessExpectation("order_id", severity=Severity.CRITICAL),
        ValueRangeExpectation("amount", min_value=0, severity=Severity.ERROR),
    ],
    dataset_name="orders",
)
report = validator.run(df)
ConsoleReporter().report(report)
```

### CLI

```bash
python -m dq_framework run \
    --config configs/orders.yaml \
    --table  examples/data/orders_sample.csv \
    --format csv --header --infer-schema \
    --report-json out/orders_report.json
```

Exit codes: `0` (pass), `1` (failure), `2` (CRITICAL failure aborted the run).

### YAML rule set

```yaml
dataset: orders
fail_on: [CRITICAL, ERROR]
raise_on_critical: true

expectations:
  - type: row_count
    min_rows: 1
    severity: CRITICAL

  - type: null_rate
    column: order_id
    max_null_rate: 0.0
    severity: CRITICAL

  - type: uniqueness
    columns: [order_id]
    severity: CRITICAL

  - type: value_range
    column: amount
    min_value: 0
    severity: ERROR

  - type: regex
    column: status
    pattern: "^(PENDING|SHIPPED|DELIVERED|COMPLETED|RETURNED)$"
    severity: ERROR

  - type: freshness
    column: order_date
    max_age_hours: 48
    severity: WARNING
```

## 🧩 Built-in expectations

| Expectation                          | What it checks                                                |
|--------------------------------------|---------------------------------------------------------------|
| `RowCountExpectation`                | Row count is within `[min_rows, max_rows]`                    |
| `NullRateExpectation`                | A column's null rate is ≤ `max_null_rate`                     |
| `UniquenessExpectation`              | Column (or compound key) is unique                            |
| `ValueRangeExpectation`              | Numeric column values lie within `[min, max]`                 |
| `RegexExpectation`                   | String column matches a regex pattern                         |
| `ReferentialIntegrityExpectation`    | Every FK value exists in a reference DataFrame                |
| `FreshnessExpectation`               | `MAX(timestamp)` is within `max_age` of "now"                 |

Each expectation has a uniform contract — `validate(df) -> ValidationResult` —
so adding a new rule is a one-class change. Every result carries an
**observed metrics** dict, an **expected/threshold** dict, and a small
**failed-row sample** (configurable per expectation).

## 🚦 Severity

```
CRITICAL > ERROR > WARNING > INFO
```

* **CRITICAL** failures raise `CriticalDataQualityError` and abort the run
  (configurable via `raise_on_critical=False`).
* **ERROR** / **WARNING** failures mark the report as failed but the run
  continues.
* **INFO** is purely informational — never fails the run.
* The validator's `fail_on=(...)` controls which severities count as failure
  for the report's overall `success` flag.

## 📡 Reporters

* `ConsoleReporter` — human-readable summary, useful for local runs and CI.
* `JsonReporter` — writes the full report to a JSON file (or stdout).
* `S3Reporter` — uploads the JSON to `s3://bucket/key`. Reads credentials
  from the standard AWS chain; **no secrets are hardcoded**.
* `SlackReporter` — posts a short summary to an incoming webhook URL read
  from `$SLACK_WEBHOOK_URL`. Fails open if the env var is missing.

You can attach as many reporters as you want; failures in one reporter
don't take the others down.

## 🔌 Great Expectations adapter (optional)

If you already have GE expectation suites, you can import the supported
subset directly:

```python
from dq_framework.ge_adapter import from_great_expectations
from dq_framework import DataQualityValidator

expectations = from_great_expectations("path/to/suite.json")
report = DataQualityValidator(expectations, dataset_name="orders").run(df)
```

Supported GE expectation types:

* `expect_table_row_count_to_be_between`
* `expect_column_values_to_not_be_null` (honours `mostly`)
* `expect_column_values_to_be_unique`
* `expect_column_values_to_be_between`
* `expect_column_values_to_match_regex`
* `expect_column_values_to_be_in_set`

Unsupported types are skipped with a warning rather than crashing the run.

## 🧪 Tests

```bash
pip install -e ".[dev]"
pytest --cov=dq_framework
```

The suite spins up a local `SparkSession` once (session-scoped fixture in
`tests/conftest.py`) and exercises:

* One passing + one failing test per expectation class.
* Validator orchestration (severities, raise-on-critical, exception capture).
* Reporters (console rendering, JSON serialization, S3 client injection,
  Slack env-var gating).
* Config loader (every rule type, error paths).
* Great Expectations adapter translation.

## 🛠️ Tech stack

| Component            | Tech                       |
|----------------------|----------------------------|
| Compute              | Apache Spark (PySpark ≥3.3) |
| Config               | PyYAML                     |
| GE bridge (optional) | Great Expectations         |
| Reporters (optional) | boto3 (S3), requests (Slack) |
| Tests / Lint         | pytest, pytest-cov, black, isort, flake8, pre-commit |
| CI                   | GitHub Actions             |

## 📄 License

MIT — see [LICENSE](LICENSE).

---

*A small framework, written to be read. Every rule is one class; every
report is one dict; every reporter is one method.*
