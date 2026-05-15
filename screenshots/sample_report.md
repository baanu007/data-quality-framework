# Sample Validation Report

This page describes what a `dq_framework` validation report looks like.
A real screenshot of the console renderer goes here once the framework is
run against a real dataset; the JSON snippet below is the canonical
machine-readable form emitted by `JsonReporter`.

## Console output (illustrative)

```
❌ FAIL — dataset='orders_sample'
  ran 8 expectation(s): 5 passed, 3 failed
  duration=2.713s
  failures by severity: CRITICAL=0, ERROR=2, WARNING=1, INFO=0

  [PASS][CRITICAL] RowCountExpectation
      Row count must be between 1 and None
      observed: row_count=100
  [FAIL][ERROR] NullRateExpectation (column=customer_id)
      Null rate of 'customer_id' must be <= 0.0
      observed: null_rate=0.0200 (nulls=2/100)
      failed sample:
        - {'order_id': 1011, 'customer_id': None, ...}
        - {'order_id': 1076, 'customer_id': None, ...}
  [FAIL][ERROR] ValueRangeExpectation (column=amount)
      Values of 'amount' must be in [0, None]
      observed: out_of_range=1/100
  [FAIL][WARNING] RegexExpectation (column=payment_method)
      Values of 'payment_method' must match regex '^(CREDIT_CARD|DEBIT_CARD|PAYPAL|APPLE_PAY)$'
      observed: mismatches=1/100
```

## JSON output (machine-readable, illustrative)

```json
{
  "dataset": "orders_sample",
  "started_at": "2026-05-15T16:00:00+00:00",
  "finished_at": "2026-05-15T16:00:02+00:00",
  "duration_seconds": 2.713,
  "success": false,
  "totals": {
    "total": 8,
    "passed": 5,
    "failed": 3,
    "by_severity": {"CRITICAL": 0, "ERROR": 2, "WARNING": 1, "INFO": 0}
  },
  "results": [
    {
      "expectation_name": "RowCountExpectation",
      "description": "Row count must be between 1 and None",
      "success": true,
      "severity": "CRITICAL",
      "observed": {"row_count": 100},
      "expected": {"min_rows": 1, "max_rows": null},
      "failed_sample": [],
      "column": null,
      "message": "row_count=100"
    },
    {
      "expectation_name": "NullRateExpectation",
      "description": "Null rate of 'customer_id' must be <= 0.0",
      "success": false,
      "severity": "ERROR",
      "observed": {"row_count": 100, "null_count": 2, "null_rate": 0.02},
      "expected": {"max_null_rate": 0.0},
      "failed_sample": [
        {"order_id": 1011, "customer_id": null}
      ],
      "column": "customer_id",
      "message": "null_rate=0.0200 (nulls=2/100)"
    }
  ],
  "metadata": {"source": "examples/data/orders_sample.csv"}
}
```

## How to capture a real screenshot

```bash
pip install -e .
python examples/validate_orders.py | tee examples/last_report.txt
# Then screenshot the terminal and drop the PNG in this folder.
```
