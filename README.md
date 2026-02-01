# 🔍 Data Quality Framework

A comprehensive data quality validation framework using **Great Expectations**, **Python**, and **Apache Airflow** for automated data testing and monitoring.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6F00?style=for-the-badge)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)

## 📋 Overview

This framework provides automated, scalable data quality validation with:

- **Schema Validation**: Ensure data structure consistency
- **Business Rules**: Custom validation logic for domain-specific checks
- **Anomaly Detection**: Statistical checks for data drift
- **Automated Alerting**: Slack/Email notifications on failures
- **Data Docs**: Auto-generated documentation and reports

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│   │  Snowflake  │  │     S3      │  │   Postgres  │  │     API     │   │
│   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘   │
└──────────┼────────────────┼────────────────┼────────────────┼──────────┘
           │                │                │                │
           ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    GREAT EXPECTATIONS CORE                               │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Data Sources → Expectation Suites → Checkpoints → Validation    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │ Schema Checks   │  │ Statistical     │  │ Business Rules  │         │
│  │ • Column types  │  │ • Null rates    │  │ • Referential   │         │
│  │ • Required cols │  │ • Value ranges  │  │ • Custom SQL    │         │
│  │ • Uniqueness    │  │ • Distributions │  │ • Cross-table   │         │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DATA DOCS     │    │   ALERTING      │    │   METRICS       │
│  HTML Reports   │    │  Slack/Email    │    │  Prometheus     │
│  Validation     │    │  PagerDuty      │    │  CloudWatch     │
│  History        │    │  SNS            │    │  Dashboards     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📁 Project Structure

```
data-quality-framework/
├── great_expectations/
│   ├── expectations/           # Expectation suites
│   │   ├── orders_suite.json
│   │   ├── customers_suite.json
│   │   └── products_suite.json
│   ├── checkpoints/           # Validation checkpoints
│   ├── plugins/               # Custom expectations
│   ├── uncommitted/           # Local configs
│   └── great_expectations.yml
├── src/
│   ├── validators/            # Custom validators
│   │   ├── schema_validator.py
│   │   ├── business_rules.py
│   │   └── anomaly_detector.py
│   ├── alerts/                # Alerting integrations
│   │   ├── slack_notifier.py
│   │   └── email_notifier.py
│   ├── metrics/               # Metrics collection
│   └── utils/
├── dags/                      # Airflow DAGs
│   ├── dq_validation_dag.py
│   └── dq_reporting_dag.py
├── tests/
├── config/
│   └── dq_config.yaml
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/baanu007/data-quality-framework.git
cd data-quality-framework

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Initialize Great Expectations
great_expectations init
```

### Define Expectations

```python
# Create expectation suite for orders table
import great_expectations as gx

context = gx.get_context()

# Create expectation suite
suite = context.add_expectation_suite("orders_suite")

# Add expectations
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="orders_suite"
)

validator.expect_column_to_exist("order_id")
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_be_between("quantity", min_value=1, max_value=100)
validator.expect_column_values_to_be_in_set("status", ["PENDING", "SHIPPED", "DELIVERED"])

validator.save_expectation_suite()
```

### Run Validation

```bash
# Run checkpoint
great_expectations checkpoint run orders_checkpoint

# View Data Docs
great_expectations docs build
```

## 📊 Expectation Types

### Schema Validations
```yaml
- expect_column_to_exist
- expect_column_values_to_be_of_type
- expect_table_column_count_to_equal
- expect_table_columns_to_match_ordered_list
```

### Statistical Validations
```yaml
- expect_column_values_to_not_be_null
- expect_column_values_to_be_unique
- expect_column_values_to_be_between
- expect_column_mean_to_be_between
- expect_column_stdev_to_be_between
```

### Business Rules
```yaml
- expect_column_pair_values_A_to_be_greater_than_B
- expect_compound_columns_to_be_unique
- expect_column_values_to_match_regex
- expect_column_values_to_be_in_set
```

## 🔧 Configuration

```yaml
# config/dq_config.yaml
datasources:
  snowflake:
    type: snowflake
    credentials:
      account: ${SNOWFLAKE_ACCOUNT}
      user: ${SNOWFLAKE_USER}
      warehouse: COMPUTE_WH
      database: ANALYTICS
      
validation:
  fail_threshold: 0.95  # 95% pass rate required
  critical_tables:
    - orders
    - customers
    - payments
    
alerting:
  slack:
    webhook_url: ${SLACK_WEBHOOK_URL}
    channel: "#data-quality-alerts"
  email:
    smtp_server: smtp.gmail.com
    recipients:
      - data-team@company.com

metrics:
  prometheus:
    enabled: true
    port: 9090
```

## 🛠️ Technologies

| Component | Technology |
|-----------|------------|
| Validation Engine | Great Expectations |
| Orchestration | Apache Airflow |
| Alerting | Slack, Email, PagerDuty |
| Metrics | Prometheus, Grafana |
| Data Sources | Snowflake, S3, PostgreSQL |

## 📄 License

MIT License

---

*Ensuring data quality at every step of the pipeline*
