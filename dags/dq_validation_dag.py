"""
Data Quality Validation DAG
Runs daily data quality checks on critical tables
"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-team@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Tables to validate
VALIDATION_CONFIG = {
    "raw.orders": {
        "suite": "orders_suite",
        "priority": "critical",
        "checkpoint": "orders_checkpoint"
    },
    "raw.customers": {
        "suite": "customers_suite", 
        "priority": "critical",
        "checkpoint": "customers_checkpoint"
    },
    "raw.products": {
        "suite": "products_suite",
        "priority": "high",
        "checkpoint": "products_checkpoint"
    },
    "raw.inventory": {
        "suite": "inventory_suite",
        "priority": "medium",
        "checkpoint": "inventory_checkpoint"
    }
}


def run_validation(table: str, config: Dict, **context) -> Dict:
    """
    Run Great Expectations validation for a table
    
    Args:
        table: Table name to validate
        config: Validation configuration
        
    Returns:
        Validation results dictionary
    """
    import sys
    sys.path.append("/opt/airflow/dags/repo/src")
    
    from validators.data_quality_validator import DataQualityValidator
    
    validator = DataQualityValidator()
    result = validator.run_checkpoint(config["checkpoint"])
    
    # Push results to XCom
    context["ti"].xcom_push(key=f"{table}_result", value={
        "table": table,
        "success": result.success,
        "statistics": result.statistics,
        "failed_count": len(result.failed_expectations),
        "priority": config["priority"],
        "run_time": result.run_time.isoformat()
    })
    
    if not result.success and config["priority"] == "critical":
        raise ValueError(f"Critical validation failed for {table}")
    
    return result.__dict__


def aggregate_results(**context) -> Dict:
    """Aggregate all validation results"""
    ti = context["ti"]
    
    results = []
    for table in VALIDATION_CONFIG.keys():
        result = ti.xcom_pull(key=f"{table}_result")
        if result:
            results.append(result)
    
    summary = {
        "total_tables": len(results),
        "passed": sum(1 for r in results if r["success"]),
        "failed": sum(1 for r in results if not r["success"]),
        "critical_failures": sum(
            1 for r in results 
            if not r["success"] and r["priority"] == "critical"
        ),
        "run_date": datetime.now().isoformat()
    }
    
    ti.xcom_push(key="validation_summary", value=summary)
    return summary


def format_slack_message(**context) -> str:
    """Format Slack notification message"""
    ti = context["ti"]
    summary = ti.xcom_pull(key="validation_summary")
    
    if summary["failed"] == 0:
        emoji = "✅"
        status = "All Passed"
    elif summary["critical_failures"] > 0:
        emoji = "🚨"
        status = "Critical Failures"
    else:
        emoji = "⚠️"
        status = "Some Failures"
    
    message = f"""
{emoji} *Data Quality Report - {summary['run_date'][:10]}*

*Status:* {status}

📊 *Summary*
• Tables Validated: {summary['total_tables']}
• Passed: {summary['passed']}
• Failed: {summary['failed']}
• Critical Failures: {summary['critical_failures']}

<https://datadocs.company.com|View Full Report>
"""
    return message


# Define DAG
with DAG(
    dag_id="data_quality_validation",
    default_args=default_args,
    description="Daily data quality validation pipeline",
    schedule_interval="0 6 * * *",  # 6 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-quality", "validation"],
) as dag:
    
    start = DummyOperator(task_id="start")
    
    # Create validation tasks for each table
    with TaskGroup("validate_tables") as validate_group:
        validation_tasks = []
        
        for table, config in VALIDATION_CONFIG.items():
            task = PythonOperator(
                task_id=f"validate_{table.replace('.', '_')}",
                python_callable=run_validation,
                op_kwargs={"table": table, "config": config},
                provide_context=True
            )
            validation_tasks.append(task)
    
    # Aggregate results
    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=aggregate_results,
        provide_context=True
    )
    
    # Send Slack notification
    notify_slack = SlackWebhookOperator(
        task_id="notify_slack",
        slack_webhook_conn_id="slack_webhook",
        message="{{ ti.xcom_pull(task_ids='format_message') }}",
        channel="#data-quality-alerts"
    )
    
    format_message = PythonOperator(
        task_id="format_message",
        python_callable=format_slack_message,
        provide_context=True
    )
    
    end = DummyOperator(task_id="end")
    
    # Define task dependencies
    start >> validate_group >> aggregate >> format_message >> notify_slack >> end
