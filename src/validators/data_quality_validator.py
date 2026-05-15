"""
Data Quality Validator
Core validation engine using Great Expectations
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation run"""

    suite_name: str
    success: bool
    statistics: Dict[str, Any]
    failed_expectations: List[Dict]
    run_time: datetime
    docs_url: Optional[str] = None


class DataQualityValidator:
    """
    Orchestrates data quality validation using Great Expectations
    """

    def __init__(self, context_root_dir: str = None):
        """
        Initialize validator with GX context

        Args:
            context_root_dir: Path to great_expectations directory
        """
        self.context_root_dir = context_root_dir or os.path.join(
            os.path.dirname(__file__), "..", "..", "great_expectations"
        )
        self.context = gx.get_context(context_root_dir=self.context_root_dir)
        logger.info(f"Initialized GX context from {self.context_root_dir}")

    def validate_dataframe(
        self, df, expectation_suite_name: str, data_asset_name: str = "runtime_data"
    ) -> ValidationResult:
        """
        Validate a pandas DataFrame against an expectation suite

        Args:
            df: Pandas DataFrame to validate
            expectation_suite_name: Name of the expectation suite
            data_asset_name: Name for the data asset

        Returns:
            ValidationResult with validation details
        """
        logger.info(f"Validating DataFrame against suite: {expectation_suite_name}")

        # Create runtime batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="runtime_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name=data_asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": datetime.now().isoformat()},
        )

        # Get validator
        validator = self.context.get_validator(
            batch_request=batch_request, expectation_suite_name=expectation_suite_name
        )

        # Run validation
        results = validator.validate()

        return self._parse_results(results, expectation_suite_name)

    def validate_snowflake_table(
        self, table_name: str, schema: str, expectation_suite_name: str, query: str = None
    ) -> ValidationResult:
        """
        Validate a Snowflake table against an expectation suite

        Args:
            table_name: Name of the table
            schema: Database schema
            expectation_suite_name: Name of the expectation suite
            query: Optional custom SQL query

        Returns:
            ValidationResult with validation details
        """
        logger.info(f"Validating Snowflake table: {schema}.{table_name}")

        if query:
            batch_request = RuntimeBatchRequest(
                datasource_name="snowflake_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name=f"{schema}.{table_name}",
                runtime_parameters={"query": query},
                batch_identifiers={"run_id": datetime.now().isoformat()},
            )
        else:
            batch_request = {
                "datasource_name": "snowflake_datasource",
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": f"{schema}.{table_name}",
            }

        validator = self.context.get_validator(
            batch_request=batch_request, expectation_suite_name=expectation_suite_name
        )

        results = validator.validate()

        return self._parse_results(results, expectation_suite_name)

    def run_checkpoint(self, checkpoint_name: str) -> ValidationResult:
        """
        Run a predefined checkpoint

        Args:
            checkpoint_name: Name of the checkpoint to run

        Returns:
            ValidationResult with validation details
        """
        logger.info(f"Running checkpoint: {checkpoint_name}")

        checkpoint = self.context.get_checkpoint(checkpoint_name)
        results = checkpoint.run()

        # Get the first validation result (checkpoints can have multiple)
        validation_result = list(results.run_results.values())[0]
        suite_name = validation_result.expectation_suite_name

        return self._parse_results(validation_result.validation_result, suite_name)

    def _parse_results(self, results, suite_name: str) -> ValidationResult:
        """Parse GX results into ValidationResult"""

        failed_expectations = []
        for result in results.results:
            if not result.success:
                failed_expectations.append(
                    {
                        "expectation_type": result.expectation_config.expectation_type,
                        "column": result.expectation_config.kwargs.get("column"),
                        "details": result.result,
                    }
                )

        return ValidationResult(
            suite_name=suite_name,
            success=results.success,
            statistics=results.statistics,
            failed_expectations=failed_expectations,
            run_time=datetime.now(),
            docs_url=None,  # Would be set after building docs
        )

    def create_expectation_suite(
        self, suite_name: str, expectations: List[Dict], overwrite: bool = False
    ):
        """
        Create an expectation suite programmatically

        Args:
            suite_name: Name for the suite
            expectations: List of expectation configurations
            overwrite: Whether to overwrite existing suite
        """
        logger.info(f"Creating expectation suite: {suite_name}")

        suite = self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

        for exp in expectations:
            suite.add_expectation(
                gx.core.ExpectationConfiguration(
                    expectation_type=exp["type"], kwargs=exp.get("kwargs", {})
                )
            )

        self.context.save_expectation_suite(suite)
        logger.info(f"Saved expectation suite with {len(expectations)} expectations")

    def build_data_docs(self) -> str:
        """
        Build Data Docs and return the URL

        Returns:
            URL to the Data Docs site
        """
        self.context.build_data_docs()
        docs_sites = self.context.get_docs_sites_urls()
        return docs_sites[0]["site_url"] if docs_sites else None


# Common expectation templates
COMMON_EXPECTATIONS = {
    "primary_key": lambda col: [
        {"type": "expect_column_to_exist", "kwargs": {"column": col}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": col}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": col}},
    ],
    "required_string": lambda col: [
        {"type": "expect_column_to_exist", "kwargs": {"column": col}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": col}},
        {"type": "expect_column_values_to_be_of_type", "kwargs": {"column": col, "type_": "str"}},
    ],
    "positive_number": lambda col: [
        {"type": "expect_column_to_exist", "kwargs": {"column": col}},
        {"type": "expect_column_values_to_be_between", "kwargs": {"column": col, "min_value": 0}},
    ],
    "valid_email": lambda col: [
        {
            "type": "expect_column_values_to_match_regex",
            "kwargs": {"column": col, "regex": r"^[\w\.-]+@[\w\.-]+\.\w+$"},
        }
    ],
}


def main():
    """Example usage"""
    import pandas as pd

    # Sample data
    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [101, 102, 103, 104, 105],
            "amount": [99.99, 149.99, 29.99, 199.99, 79.99],
            "status": ["SHIPPED", "PENDING", "DELIVERED", "SHIPPED", "PENDING"],
        }
    )

    # Initialize validator
    validator = DataQualityValidator()

    # Create expectation suite
    expectations = [
        *COMMON_EXPECTATIONS["primary_key"]("order_id"),
        *COMMON_EXPECTATIONS["positive_number"]("amount"),
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "status", "value_set": ["PENDING", "SHIPPED", "DELIVERED"]},
        },
    ]

    validator.create_expectation_suite("orders_suite", expectations)

    # Run validation
    result = validator.validate_dataframe(df, "orders_suite")

    print(f"Validation {'PASSED' if result.success else 'FAILED'}")
    print(f"Statistics: {result.statistics}")

    if not result.success:
        print("Failed expectations:")
        for exp in result.failed_expectations:
            print(f"  - {exp['expectation_type']} on {exp['column']}")


if __name__ == "__main__":
    main()
