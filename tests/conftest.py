"""Shared pytest fixtures, including a session-scoped local Spark session."""

from __future__ import annotations

import os

import pytest

# Quiet down Spark/py4j logs in tests.
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "pyspark-shell")


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession.

    Spark is expensive to start up, so we share a single session across the
    entire test suite. Individual tests should create their own DataFrames
    rather than mutating shared state.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("dq_framework-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


@pytest.fixture(autouse=True)
def _reset_logging():
    """Make sure log level is sane between tests."""
    import logging

    logging.getLogger("dq_framework").setLevel(logging.WARNING)
    yield
