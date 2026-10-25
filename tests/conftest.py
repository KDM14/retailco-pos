"""
Shared PyTest fixtures for RetailCo Data Platform.
"""
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Provides a shared local SparkSession across the test session."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailCo_PyTest_Suite")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
