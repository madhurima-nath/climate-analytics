"""pytest configuration and fixtures for Databricks tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provide SparkSession for all tests."""
    spark_session = SparkSession.builder.getOrCreate()
    yield spark_session


@pytest.fixture(scope="session")
def catalog():
    """Provide catalog name for tests."""  
    return "climate_energy_demand"
