# Location: src/tests/test_shared_logic.py
# Purpose: Verification of the "Engine" logic

import pytest
from src.common.shared_logic import calculate_thermal_stress
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_calculate_thermal_stress(spark_session):
    # Mock data: 10C (Heating expected) and 30C (Cooling expected)
    data = [(10.0,), (30.0,)]
    df = spark_session.createDataFrame(data, ["temp"])
    
    result = calculate_thermal_stress(df, "temp").collect()
    
    # Assert Heating Degree Days for 10C (15 - 10 = 5)
    assert result[0]["heating_degree_days"] == 5.0
    # Assert Cooling Degree Days for 30C (30 - 25 = 5)
    assert result[1]["cooling_degree_days"] == 5.0