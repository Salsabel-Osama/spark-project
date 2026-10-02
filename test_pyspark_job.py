import os
import sys
import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="module")
def spark():
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("PySparkTests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "30s")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_valid_records_are_kept(spark):
    data = [
        ("Ali", 100.0),
        ("Sara", 200.0),
    ]

    df = spark.createDataFrame(
        data,
        ["name", "amount"]
    )

    result = clean_data(df)

    assert result.count() == 2


def test_records_with_non_positive_amount_are_removed(spark):
    data = [
        ("Ali", 100.0),
        ("Sara", 0.0),
        ("Omar", -50.0),
    ]

    df = spark.createDataFrame(
        data,
        ["name", "amount"]
    )

    result = clean_data(df)

    names = [row["name"] for row in result.collect()]

    assert names == ["Ali"]


def test_records_with_null_names_are_removed(spark):
    data = [
        ("Ali", 100.0),
        (None, 200.0),
    ]

    df = spark.createDataFrame(
        data,
        ["name", "amount"]
    )

    result = clean_data(df)

    names = [row["name"] for row in result.collect()]

    assert names == ["Ali"]


def test_amount_with_tax_is_calculated_correctly(spark):
    data = [
        ("Ali", 100.0),
        ("Sara", 200.0),
    ]

    df = spark.createDataFrame(
        data,
        ["name", "amount"]
    )

    result = clean_data(df)

    rows = result.collect()

    assert rows[0]["amount_with_tax"] == 120.0
    assert rows[1]["amount_with_tax"] == 240.0