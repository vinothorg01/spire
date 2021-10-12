import pytest
from spire.utils.spark_helpers import init_spark
from pyspark.sql import SparkSession


@pytest.fixture(autouse=True, scope="session")
def spark() -> SparkSession:
    spark = init_spark()
    return spark
