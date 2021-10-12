import pytest
import logging

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def quiet_py4j():
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(spark):
    def _get_session(*args, **kwargs):
        return spark

    quiet_py4j()

    return _get_session
