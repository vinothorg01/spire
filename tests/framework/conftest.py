import pytest
import datetime
from pyspark.sql.types import (
    IntegerType,
    StructType,
    StructField,
    FloatType,
    StringType,
)
from pyspark.sql import functions as F
from spire.framework.workflows import History


ALEPH_V1_FEATURES_FIXTURE_PATH = (
    "tests/framework/fixtures/aleph_v1_features_fixture.csv"
)
SCORES_INPUT_FIXTURE_PATH = "tests/framework/fixtures/scores.csv"
SCORES_TRAN_FIXTURE_PATH = "tests/framework/fixtures/scores_tran.csv"
SCORES_STATS_FIXTURE_PATH = "tests/framework/fixtures/scores_stats.csv"


@pytest.fixture(scope="function")
def wf_def():
    wf_name = "spire_foo_test"
    wf_desc = "test > test"
    wf_def = {
        "name": wf_name,
        "description": wf_desc,
        "is_proxy": False,
    }
    return wf_def


@pytest.fixture(scope="function")
def wf_proxy_def():
    wf_name = "spire_foo_test"
    wf_desc = "test > test"
    wf_def = {
        "name": wf_name,
        "description": wf_desc,
        "is_proxy": True,
    }
    return wf_def


@pytest.fixture(scope="module")
def date_fixture():
    return datetime.datetime(2021, 4, 11, 0, 0)


@pytest.fixture(scope="module")
def aleph_v1_features_fixture(spark):
    return spark.read.csv(
        ALEPH_V1_FEATURES_FIXTURE_PATH, header=True, inferSchema=True
    ).drop("_c0")


@pytest.fixture(scope="module")
def scores_fixture(spark):
    schema = StructType(
        [
            StructField("score", FloatType(), True),
            StructField("temp_score", FloatType(), True),
            StructField("infinity_id", IntegerType(), True),
        ]
    )
    return spark.read.csv(SCORES_INPUT_FIXTURE_PATH, schema, header=True)


@pytest.fixture(scope="module")
def scores_tran_fixture(spark):
    schema = StructType(
        [
            StructField("xid", StringType(), True),
            StructField("score", FloatType(), True),
            StructField("decile", IntegerType(), True),
        ]
    )
    scores_tran = spark.read.csv(SCORES_TRAN_FIXTURE_PATH, schema, header=True)
    return scores_tran.withColumn("score", F.round(scores_tran["score"], scale=3))


@pytest.fixture(scope="module")
def scores_stats_fixture(spark):
    return spark.read.csv(SCORES_STATS_FIXTURE_PATH, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def last_successful_train_fixture():
    # This history definition was created from a former functional / integrations test
    # for a workflow that was successfully assembled and trained, and is now used as a
    # unit-test fixture.
    return History(
        workflow_id="4c3ee043-729e-4434-8b0e-947f74edead7",
        stage="training",
        status="success",
        arg_date="2021-07-16",
        execution_date="2021-07-16 15:07:11.298849",
    )
