import pytest
import datetime
import pyspark.sql.functions as F
from tests.targets.conftest import load_targets_fixtures

#############
# Constants #
#############

# Common
AAM_SPIRE_SEGS_RAW_FIXTURE_PATH = "tests/targets/fixtures/aam_segments_raw_fixture"

# Active Segments
AAM_MAPPED_PARQUET_FIXTURE_PATH = "tests/targets/fixtures/aam_mapped_parquet"
AAM_EXTRACT_SEGMENTS_FIXTURE_PATH = (
    "tests/targets/fixtures/aam_extract_segments_fixture"
)
AAM_CREATE_ACTIVE_SEGS_FIXTURE_PATH = (
    "tests/targets/fixtures/create_aam_segments_table_fixture"
)

# MCID Segments
AAM_UNPARSED_FIXTURE_PATH = (
    "tests/targets/fixtures/S3_77904_1955_iter_1612500023000.sync"
)
AAM_PARSE_SEGS_FIXTURE_PATH = "tests/targets/fixtures/parse_aam_segments_fixture"
AAM_UPDATE_TABLE_FIXTURE_PATH = "tests/targets/fixtures/aam_update_table_fixture"

# Runthrough
AAM_TARGETS_RUNTHROUGH_FIXTURE_PATH = (
    "tests/targets/fixtures/aam_targets_runthrough_fixture.csv"
)
AAM_SEGS_TABLES_UPDATED_FIXTURE_PATH = (
    "tests/targets/fixtures/aam_segments_tables_updated_fixture"
)
AAM_MCID_SEGMENTS_RUNTHROUGH_FIXTURE_PATH = (
    "tests/targets/fixtures/mcid_segments_runthrough_fixture"
)

############
# Fixtures #
############


# Active Segments


@pytest.fixture(scope="module")
def latest_aam_segments_fixture(aam_mapped_fixture):
    # Note: aam_mapped_fixture is from tests/targets/conftest
    return aam_mapped_fixture.select("group", "date").orderBy("group", "date")


@pytest.fixture(scope="module")
def aam_extract_segments_fixture(spark):
    return spark.read.parquet(AAM_EXTRACT_SEGMENTS_FIXTURE_PATH).orderBy("segment_id")


@pytest.fixture(scope="module")
def aam_segments_table_fixture(spark, aam_extract_segments_fixture, date_fixture):
    return aam_extract_segments_fixture.withColumn(
        "last_processed_date", F.lit(date_fixture)
    )


@pytest.fixture(scope="module")
def aam_segments_removed_fixture(spark, aam_extract_segments_fixture, date_fixture):
    execution_date = datetime.date(2021, 2, 1)
    columns = [
        "segment_id",
        "segment_name",
        "start_date",
        "end_date",
        "last_processed_date",
    ]
    values = [
        (
            18365619,
            "TransUnion_Consumer_Finance_Credit_Behavior_Average_Aggregated_Credit_"
            + "Tiers_Near_Prime_to_Prime_3rd_Quartile",
            date_fixture,
            execution_date - datetime.timedelta(days=1),
            date_fixture,
        )
    ]
    removed = spark.createDataFrame(values, columns)
    return aam_extract_segments_fixture.limit(3).union(removed)


@pytest.fixture(scope="module")
def aam_segments_added_fixture(spark, aam_extract_segments_fixture, date_fixture):
    values = [(10000, "new_segment")]
    cols = ["segment_id", "segment_name"]
    added = spark.createDataFrame(values, cols)
    added = added.withColumn("start_date", F.lit(date_fixture))
    added = added.withColumn("end_date", F.lit(None))
    added = added.withColumn("last_processed_date", F.lit(None))
    return aam_extract_segments_fixture.union(added)


@pytest.fixture(scope="module")
def create_aam_segments_table_fixture(spark):
    return spark.read.parquet(AAM_CREATE_ACTIVE_SEGS_FIXTURE_PATH).orderBy("segment_id")


# MCID Segments


@pytest.fixture(scope="module")
def aam_unparsed_fixture(spark):
    return spark.read.csv(AAM_UNPARSED_FIXTURE_PATH)


@pytest.fixture(scope="module")
def mcid_segments_fixture(spark):
    mcid_segments_values = [
        (datetime.date(2018, 10, 18), "full", 1539869790, "A", [8, 80, 129]),
        (datetime.date(2018, 3, 23), "full", 1521839790, "B", [8, 80]),
    ]
    mcid_segments_columns = ("ref_date", "type", "timestamp", "mcid", "segment_array")
    return spark.createDataFrame(mcid_segments_values, mcid_segments_columns)


@pytest.fixture(scope="module")
def aam_parse_segs_fixture(spark):
    return spark.read.parquet(AAM_PARSE_SEGS_FIXTURE_PATH)


@pytest.fixture(scope="session")
def aam_update_table_fixture(spark):
    return spark.read.parquet(AAM_UPDATE_TABLE_FIXTURE_PATH)


# Runthrough


@pytest.fixture(scope="session")
def aam_targets_runthrough_fixture(spark):
    return load_targets_fixtures(AAM_TARGETS_RUNTHROUGH_FIXTURE_PATH, spark)


@pytest.fixture(scope="session")
def aam_runthrough_updated_active_segs_fixture(spark):
    return spark.read.parquet(AAM_SEGS_TABLES_UPDATED_FIXTURE_PATH)


@pytest.fixture(scope="session")
def aam_mcid_segments_runthrough_fixture(spark):
    fixture = spark.read.parquet(AAM_MCID_SEGMENTS_RUNTHROUGH_FIXTURE_PATH)
    return fixture.orderBy("ref_date", "type", "timestamp", "mcid", "segment_array")
