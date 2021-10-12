import pytest
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    FloatType,
    StructField,
    StructType,
)


TARGET_SCHEMA = StructType(
    [
        StructField("xid", StringType(), True),
        StructField("date", DateType(), True),
        StructField("vendor", StringType(), True),
        StructField("source", StringType(), True),
        StructField("group", StringType(), True),
        StructField("aspect", StringType(), True),
        StructField("value", FloatType(), True),
    ]
)

#############
# Constants #
#############

# Common + Utils
GA_IDS_FIXTURE_PATH = "tests/targets/fixtures/ga_ids_fixture"
# NOTE: GA_IDS_FULL_FIXTURE_PATH is identical to GA_IDS_FIXTURE_PATH but contains
# a few extra columns which are needed for passthrough / proxy targets
GA_IDS_FULL_FIXTURE_PATH = "tests/targets/fixtures/ga_ids_full_fixture"
GA_XIDS_FIXTURE_PATH = "tests/targets/fixtures/ga_xids_fixture.csv"
ALL_CDS_ALT_DATA_FIXTURE = "tests/targets/fixtures/cds_all_data_fixture.csv"
CDS_ALT_FIXTURES_PATH = "tests/targets/fixtures/cds_"

# CDS
CDS_ALT_INDIV_FIXTURE_PATH = "tests/targets/fixtures/cds_alt_indiv_fixture.csv"
CDS_ALT_INDIV_D_FIXTURE_PATH = "tests/targets/fixtures/cds_alt_indiv_demo_fixture.csv"
CDS_INDIV_TRANS_FIXTURE_PATH = "tests/targets/fixtures/cds_indiv_transform_fixture.csv"
CDS_INDIV_D_TRANS_FIXTURE_PATH = "tests/targets/fixtures/cds_indiv_d_trans_fixture.csv"
CDS_INDIV_TAR_FIXTURE_PATH = "tests/targets/fixtures/cds_indiv_targets_fixture.csv"
CDS_INDIV_D_TAR_FIXTURE_PATH = (
    "tests/targets/fixtures/cds_indiv_demo_targets_fixture.csv"
)

# DAR
DAR_INPUT_FIXTURE_PATH = "tests/targets/fixtures/dar_fixture.csv"
DAR_TARGETS_FIXTURE_PATH = "tests/targets/fixtures/dar_targets.csv"

# DFP
DFP_CLICKS_INPUT_FIXTURE_PATH = "tests/targets/fixtures/dfp_clicks_input_fixture"
DFP_CLICKS_TRANSFORM_FIXTURE_PATH = "tests/targets/fixtures/dfp_clicks_input_fixture"
DFP_CLICKS_TARGETS_FIXTURE_PATH = "tests/targets/fixtures/dfp_clicks_tar_fixture.csv"

DFP_IMP_INPUT_FIXTURE_PATH = "tests/targets/fixtures/dfp_imp_input_fixture"
DFP_IMP_TRANSFORM_FIXTURE_PATH = "tests/targets/fixtures/dfp_imp_input_fixture"
DFP_IMP_TARGETS_FIXTURE_PATH = "tests/targets/fixtures/dfp_imp_targets_fixture.csv"

# NCS
NCS_CUSTOM_INPUT_FIXTURE_PATH = "tests/targets/fixtures/ncs_custom_input_fixture.csv"
NCS_CUSTOM_TRANSFORM_FIXTURE_PATH = "tests/targets/fixtures/ncs_custom_tran_fixture.csv"
NCS_CUSTOM_TARGETS_FIXTURE_PATH = "tests/targets/fixtures/ncs_custom_tar_fixture.csv"

NCS_SYN_INPUT_FIXTURE_PATH = "tests/targets/fixtures/ncs_syn_input_fixture.csv"
NCS_SYN_TRANSFORM_FIXTURE_PATH = "tests/targets/fixtures/ncs_syn_tran_fixture.csv"
NCS_SYN_TARGETS_FIXTURE_PATH = "tests/targets/fixtures/ncs_syn_targets_fixture.csv"

# AAM
AAM_INTERSECT_SEGS_FIXTURE_PATH = "tests/targets/fixtures/aam_intersect_segs_fixture"
AAM_GA_JOINED_FIXTURE_PATH = "tests/targets/fixtures/aam_ga_joined_fixture"
AAM_TARGETS_FIXTURE_PATH = "tests/targets/fixtures/aam_targets_fixture"
AAM_MAPPED_FIXTURE_PATH = "tests/targets/fixtures/aam_mapped_fixture.csv"
AAM_GET_ACTIVE_SEGS_OUTPUT_FIXTURE_PATH = (
    "tests/targets/fixtures/get_active_segments_output_fixture"
)
AAM_SEGS_TO_PROCESS_FIXTURE_PATH = "tests/targets/fixtures/aam_segments_to_process"
AAM_SEGS_TO_PROCESS_BY_DATE_FIXTURE_PATH = (
    "tests/targets/fixtures/aam_segments_to_process_by_date"
)
AAM_MCID_FIXTURE_PATH = "tests/targets/fixtures/aam_mcid_fixture.csv"
AAM_XID_FIXTURE_PATH = "tests/targets/fixtures/aam_xid_fixture.csv"
AAM_MCID_SEGMENTS_FIXTURE_PATH = "tests/targets/fixtures/mcid_segments_fixture"
AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH = (
    "tests/targets/fixtures/get_active_segments_input_fixture"
)

# Passthrough
AFFILIATE_PT_TRANSFORM_FIXTURE_PATH = (
    "tests/targets/fixtures/affiliate_pt_tran_fixture.csv"
)
COVID_PT_TRANSFORM_FIXTURE_PATH = "tests/targets/fixtures/covid_pt_tran_fixture.csv"

# Custom
CUSTOM_FIXTURE_PATH = "tests/targets/fixtures/custom_fixture.csv"

###########
# Helpers #
###########


@pytest.fixture(scope="module")
def date_fixture():
    return datetime.date(2021, 1, 31)


def load_input_fixtures(fixture_path, spark):
    return spark.read.csv(fixture_path, header=True, inferSchema=True)


def load_targets_fixtures(fixture_path, spark, delim=","):
    return spark.read.csv(fixture_path, sep=delim, header=True, schema=TARGET_SCHEMA)


############
# Fixtures #
############

#  Utils Fixtures
@pytest.fixture(scope="module")
def ga_ids_fixture(spark):
    return spark.read.parquet(GA_IDS_FIXTURE_PATH)


@pytest.fixture(scope="module")
def ga_ids_full_fixture(spark):
    return spark.read.parquet(GA_IDS_FULL_FIXTURE_PATH)


@pytest.fixture(scope="module")
def ga_xids_fixture(spark):
    ga_xids = load_input_fixtures(GA_XIDS_FIXTURE_PATH, spark)
    ga_xids = ga_xids.withColumn("date", F.col("date").cast(DateType()))
    return ga_xids


@pytest.fixture(scope="module")
def cds_all_data_fixture(spark):
    cds_data = load_input_fixtures(ALL_CDS_ALT_DATA_FIXTURE, spark)
    cds_data = cds_data.withColumn("date", F.col("date").cast(DateType()))
    return cds_data


# CDS Fixtures
@pytest.fixture(scope="module")
def cds_indiv_fixture(spark):
    return load_input_fixtures(CDS_ALT_INDIV_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def cds_indiv_demo_fixture(spark):
    return load_input_fixtures(CDS_ALT_INDIV_D_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def cds_indiv_transform_fixture(spark):
    return load_input_fixtures(CDS_INDIV_TRANS_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def cds_indiv_demo_transform_fixture(spark):
    return load_input_fixtures(CDS_INDIV_D_TRANS_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def cds_indiv_targets_fixture(spark):
    return load_targets_fixtures(CDS_INDIV_TAR_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def cds_indiv_demo_targets_fixture(spark):
    return load_targets_fixtures(CDS_INDIV_D_TAR_FIXTURE_PATH, spark)


# DAR Fixtures
@pytest.fixture(scope="module")
def dar_input_fixture(spark):
    return load_input_fixtures(DAR_INPUT_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def dar_targets_fixture(spark):
    return load_targets_fixtures(DAR_TARGETS_FIXTURE_PATH, spark)


# DFP Fixtures
@pytest.fixture(scope="module")
def dfp_clicks_input_fixture(spark):
    return spark.read.parquet(DFP_CLICKS_INPUT_FIXTURE_PATH)


@pytest.fixture(scope="module")
def dfp_clicks_transform_fixture(spark):
    return spark.read.parquet(DFP_CLICKS_TRANSFORM_FIXTURE_PATH)


@pytest.fixture(scope="module")
def dfp_clicks_targets_fixture(spark):
    # DFP Targets fixtures delimited with '|' due to ',' in fixture content
    return load_targets_fixtures(DFP_CLICKS_TARGETS_FIXTURE_PATH, spark, delim="|")


@pytest.fixture(scope="module")
def dfp_imp_input_fixture(spark):
    return spark.read.parquet(DFP_IMP_INPUT_FIXTURE_PATH)


@pytest.fixture(scope="module")
def dfp_imp_transform_fixture(spark):
    return spark.read.parquet(DFP_IMP_TRANSFORM_FIXTURE_PATH)


@pytest.fixture(scope="module")
def dfp_imp_targets_fixture(spark):
    # DFP Targets fixtures delimited with '|' due to ',' in fixture content
    return load_targets_fixtures(DFP_IMP_TARGETS_FIXTURE_PATH, spark, delim="|")


# NCS Fixtures
@pytest.fixture(scope="module")
def ncs_custom_input_fixture(spark):
    return load_input_fixtures(NCS_CUSTOM_INPUT_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def ncs_custom_transform_fixture(spark):
    return load_input_fixtures(NCS_CUSTOM_TRANSFORM_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def ncs_custom_targets_fixture(spark):
    return load_targets_fixtures(NCS_CUSTOM_TARGETS_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def ncs_syn_input_fixture(spark):
    return load_input_fixtures(NCS_SYN_INPUT_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def ncs_syn_transform_fixture(spark):
    return load_input_fixtures(NCS_SYN_TRANSFORM_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def ncs_syn_targets_fixture(spark):
    return load_targets_fixtures(NCS_SYN_TARGETS_FIXTURE_PATH, spark)


# Passthrough Fixtures
@pytest.fixture(scope="module")
def affiliate_pt_transform_fixture(spark):
    return load_targets_fixtures(AFFILIATE_PT_TRANSFORM_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def covid_pt_transform_fixture(spark):
    return load_targets_fixtures(COVID_PT_TRANSFORM_FIXTURE_PATH, spark)


# AAM Fixtures
@pytest.fixture(scope="module")
def aam_intersect_segments_fixture(spark):
    return spark.read.parquet(AAM_INTERSECT_SEGS_FIXTURE_PATH)


@pytest.fixture(scope="module")
def aam_ga_joined_fixture(spark):
    return spark.read.parquet(AAM_GA_JOINED_FIXTURE_PATH)


@pytest.fixture(scope="module")
def aam_targets_fixture(spark):
    return spark.read.parquet(AAM_TARGETS_FIXTURE_PATH)


@pytest.fixture(scope="module")
def aam_mapped_fixture(spark):
    return load_targets_fixtures(AAM_MAPPED_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def get_active_segments_output_fixture(spark):
    return spark.read.parquet(AAM_GET_ACTIVE_SEGS_OUTPUT_FIXTURE_PATH)


@pytest.fixture(scope="module")
def aam_segs_to_process_fixture(spark):
    return spark.read.parquet(AAM_SEGS_TO_PROCESS_FIXTURE_PATH)


@pytest.fixture(scope="module")
def aam_segs_to_process_by_date_fixture(spark):
    return spark.read.parquet(AAM_SEGS_TO_PROCESS_BY_DATE_FIXTURE_PATH)


@pytest.fixture(scope="session")
def aam_mcid_segments_fixture(spark):
    return spark.read.parquet(AAM_MCID_SEGMENTS_FIXTURE_PATH)


@pytest.fixture(scope="session")
def aam_mcid_fixture(spark):
    return load_input_fixtures(AAM_MCID_FIXTURE_PATH, spark)


@pytest.fixture(scope="session")
def aam_xid_fixture(spark):
    return load_input_fixtures(AAM_XID_FIXTURE_PATH, spark)


@pytest.fixture(scope="session")
def get_active_segments_input_fixture(spark):
    return spark.read.parquet(AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH)


# Custom Fixtures
@pytest.fixture(scope="module")
def custom_fixture(spark):
    return load_input_fixtures(CUSTOM_FIXTURE_PATH, spark)


@pytest.fixture(scope="module")
def custom_mapped_fixture(spark):
    # Note that although this reads the same fixture, the load_targets_fixtures
    # function enforces the targets schema when reading the data.
    # Hence, although custom_fixture has the same data, the schema is different.
    return load_targets_fixtures(CUSTOM_FIXTURE_PATH, spark)
