"""
Runthrough Tests
----------------
These tests mimic the aam_targets airflow dag which has the following tasks:
1. update_spire_aam_segments
2. update_mcid_segments_table
3. update_active_segments_table
4. process_targets
5. update_aam_segments_tables

Notes
------
# Task 1 has been skipped as it involves copying a file.
That file (SPIRE_AAM_SEGMENTS_URI) has been patched in.

# Tasks 2 - 5 have the following test pattern:
- run_task --> has to be a fixture to enable use of tmp directories
           --> output = data from tmp locations
- test_task_output --> compare output of run_task to fixture

# The patch decorator has not been used in several places as PyTest does not support
patching in fixtures using the decorator.
"""
import pytest
from unittest import mock
from unittest.mock import patch
import spire.utils.s3
import spire.utils.spark_helpers
import spire.targets.constants
from spire.targets import AdobeConfig
import spire.targets.aam_tables.create_aam_mcid_segments
from tests.targets.fixtures import aam_fixtures as fixtures
from spire.tasks.execute_targets import main as execute_targets
from spire.tasks.execute_mcid_segments_update import (
    main as execute_mcid_segments_update,
)
from spire.tasks.execute_active_segments_update import (
    main as execute_active_segments_update,
)
from spire.tasks.execute_aam_segments_tables_updates import (
    main as execute_aam_segments_tables_updates,
)
from tests.targets.aam_tables.conftest import AAM_SPIRE_SEGS_RAW_FIXTURE_PATH


#############
# Constants #
#############

EXECUTION_DATE_STR = "2021-02-04"

##########################################
# Prepare temporary directory and tables #
##########################################


@pytest.fixture(scope="session")
def base_tmp_dir(tmpdir_factory):
    """Temporary base directory where mcid and active segments tables are stored."""
    return tmpdir_factory.mktemp("test_dir1")


@pytest.fixture(scope="session")
def targets_output_tmp_dir(tmpdir_factory):
    """Temporary targets output directory."""
    path = tmpdir_factory.mktemp("targets_output")
    return str(path)


@pytest.fixture(scope="session")
def mcid_segments_tmp_table_path(base_tmp_dir, aam_mcid_segments_fixture):
    """Create temporary mcid segments table location for updates and access.
    Write fixture table to tmp location.
    +----------+----+----------+----+--------------------+
    |  ref_date|type| timestamp|mcid|       segment_array|
    +----------+----+----------+----+--------------------+
    |2021-02-01|full|1612200000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612300000|   D|[11191954, 11940211]|
    |2021-02-01|iter|1612400000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612500000|   D|                  []|
    |2021-01-07|full|1610079790|   A|           [80, 129]|
    +----------+----+----------+----+--------------------+
    """
    path = base_tmp_dir.join("mcid_segments_table")
    aam_mcid_segments_fixture.write.format("delta").partitionBy(
        "ref_date", "type", "timestamp"
    ).save(str(path))
    return str(path)


@pytest.fixture(scope="session")
def active_segments_tmp_table_path(base_tmp_dir, get_active_segments_input_fixture):
    """Create temporary active segments table location for updates and access.
    Write fixture table to tmp location.
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|         2021-02-02|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-01-30|
    |  11940211|             married|2021-01-31|    null|         2021-02-01|
    +----------+--------------------+----------+--------+-------------------+
    """
    path = base_tmp_dir.join("active_segments_table")
    get_active_segments_input_fixture.write.format("delta").save(str(path))
    return str(path)


#########
# Tests #
#########

#################################
# 2. update_mcid_segments_table #
#################################


@pytest.fixture(scope="session")
@patch("spire.targets.constants.MCID_REGEX", "")
@patch("spire.targets.constants.DATA_VENDOR_URI", "tests/")
@patch.object(spire.utils.s3, "get_all_keys")
def run_mcid_segments(get_all_keys_patch, mcid_segments_tmp_table_path, spark):
    """Update the mcid segments with new mcid/segment for the given timestamp."""
    get_all_keys_patch.return_value = fixtures.AAM_RUNTHROUGH_KEYS
    with patch(
        "spire.targets.constants.MCID_SEGMENTS_TABLE_URI", mcid_segments_tmp_table_path
    ):
        execute_mcid_segments_update()
    return spark.read.format("delta").load(mcid_segments_tmp_table_path)


@pytest.mark.dependency()
def test_mcid_segments(run_mcid_segments, aam_mcid_segments_runthrough_fixture):
    """
    Output
    +----------+----+----------+----+--------------------+
    |  ref_date|type| timestamp|mcid|       segment_array|
    +----------+----+----------+----+--------------------+
    |2021-02-01|full|1612200000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612300000|   D|[11191954, 11940211]|
    |2021-02-01|iter|1612400000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612500000|   D|                  []|
    |2021-01-07|full|1610079790|   A|           [80, 129]|
    |2021-02-01|iter|1612500023|   D|          [18365619]|
    +----------+----+----------+----+--------------------+
    """
    output = run_mcid_segments.orderBy(
        "ref_date", "type", "timestamp", "mcid", "segment_array"
    )
    assert output.collect() == aam_mcid_segments_runthrough_fixture.collect()


###################################
# 3. update_active_segments_table #
###################################


@pytest.fixture(scope="session")
@patch(
    "spire.targets.constants.SPIRE_AAM_SEGMENTS_URI", AAM_SPIRE_SEGS_RAW_FIXTURE_PATH
)
def run_active_segments(active_segments_tmp_table_path, spark):
    """No new segments are added so the active_segments table remains unchanged."""
    # Since no new segments were added, the table remains unchanged.
    with patch(
        "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
        active_segments_tmp_table_path,
    ):
        execute_active_segments_update(EXECUTION_DATE_STR)
    return spark.read.format("delta").load(active_segments_tmp_table_path)


@pytest.mark.dependency(depends=["test_mcid_segments"])
def test_run_active_segments(run_active_segments, get_active_segments_input_fixture):
    """
    Output
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|         2021-02-02|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-01-30|
    |  11940211|             married|2021-01-31|    null|         2021-02-01|
    +----------+--------------------+----------+--------+-------------------+
    """
    fixture = get_active_segments_input_fixture.orderBy("segment_id")
    output = run_active_segments.orderBy("segment_id")
    assert fixture.collect() == output.collect()


######################
# 4. process_targets #
######################


@pytest.fixture(scope="session")
@patch.object(
    spire.targets.aam,
    "load_ga_mapping_tables",
)
def run_targets(
    ga_tables_patch,
    mcid_segments_tmp_table_path,
    active_segments_tmp_table_path,
    aam_mcid_fixture,
    aam_xid_fixture,
    targets_output_tmp_dir,
    spark,
):
    ga_tables_patch.return_value = (aam_mcid_fixture, aam_xid_fixture)
    # Fixtures cannot be patched in using decorators
    with patch(
        "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
        active_segments_tmp_table_path,
    ):
        with patch(
            "spire.targets.constants.TARGETS_OUTPUT_URI", targets_output_tmp_dir
        ):
            with patch.object(
                AdobeConfig, "uri", new_callable=mock.PropertyMock
            ) as uri:
                # Patches are made where an object is looked up, not necessarily where
                # it's defined. Additionally, Python is dynamic and does not
                # re-evaluate modules and class hierarchy after the patch.
                # The mcid segments table had to be patched in this way as it is
                # looked up by the uri attribute of AdobeConfig.
                # See https://bit.ly/3DkZMeI
                uri.return_value = mcid_segments_tmp_table_path
                execute_targets("adobe", "mcid", EXECUTION_DATE_STR)
    return spark.read.format("delta").load(targets_output_tmp_dir)


@pytest.mark.dependency(depends=["test_run_active_segments"])
def test_run_targets(run_targets, aam_targets_runthrough_fixture):
    """
    For output see tests/targets/fixtures/aam_targets_runthrough_fixture.csv
    """
    output = run_targets.orderBy("date", "group")
    fixture = aam_targets_runthrough_fixture.orderBy("date", "group")
    assert output.collect() == fixture.collect()


#################################
# 5. update_aam_segments_tables #
#################################


@pytest.fixture(scope="session")
@patch.object(spire.utils.spark_helpers, "check_if_partitions_exist")
def run_aam_segments_tables_updates(
    partition_check_patch,
    targets_output_tmp_dir,
    active_segments_tmp_table_path,
    mcid_segments_tmp_table_path,
    spark,
):
    """Update the last processed date of segments after targets have been processed."""
    partition_check_patch.return_value = True
    # Fixtures cannot be patched in using decorators
    with patch(
        "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
        active_segments_tmp_table_path,
    ):
        with patch(
            "spire.targets.constants.TARGETS_OUTPUT_URI", targets_output_tmp_dir
        ):
            with patch(
                "spire.targets.constants.MCID_SEGMENTS_TABLE_URI",
                mcid_segments_tmp_table_path,
            ):
                execute_aam_segments_tables_updates(EXECUTION_DATE_STR)
    return spark.read.format("delta").load(active_segments_tmp_table_path)


@pytest.mark.dependency(depends=["test_run_targets"])
def test_aam_segments_tables_updates(
    run_aam_segments_tables_updates, aam_runthrough_updated_active_segs_fixture
):
    """
    Output
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|         2021-02-04|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|         2021-02-04|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-02-04|
    |  11940211|             married|2021-01-31|    null|         2021-02-04|
    +----------+--------------------+----------+--------+-------------------+
    """
    output = run_aam_segments_tables_updates.orderBy("segment_id")
    fixture = aam_runthrough_updated_active_segs_fixture.orderBy("segment_id")
    assert output.collect() == fixture.collect()
