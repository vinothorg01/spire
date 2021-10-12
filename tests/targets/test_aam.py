"""Unit tests for AAM data processing to generate targets"""
import pytest
import datetime
from unittest.mock import patch
import spire.targets.aam
from spire.targets import AdobeConfig, TargetsRunner
from spire.targets.aam import (
    get_latest_full_file_date,
    intersect_spire_aam_segments,
    join_xid_to_segments,
    create_targets,
    get_active_segments,
    get_dates_and_segments,
    get_segments_to_process_by_date,
    get_segments_to_process,
    create_query,
)
from tests.targets.conftest import AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH
from spire.targets.aam_tables.update_aam_segments_tables import get_latest_aam_file_date

# Constants
BACKFILL_DAYS = 35
LATEST_FULL_FILE_DATE = datetime.date(2021, 2, 1)
LATEST_ADOBE_FILE_DATE = datetime.date(2021, 2, 4)
EXECUTION_DATE = datetime.date(2021, 2, 4)

# Tests


@patch(
    "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
    AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH,
)
def test_get_active_segments(get_active_segments_output_fixture):
    """
    For reference, the active segments input (from the path) is:
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|         2021-02-02|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-01-30|
    |  11940211|             married|2021-01-31|    null|         2021-02-01|
    +----------+--------------------+----------+--------+-------------------+
    This test ensures that a processing_start_date column is added to this table
    for each segment_id depending the last processed date (for older segments)
    or the latest full file date (for new segments).
    """

    output = get_active_segments(
        LATEST_FULL_FILE_DATE,
        EXECUTION_DATE,
        input_format="parquet",
    )
    assert get_active_segments_output_fixture.collect() == output.collect()


def test_get_segments_to_process(
    get_active_segments_output_fixture, aam_segs_to_process_fixture
):
    """
    The segs to process fixture is as follows:
    +---------------------+--------------------+
    |processing_start_date|         select_segs|
    +---------------------+--------------------+
    |           2021-02-01|[11191954, 18365619]|
    |           2021-02-03|          [13855482]|
    |           2021-02-02|          [11940211]|
    |           2021-02-04|                null|
    +---------------------+--------------------+
    This test ensures that the function that groups together segments
    by their processing start date and joins it to a list of dates
    in the range (min_processing_start_date, execution_date) inclusive, works.
    """
    segs_grouped_by_dates = get_segments_to_process(
        get_active_segments_output_fixture,
        LATEST_FULL_FILE_DATE,
        EXECUTION_DATE,
        BACKFILL_DAYS,
    )
    fixture = aam_segs_to_process_fixture.orderBy("processing_start_date")
    output = segs_grouped_by_dates.orderBy("processing_start_date")
    assert output.collect() == fixture.collect()


def test_get_segments_to_process_by_date(
    aam_segs_to_process_fixture, aam_segs_to_process_by_date_fixture
):
    """
    This is what the segs_to_process_by_date fixture is:
    +---------------------+----------------------------------------+
    |processing_start_date|segs_to_process                         |
    +---------------------+----------------------------------------+
    |2021-02-01           |[11191954, 18365619]                    |
    |2021-02-02           |[11940211, 11191954, 18365619]          |
    |2021-02-03           |[11940211, 11191954, 18365619, 13855482]|
    |2021-02-04           |[11940211, 11191954, 18365619, 13855482]|
    +---------------------+----------------------------------------+
    Tests whether the window function which gets the segments to process
    recursively works.
    """
    output = get_segments_to_process_by_date(aam_segs_to_process_fixture)
    assert aam_segs_to_process_by_date_fixture.collect() == output.collect()


def test_get_dates_and_segments(
    get_active_segments_output_fixture, aam_segs_to_process_by_date_fixture
):
    output = get_dates_and_segments(
        get_active_segments_output_fixture,
        LATEST_FULL_FILE_DATE,
        EXECUTION_DATE,
        BACKFILL_DAYS,
    )
    assert aam_segs_to_process_by_date_fixture.collect() == output.collect()


def test_get_aam_file_dates(aam_mcid_segments_fixture):
    """
    For reference, the mcid segments fixture is as follows:
    +----------+----+----------+----+--------------------+
    |  ref_date|type| timestamp|mcid|       segment_array|
    +----------+----+----------+----+--------------------+
    |2021-02-01|full|1612200000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612300000|   D|[11191954, 11940211]|
    |2021-02-01|iter|1612400000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612500000|   D|                  []|
    |2021-01-07|full|1610079790|   A|           [80, 129]|
    +----------+----+----------+----+--------------------+
    The test ensures that the latest full file date (from the ref_date column)
    and the latest timestamp date are returned.
    """
    latest_full_file_date_output = get_latest_full_file_date(aam_mcid_segments_fixture)
    latest_adobe_file_date_output = get_latest_aam_file_date(aam_mcid_segments_fixture)

    assert latest_full_file_date_output == LATEST_FULL_FILE_DATE
    assert latest_adobe_file_date_output == LATEST_ADOBE_FILE_DATE


def test_intersect_spire_aam_segments(
    aam_segs_to_process_by_date_fixture,
    aam_intersect_segments_fixture,
    aam_mcid_segments_fixture,
):
    """
    Intersect segs joins the mcid segments table to the segments to process table
    and filters it to the mcids with at least one segment. The list of segments to
    process for each date is intersected with the list of segments corresponding to
    a mcid. The result is as follows:

    +---------------------+----+--------------------+
    |processing_start_date|mcid|      segment_select|
    +---------------------+----+--------------------+
    |           2021-02-02|   D|[11940211, 11191954]|
    |           2021-02-01|   C|[11191954, 18365619]|
    |           2021-02-03|   C|[11191954, 18365619]|
    +---------------------+----+--------------------+
    """
    intersect_segs = intersect_spire_aam_segments(
        LATEST_FULL_FILE_DATE,
        aam_segs_to_process_by_date_fixture,
        aam_mcid_segments_fixture,
    )
    output = intersect_segs.orderBy("processing_start_date")
    fixture = aam_intersect_segments_fixture.orderBy("processing_start_date")
    assert output.collect() == fixture.collect()


def test_join_xid_to_segments(
    aam_mcid_fixture,
    aam_xid_fixture,
    aam_intersect_segments_fixture,
    aam_ga_joined_fixture,
):
    """
    Join the intersected table to GA data to get the corresponding XIDs.
    ga_joined_fixture is as follows:
    +---------------------+--------------------+--------------------+
    |processing_start_date|                 xid|      segment_select|
    +---------------------+--------------------+--------------------+
    |           2021-02-02|0a838749-f9a4-41b...|[11940211, 11191954]|
    |           2021-02-01|6bb9903f-bcbe-4ae...|[11191954, 18365619]|
    |           2021-02-03|6bb9903f-bcbe-4ae...|[11191954, 18365619]|
    +---------------------+--------------------+--------------------+
    """
    ga_joined = join_xid_to_segments(
        aam_intersect_segments_fixture, aam_mcid_fixture, aam_xid_fixture
    )
    output = ga_joined.orderBy("processing_start_date")
    fixture = aam_ga_joined_fixture.orderBy("processing_start_date")
    assert output.collect() == fixture.collect()


def test_create_targets(
    aam_ga_joined_fixture, aam_targets_fixture, get_active_segments_output_fixture
):
    """
    Explode the segment select column to obtain the segments and join to the active
    segments table to get the segment names.
    The aam_targets_fixture is as follows:
    +--------------------+---------------------+--------+--------------------+
    |                 xid|processing_start_date|   group|        segment_name|
    +--------------------+---------------------+--------+--------------------+
    |0a838749-f9a4-41b...|           2021-02-02|11940211|             married|
    |6bb9903f-bcbe-4ae...|           2021-02-03|11191954|Allergan_JUV_STAT...|
    |6bb9903f-bcbe-4ae...|           2021-02-01|11191954|Allergan_JUV_STAT...|
    |0a838749-f9a4-41b...|           2021-02-02|11191954|Allergan_JUV_STAT...|
    |6bb9903f-bcbe-4ae...|           2021-02-03|18365619|TransUnion_Consum...|
    |6bb9903f-bcbe-4ae...|           2021-02-01|18365619|TransUnion_Consum...|
    +--------------------+---------------------+--------+--------------------+
    """
    # This is the last step in the transform method.
    # Hence, output is the same as that of the transform method
    targets = create_targets(aam_ga_joined_fixture, get_active_segments_output_fixture)
    output = targets.orderBy("processing_start_date", "group")
    fixture = aam_targets_fixture.orderBy("processing_start_date", "group")
    assert output.collect() == fixture.collect()


def test_create_query(aam_segs_to_process_by_date_fixture):
    fixture = (
        "vendor='adobe' and source='mcid' and ((date='2021-02-01' and "
        + "group in ('11191954','18365619')) or (date='2021-02-02' and "
        + "group in ('11940211','11191954','18365619')) or "
        + "(date='2021-02-03' and group in "
        + "('11940211','11191954','18365619','13855482')) or "
        + "(date='2021-02-04' and group in "
        + "('11940211','11191954','18365619','13855482')))"
    )
    output = create_query(aam_segs_to_process_by_date_fixture)
    assert output == fixture


@patch(
    "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
    AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH,
)
@patch.object(
    spire.targets.aam,
    "load_ga_mapping_tables",
)
@patch.object(
    spire.targets.aam,
    "get_active_segments",
)
def test_transform(
    active_segments_patch,
    ga_tables_patch,
    aam_mcid_segments_fixture,
    aam_targets_fixture,
    aam_mcid_fixture,
    aam_xid_fixture,
):
    """Test all the steps of the data transformation for Adobe.
    This is a complete runthrough of all the steps, but using fixtures.

    Input Data
    Take the mcid segments table follows:
    +----------+----+----------+----+--------------------+
    |  ref_date|type| timestamp|mcid|       segment_array|
    +----------+----+----------+----+--------------------+
    |2021-02-01|full|1612200000|   C|[11191954, 18365619]|
    |2021-02-01|iter|1612300000|   D|[11191954, 11940211]|
    +----------+----+----------+----+--------------------+

    Output Data
    Convert to targets:
    +--------------------+---------------------+--------+--------------------+
    |                 xid|processing_start_date|   group|        segment_name|
    +--------------------+---------------------+--------+--------------------+
    |0a838749-f9a4-41b...|           2021-02-02|11940211|             married|
    |6bb9903f-bcbe-4ae...|           2021-02-03|11191954|Allergan_JUV_STAT...|
    +--------------------+---------------------+--------+--------------------+
    Note: These are snapshots of the data. Schema Normalization is done by map_schema.
    """
    # Patch in ga tables as we can't read from s3
    ga_tables_patch.return_value = (aam_mcid_fixture, aam_xid_fixture)
    # Patch the call to get_active_segments so that it reads in the parquet fixture
    active_segments_patch.return_value = get_active_segments(
        LATEST_FULL_FILE_DATE,
        EXECUTION_DATE,
        input_format="parquet",
    )

    aam_config = AdobeConfig(EXECUTION_DATE)
    runner = TargetsRunner(aam_config)
    transformed = runner.transform(aam_mcid_segments_fixture)

    # Test regular case
    # Order the outputs and the fixtures so the comparison is accurate
    transformed = transformed.orderBy("processing_start_date", "group")
    transform_fixture = aam_targets_fixture.orderBy("processing_start_date", "group")
    assert transformed.collect() == transform_fixture.collect()

    # Test exception
    # Ensure exception is raised if there has been no full file for more than 35 days
    with pytest.raises(Exception) as e_info:
        aam_config = AdobeConfig(datetime.date(2021, 8, 10))
        runner = TargetsRunner(aam_config)
        transformed = runner.transform(aam_mcid_segments_fixture)
    # Ensure that the exception message was customized correctly
    message = "Wait for Adobe data to be loaded"
    assert e_info.value.args[0] == message


def test_map_schema(aam_targets_fixture, aam_mapped_fixture):
    """
    See the tests/targets/fixtures/aam_mapped_fixture.csv for
    the fixture.
    """
    aam_config = AdobeConfig(EXECUTION_DATE)
    runner = TargetsRunner(aam_config)

    mapped = runner.map_schema(aam_targets_fixture)
    output = mapped.orderBy("date", "group")
    fixture = aam_mapped_fixture.orderBy("date", "group")
    assert output.collect() == fixture.collect()
