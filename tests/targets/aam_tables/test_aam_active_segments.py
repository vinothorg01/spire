import pytest
import datetime
from unittest.mock import patch
import spire.utils.spark_helpers
from tests.targets.aam_tables.conftest import (
    AAM_SPIRE_SEGS_RAW_FIXTURE_PATH,
    AAM_MAPPED_PARQUET_FIXTURE_PATH,
)
from tests.targets.conftest import (
    AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH,
    AAM_MCID_SEGMENTS_FIXTURE_PATH,
)
import spire.targets.aam_tables.create_aam_active_segments
from spire.targets.aam_tables.create_aam_active_segments import (
    load_aam_segments_table,
    extract_spire_aam_segments,
    get_latest_aam_segments,
    create_aam_segments_table,
    update_aam_segments,
)


def test_load_aam_segments_table():
    """Test that the active segments table is loaded and if not, its value is
    set to None.

    Expected Output
    ---------------
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|         2021-02-02|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-01-30|
    |  11940211|             married|2021-01-31|    null|         2021-02-01|
    +----------+--------------------+----------+--------+-------------------+
    """
    # Test case wherein table exists and is not empty
    with patch(
        "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
        AAM_GET_ACTIVE_SEGS_INPUT_FIXTURE_PATH,
    ):
        segments_table = load_aam_segments_table(input_format="parquet")
        assert segments_table.count() > 0

    # Test case wherein table does not exist
    with patch(
        "spire.targets.constants.ACTIVE_SEGMENTS_TABLE_URI",
        "invalid/path",
    ):
        segments_table = load_aam_segments_table(input_format="parquet")
        assert segments_table is None


@patch(
    "spire.targets.constants.SPIRE_AAM_SEGMENTS_URI", AAM_SPIRE_SEGS_RAW_FIXTURE_PATH
)
def test_extract_spire_aam_segments(date_fixture, aam_extract_segments_fixture):
    """Ensure that segments from the spire segments csv are extracted correctly.

    Expected Output
    ---------------
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    |  11940211|             married|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+
    Note that the last processed dates are null but will be filled in later.
    """
    output = extract_spire_aam_segments(date_fixture).orderBy("segment_id")
    assert output.collect() == aam_extract_segments_fixture.collect()


@patch(
    "spire.targets.constants.MCID_SEGMENTS_TABLE_URI",
    AAM_MCID_SEGMENTS_FIXTURE_PATH,
)
@patch(
    "spire.targets.constants.TARGETS_OUTPUT_URI",
    AAM_MAPPED_PARQUET_FIXTURE_PATH,
)
@patch.object(spire.utils.spark_helpers, "check_if_partitions_exist")
def test_get_latest_aam_segments(partition_check_patch, latest_aam_segments_fixture):
    """Ensure that there are active segments created after the last full file drop.
    These active segments are extracted from the targets.
    This function is used when the active segments table is being created for
    the first time.

    Expected Output
    ---------------
    +--------+----------+
    |   group|      date|
    +--------+----------+
    |11940211|2021-02-02|
    |11191954|2021-02-03|
    |11191954|2021-02-01|
    |11191954|2021-02-02|
    |18365619|2021-02-03|
    |18365619|2021-02-01|
    +--------+----------+
    """

    # Test case: Targets were created after latest full file drop
    partition_check_patch.return_value = True
    output = get_latest_aam_segments(targets_format="parquet").orderBy("group", "date")
    assert output.collect() == latest_aam_segments_fixture.collect()

    # Test case: No Targets were created after latest full file drop
    partition_check_patch.return_value = False
    with pytest.raises(Exception) as e_info:
        output = get_latest_aam_segments(targets_format="parquet").orderBy(
            "group", "date"
        )
    # Ensure that the exception message was customized correctly
    message = "No targets greater than 2021-02-01 found."
    assert e_info.value.args[0] == message


@patch.object(
    spire.targets.aam_tables.create_aam_active_segments, "get_latest_aam_segments"
)
def test_create_aam_segments_table(
    latest_aam_segments_patch,
    latest_aam_segments_fixture,
    aam_extract_segments_fixture,
    create_aam_segments_table_fixture,
):
    """Ensure that the segments are active, have correct start dates and last
    processed dates.
    This function is used when the active segments table is being created for
    the first time.

    Expected Input
    --------------
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    |  11940211|             married|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+

    Expected Output
    ---------------
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  11940211|             married|2021-01-31|    null|         2021-02-02|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-02-03|
    |  18365619|TransUnion_Consum...|2021-01-31|    null|         2021-02-03|
    +----------+--------------------+----------+--------+-------------------+
    """
    # Patch the output of get_latest_aam_segments tested in the previous test
    # to avoid repeating the same patches here
    latest_aam_segments_patch.return_value = latest_aam_segments_fixture

    output_table = create_aam_segments_table(aam_extract_segments_fixture)
    # Ensure that the segments are in the same order as the group by within the
    # functions shuffles the segments
    output_table = output_table.orderBy("segment_id")
    assert output_table.collect() == create_aam_segments_table_fixture.collect()


def test_update_aam_segments(
    aam_segments_table_fixture,
    aam_extract_segments_fixture,
    aam_segments_added_fixture,
    aam_segments_removed_fixture,
):
    """
    Test multiple update scenarios. update_aam_segments is called when the active
    segments table already exists.

    For these tests, the active_segments table (aam_segments_table_fixture) is as
    follows:
    Main Table
    -----------
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  11940211|             married|2021-01-31|    null|         2021-01-31|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|         2021-01-31|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|         2021-01-31|
    |  18365619|TransUnion_Consum...|2021-01-31|    null|         2021-01-31|
    +----------+--------------------+----------+--------+-------------------+

    NOTE: The inputs will be identical to the outputs of the function in Case 1 - 2
    as at each time the

    Case 1 : No new segments added
    -------------------------------
    Extracted Segments:
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    |  11940211|             married|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+
    Main Table after Extracted Segments have been added:
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    |  11940211|             married|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+
    Note: last_processed_date = null may not seem intuitive since the main table
    originally had non-null last_processed_dates for the segments. However, when
    writing the updates, the active segments table is not updated with nulls, the
    existing last processed date (2021-01-01) is retained.

    Case 2: New segment is added
    ----------------------------
    Extracted Segments:
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    |  11940211|             married|2021-01-31|    null|               null|
    |     10000|         new_segment|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+
    Note: In this case a new segment was extracted along with the older ones
    Main Table after Extracted Segments have been added:
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    |  11940211|             married|2021-01-31|    null|               null|
    |     10000|         new_segment|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+
    Similar to case 1, the last processed date will be retained for the old segments.
    However, for the new segments, the last processed date will be written as null.

    Case 2: A segment is removed
    ----------------------------
    Extracted Segments:
    +----------+--------------------+----------+--------+-------------------+
    |segment_id|        segment_name|start_date|end_date|last_processed_date|
    +----------+--------------------+----------+--------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|    null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|    null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|    null|               null|
    +----------+--------------------+----------+--------+-------------------+
    Note: In this case, the married segment is removed.
    Main Table after Extracted Segments have been updated:
    +----------+--------------------+----------+----------+-------------------+
    |segment_id|        segment_name|start_date|  end_date|last_processed_date|
    +----------+--------------------+----------+----------+-------------------+
    |  18365619|TransUnion_Consum...|2021-01-31|      null|               null|
    |  13855482|CrossPixelIntentA...|2021-01-31|      null|               null|
    |  11191954|Allergan_JUV_STAT...|2021-01-31|      null|               null|
    |  11940211|             married|2021-01-31|2021-01-31|         2021-01-31|
    +----------+--------------------+----------+----------+-------------------+
    An end date has been added to the removed segment. The last processed date
    is preserved as there's a left anti join between the main table and the
    Extracted Segments table.
    """
    execution_date = datetime.date(2021, 2, 1)

    # Case 1:  No new segments added
    updated_table = update_aam_segments(
        aam_segments_table_fixture, aam_extract_segments_fixture, execution_date
    )
    assert updated_table.collect() == aam_extract_segments_fixture.collect()

    # Case 2: New segment is added
    updated_table = update_aam_segments(
        aam_segments_table_fixture, aam_segments_added_fixture, execution_date
    )
    assert updated_table.collect() == aam_segments_added_fixture.collect()

    # Case 3: A segment is removed
    removed_segment = aam_extract_segments_fixture.limit(3)
    updated_table = update_aam_segments(
        aam_segments_table_fixture, removed_segment, execution_date
    )
    assert updated_table.collect() == aam_segments_removed_fixture.collect()
