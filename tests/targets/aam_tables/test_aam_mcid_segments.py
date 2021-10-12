import datetime
import pytest
import spire.utils.s3
from unittest.mock import patch
from tests.targets.fixtures import aam_fixtures as fixtures
from tests.targets.conftest import AAM_MCID_SEGMENTS_FIXTURE_PATH
import spire.targets.aam_tables.create_aam_mcid_segments
from spire.targets.aam_tables.create_aam_mcid_segments import (
    get_info_files_to_process,
    get_aam_files,
    create_aam_info_file_dict,
    load_mcid_segments,
    filter_info_files_by_timestamp,
    get_sync_files_uri_list,
    parse_aam_segments,
    get_latest_mcid_segments_timestamp,
    get_latest_ref_date,
    get_aam_sync_file_names_list,
    load_and_process_aam_segments,
    load_aam_segments_data,
    update_mcid_segments,
)


#############
# Constants #
#############

LATEST_MCID_TABLE_TIMESTAMP = 1612500000
LATEST_FULL_FILE_TIMESTAMP = 1539866190
TEST_TIMESTAMP = 1501839790
MCID_TABLE_REF_DATE = datetime.date(2021, 2, 1)
INFO_FILE = "S3_77904_1955_iter_1612500023000.info"
SYNC_FILES_LIST = ["S3_77904_1955_iter_1612500023000.sync"]
AAM_INFO_FILE_FIXTURE_PATH = (
    "tests/targets/fixtures/S3_77904_1955_iter_1612500023000.info"
)


#########
# Tests #
#########


@patch.object(spire.utils.s3, "get_all_keys")
def test_get_aam_files(get_all_keys_patch):
    """Test the extraction of file names and file URIs from s3 for info and sync files.

    Expected Input
    -------------
    {
        "Key": "adobe/aamsegsmcid/2018-10-10/S3_77904_1955_iter_1539869790000.info",
        "LastModified": datetime.datetime(2019, 1, 3),
        "ETag": '"6e97cfc43834bbeff178721121f69da0"',
        "Size": 585,
        "StorageClass": "STANDARD",
    }
    Expected Output
    --------------
    {
    "S3_77904_1955_iter_1539869790000.info": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-10/S3_77904_1955_iter_1539869790000.info"
    )
    """
    # get_all_keys searches for information in a s3 bucket and hence must be patched
    get_all_keys_patch.return_value = fixtures.AAM_KEYS
    assert get_aam_files(".info") == fixtures.AAM_INFO_FILES
    assert get_aam_files(".sync") == fixtures.AAM_SYNC_FILES


def test_create_aam_info_file_dict():
    """Test the extraction of information from file names/URIs into a dict.

    Example Input
    -------------
    {
    "S3_77904_1955_iter_1539869790000.info": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
    + "2018-10-10/S3_77904_1955_iter_1539869790000.info"
    )
    Example Output
    --------------
    {
        "S3_77904_1955_iter_1539869890000.info": {
        "type": "iter",
        "timestamp": 1539869890,
        "date": datetime.date(2018, 10, 18),
        "uri": "s3a://cn-data-vendor/adobe/aamsegsmcid/"
        + "2018-10-10/S3_77904_1955_iter_1539869890000.info",
    }
    }
    """
    assert (
        create_aam_info_file_dict(fixtures.AAM_INFO_FILES)
        == fixtures.AAM_INFO_FILES_DICT
    )


def test_load_mcid_segments():
    """Test that the mcid segments table is loaded and if not, its value is
    set to None.

    Expected Output
    ---------------
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
    # TODO: Create delta table for mcid_segments instead of parquet
    # Case when the table exists
    with patch(
        "spire.targets.constants.MCID_SEGMENTS_TABLE_URI",
        AAM_MCID_SEGMENTS_FIXTURE_PATH,
    ):
        assert load_mcid_segments(input_format="parquet").count() > 0

    # Case when the table doesn't exist
    with patch("spire.targets.constants.MCID_SEGMENTS_TABLE_URI", "random/path"):
        assert load_mcid_segments() is None


def test_get_latest_mcid_segments_timestamp(aam_mcid_segments_fixture):
    """Test to ensure the latest timestamp is extracted - either from the mcid segments
    table or from the latest full file in the table's absence."""

    # Case when table exists
    assert (
        get_latest_mcid_segments_timestamp(
            aam_mcid_segments_fixture, fixtures.AAM_INFO_FILES_DICT
        )
        == LATEST_MCID_TABLE_TIMESTAMP
    )

    # Case when table does not exist and the latest full file timestamp must be used
    assert (
        get_latest_mcid_segments_timestamp(None, fixtures.AAM_INFO_FILES_DICT)
        == LATEST_FULL_FILE_TIMESTAMP
    )


def test_filter_info_files_by_timestamp():
    """Test whether AAM files are filtered appropriately by timestamp.

    Test that the returned list only contains files with timestamps greater than the
    input timestamp.
    """

    # Test multiple timestamps
    assert filter_info_files_by_timestamp(
        fixtures.AAM_INFO_FILES_DICT, LATEST_FULL_FILE_TIMESTAMP
    ) == [
        "S3_77904_1955_full_1539869790000.info",
        "S3_77904_1955_iter_1539869890000.info",
        "S3_77904_1955_iter_1612500023000.info",
    ]
    assert filter_info_files_by_timestamp(
        fixtures.AAM_INFO_FILES_DICT, TEST_TIMESTAMP
    ) == [
        "S3_77904_1955_full_1521839790000.info",
        "S3_77904_1955_full_1539869790000.info",
        "S3_77904_1955_iter_1539869890000.info",
        "S3_77904_1955_iter_1612500023000.info",
    ]


def test_get_aam_sync_file_names_list():
    """Ensure that .sync file names are properly extracted from .info files."""
    aam_sync_file_names = get_aam_sync_file_names_list(AAM_INFO_FILE_FIXTURE_PATH)
    assert aam_sync_file_names == SYNC_FILES_LIST


@patch.object(spire.utils.s3, "get_all_keys")
def test_get_sync_files_uri_list(get_all_keys_patch):
    """Ensure that .sync file URIs exist."""
    get_all_keys_patch.return_value = fixtures.AAM_KEYS

    # Test when all sync files listed have file URIs/exist
    sync_file_names = list(fixtures.AAM_SYNC_FILES.keys())
    assert get_sync_files_uri_list(sync_file_names) == list(
        fixtures.AAM_SYNC_FILES.values()
    )

    # Test when a sync file listed in info file does not exist/cannot be found
    sync_file_names.append("invalid_key")
    with pytest.raises(KeyError):
        get_sync_files_uri_list(sync_file_names)


@patch.object(spire.utils.s3, "get_all_keys")
def test_load_aam_segments_data(get_all_keys_patch, aam_unparsed_fixture):
    """Ensure that data from all .sync files is found and loaded correctly."""

    # In this case, the runthrough AAM keys are used as they contain
    # file locations corresponding to fixtures, not s3 buckets like AAM_KEYS
    get_all_keys_patch.return_value = fixtures.AAM_RUNTHROUGH_KEYS

    # Case when all files are found.
    # The DATA_VENDOR_URI patch ensures that segments are loaded from fixtures.
    # Without this patch, the URI becomes invalid.
    with patch("spire.targets.constants.DATA_VENDOR_URI", "tests/"):
        output = load_aam_segments_data(
            SYNC_FILES_LIST, max_tries=3, wait_time_per_check=1
        )
    assert output.collect() == aam_unparsed_fixture.collect()

    # Case when files can't be found and max tries have been achieved.
    # This is the case wherein the URI is invalid. Such an edge case may
    # occur if this process is running while a file transfer is occurring.
    # The process is supposed to check for updated URIs in wait_time_per_check
    # intervals, however, if it cannot find one by the time max_tries have elapsed,
    # the process terminates.
    with pytest.raises(Exception) as e:
        load_aam_segments_data(SYNC_FILES_LIST, max_tries=3, wait_time_per_check=1)
    # Ensure that the exception message was customized correctly
    message = "Failed 3 times. Try again after files are transferred."
    assert str(e.value) == message


@patch("spire.targets.constants.MCID_REGEX", "")
def test_parse_aam_segments(aam_unparsed_fixture, aam_parse_segs_fixture):
    """Ensure that segments from .sync files are parsed correctly.

    Expected input (.sync file contents)
    ------------------------------------
    D 18365619

    Expected output
    ---------------
    +----+-------------+
    |mcid|segment_array|
    +----+-------------+
    |   D|   [18365619]|
    +----+-------------+
    Note: Actual MCIDs are not simply letters and follow a pattern represented by
    MCID_REGEX. However, to keep it simple, mcids are letters for testing and hence
    the regex is patched.
    """
    parsed_output = parse_aam_segments(aam_unparsed_fixture)
    assert parsed_output.collect() == aam_parse_segs_fixture.collect()


@pytest.mark.dependency()
@patch.object(spire.utils.s3, "get_all_keys")
def test_get_info_files_to_process(get_all_keys_patch, aam_mcid_segments_fixture):
    """Test the runthrough of get_info_files_to_process which is a wrapper function
    for get_aam_files, create_aam_info_file_dict, get_latest_mcid_segments_timestamp,
    and filter_info_files_by_timestamp.


    Note: This test needs to run before test_get_latest_ref_date as that alters
    one of the fixtures needed in subsequent runthrough tests.
    """
    get_all_keys_patch.return_value = fixtures.AAM_KEYS
    info_files_to_process, aam_info_file_dict = get_info_files_to_process(
        aam_mcid_segments_fixture
    )
    assert aam_info_file_dict == fixtures.AAM_INFO_FILES_DICT
    assert info_files_to_process == [INFO_FILE]


@pytest.mark.dependency(depends=["test_get_info_files_to_process"])
def test_get_latest_ref_date(aam_mcid_segments_fixture):
    """Test that ref_date is extracted differently for full versus iter files.

    For AAM, iter and full files are processed one at a time.
    AAM_INFO_FILES_DICT contains the information of all files that need to be
    processed. The information of each file is in turn stored in dicts.
    Since we are testing different cases, we need to extract the information
    of specific files and hence different files are being accessed here.
    """

    # Case: iter file.
    # ref date = date of latest full file extracted from the mcid segments table
    info_file_dict = fixtures.AAM_INFO_FILES_DICT[INFO_FILE]
    output_dict = get_latest_ref_date(info_file_dict, aam_mcid_segments_fixture)
    assert output_dict["ref_date"] == MCID_TABLE_REF_DATE

    # Case: full file.
    # ref date = date of the full file
    info_file_dict = fixtures.AAM_INFO_FILES_DICT[
        "S3_77904_1955_full_1521839790000.info"
    ]
    output_dict = get_latest_ref_date(info_file_dict, aam_mcid_segments_fixture)
    assert output_dict["ref_date"] == info_file_dict["date"]


def test_update_mcid_segments(aam_parse_segs_fixture, aam_update_table_fixture):
    """Ensure that information from the new file is added correctly to a dataframe.
    This dataframe will be appended to the mcid_segments_table.

    Expected Input (ITER_1612500023000_INFO_FILE_DICT + parsed_segments)
    -------------------------------------------------
    +----+-------------+
    |mcid|segment_array|
    +----+-------------+
    |   D|   [18365619]|
    +----+-------------+

    Expected Output
    ---------------
    +----------+----+----------+----+-------------+
    |  ref_date|type| timestamp|mcid|segment_array|
    +----------+----+----------+----+-------------+
    |2021-02-01|iter|1612500023|   D|   [18365619]|
    +----------+----+----------+----+-------------+
    """
    # Test for a single file - iter_1612500023000
    update_output = update_mcid_segments(
        fixtures.ITER_1612500023000_INFO_FILE_DICT, aam_parse_segs_fixture
    )
    assert update_output.collect() == aam_update_table_fixture.collect()


@patch("spire.targets.constants.MCID_REGEX", "")
@patch("spire.targets.constants.DATA_VENDOR_URI", "tests/")
@patch.object(spire.utils.s3, "get_all_keys")
def test_load_and_process_aam_segments(get_all_keys_patch, aam_update_table_fixture):
    """Test multiple functions. load_and_process_aam_segments is a wrapper around
    get_aam_sync_file_names_list, load_aam_segments_data, parse_aam_segments and
    update_mcid_segments.

    For information on the patches, see the aforementioned functions.
    """
    get_all_keys_patch.return_value = fixtures.AAM_RUNTHROUGH_KEYS
    # Test for a single file - iter_1612500023000
    aam_segments_output = load_and_process_aam_segments(
        fixtures.ITER_1612500023000_INFO_FILE_DICT
    )
    assert aam_segments_output.collect() == aam_update_table_fixture.collect()
