"""Tests ga and cds utils functions"""
import pytest
from unittest.mock import patch
import datetime
import spire.utils.s3
from spire.targets.utils import (
    get_ga_files_uri_list,
    _check_ga_data_exists,
    _get_ga_files,
    _get_ga_xids,
    _collect_all_cds_data,
    _load_and_filter_ga_ids,
)
from tests.targets.conftest import CDS_ALT_FIXTURES_PATH, GA_IDS_FIXTURE_PATH

# Fixtures

ga_data_keys_fixture = [
    {
        "Key": "google/normalised_ga/source=GA/type=infinity_id/dt=2021-01-31/_SUCCESS",  # noqa
        "LastModified": datetime.datetime(2019, 1, 2),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
    {
        "Key": "google/normalised_ga/source=GA/type=infinity_id/dt=2018-12-01/part-jsjs",  # noqa
        "LastModified": datetime.datetime(2019, 1, 2),
        "ETag": '"3bc2025a96c6825369bb4a2aa1f8ee81"',
        "Size": 585,
        "StorageClass": "STANDARD",
    },
]

# Helpers


def get_dates_list(start_date, n_days):
    return [
        (start_date - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(n_days + 1)
    ]


# Tests


@patch.object(spire.utils.s3, "get_all_keys")
def test_get_ga_files(get_all_keys_patch):
    get_all_keys_patch.return_value = ga_data_keys_fixture
    assert _get_ga_files("infinity_id") == [
        "google/normalised_ga/source=GA/type=infinity_id/dt=2021-01-31/"
    ]


def test_check_ga_data_exists(date_fixture):
    dates_list = get_dates_list(date_fixture, n_days=0)
    file_dates_dict = {
        "2021-01-31": "google/normalised_ga/source=GA/type=infinity_id/dt=2021-01-31/"
    }
    assert _check_ga_data_exists(dates_list, file_dates_dict) == ["2021-01-31"]


@patch.object(spire.utils.s3, "get_all_keys")
def test_get_ga_files_uri_list(get_all_keys_patch, date_fixture):
    get_all_keys_patch.return_value = ga_data_keys_fixture
    # Case when there is 1 date
    dates_list = get_dates_list(date_fixture, n_days=0)
    assert get_ga_files_uri_list(dates_list, "infinity_id") == [
        "s3a://cn-data-vendor/google/normalised_ga/source=GA/type=infinity_id/dt=2021-01-31/"  # noqa
    ]

    # Case when there are 2 dates but there is no GA Data for 1 of them.
    # It should still return the path for the file that has the data
    dates_list = get_dates_list(date_fixture, n_days=1)
    assert get_ga_files_uri_list(dates_list, "infinity_id") == [
        "s3a://cn-data-vendor/google/normalised_ga/source=GA/type=infinity_id/dt=2021-01-31/"  # noqa
    ]
    # When there are no files for the date of interest
    with pytest.raises(Exception):
        dates_list = ["2021-02-01"]
        get_ga_files_uri_list(dates_list, "infinity_id")


@patch("spire.targets.constants.GA_NORM_TABLE_URI", GA_IDS_FIXTURE_PATH)
def test_get_ga_xids(ga_xids_fixture, date_fixture):
    """Test transforming ga ids to ga xids"""
    # Need to rename the date column of ga_ids_fixture which has raw ga norm data.
    # _load_and_filter_ga_ids loads the fixture using the path in the patch,
    # filters the dates and renames the date column for the test
    ga_ids = _load_and_filter_ga_ids(date_fixture, 0)

    ga_xids_output = _get_ga_xids(ga_ids)

    num_diff_output_fixture = ga_xids_output.exceptAll(ga_xids_fixture).count()
    assert num_diff_output_fixture == 0


@patch("spire.targets.constants.CDS_ALT_URI", CDS_ALT_FIXTURES_PATH)
def test_collect_all_cds_data(cds_all_data_fixture, ga_xids_fixture):
    """Use the 3 cds table fixtures to test collection of cds data.

    Implicitly tests join_cds_from_ga.
    The patches read fixtures into _load_cds_tables() by modifying
    the path.
    """
    cds_data_output = _collect_all_cds_data(ga_xids_fixture)

    num_diff_output_fixture = cds_data_output.exceptAll(cds_all_data_fixture).count()
    assert num_diff_output_fixture == 0
