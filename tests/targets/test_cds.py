"""Tests processing of CDS data to generate targets."""
from unittest.mock import patch
from spire.targets import TargetsRunner
from spire.targets import CDSIndividualConfig
from spire.targets import CDSIndividualDemoConfig
from tests.targets.conftest import GA_IDS_FIXTURE_PATH, CDS_ALT_FIXTURES_PATH

# Tests


@patch("spire.targets.constants.CDS_ALT_URI", CDS_ALT_FIXTURES_PATH)
@patch("spire.targets.constants.GA_NORM_TABLE_URI", GA_IDS_FIXTURE_PATH)
def test_cds_indiv_transform(
    cds_indiv_fixture,
    cds_indiv_transform_fixture,
    date_fixture,
):
    """Test pre-processing of CDS data.

    The patches read fixtures into _load_and_filter_ga_ids()
    and _load_cds_tables() by modifying the path.
    """

    cds_indiv = CDSIndividualConfig(date_fixture)
    cds_indiv_runner = TargetsRunner(cds_indiv)

    transform_output = cds_indiv_runner.transform(cds_indiv_fixture)

    # The dataframe of differences between processed input and fixture should be empty
    num_diff_output_fixture = transform_output.exceptAll(
        cds_indiv_transform_fixture
    ).count()
    assert num_diff_output_fixture == 0


def test_cds_indiv_map_schema(
    cds_indiv_transform_fixture, cds_indiv_targets_fixture, date_fixture
):
    """Test mapping of processed data to targets schema."""

    cds_indiv = CDSIndividualConfig(date_fixture)
    cds_indiv_runner = TargetsRunner(cds_indiv)

    targets_output = cds_indiv_runner.map_schema(cds_indiv_transform_fixture)

    num_diff_output_fixture = targets_output.exceptAll(
        cds_indiv_targets_fixture
    ).count()
    assert num_diff_output_fixture == 0


@patch("spire.targets.constants.CDS_ALT_URI", CDS_ALT_FIXTURES_PATH)
@patch("spire.targets.constants.GA_NORM_TABLE_URI", GA_IDS_FIXTURE_PATH)
def test_cds_indiv_demo_transform(
    cds_indiv_demo_fixture,
    cds_indiv_demo_transform_fixture,
    date_fixture,
):
    """Test pre-processing of CDS data.

    The patches read fixtures into _load_and_filter_ga_ids()
    and _load_cds_tables() by modifying the path.
    """

    cds_indiv_demo = CDSIndividualDemoConfig(date_fixture)
    cds_indiv_d_runner = TargetsRunner(cds_indiv_demo)

    transform_output = cds_indiv_d_runner.transform(cds_indiv_demo_fixture)

    # The dataframe of differences between processed input and fixture should be empty
    num_diff_output_fixture = transform_output.exceptAll(
        cds_indiv_demo_transform_fixture
    ).count()
    assert num_diff_output_fixture == 0


def test_cds_map_schema(
    cds_indiv_demo_transform_fixture, cds_indiv_demo_targets_fixture, date_fixture
):
    """Test mapping of processed data to targets schema."""
    cds_indiv_demo = CDSIndividualDemoConfig(date_fixture)
    cds_indiv_d_runner = TargetsRunner(cds_indiv_demo)

    targets_output = cds_indiv_d_runner.map_schema(cds_indiv_demo_transform_fixture)

    num_diff_output_fixture = targets_output.exceptAll(
        cds_indiv_demo_targets_fixture
    ).count()
    assert num_diff_output_fixture == 0
