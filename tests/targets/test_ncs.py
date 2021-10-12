"""Test processing of NCS Custom & Syndicated data to generate targets."""
from unittest.mock import patch
from spire.targets import NCSSyndicatedConfig
from spire.targets import NCSCustomConfig
from spire.targets import TargetsRunner
from tests.targets.conftest import GA_IDS_FIXTURE_PATH, CDS_ALT_FIXTURES_PATH


# Tests


@patch("spire.targets.constants.CDS_ALT_URI", CDS_ALT_FIXTURES_PATH)
@patch("spire.targets.constants.GA_NORM_TABLE_URI", GA_IDS_FIXTURE_PATH)
def test_ncs_custom_transform(
    ncs_custom_input_fixture,
    ncs_custom_transform_fixture,
    date_fixture,
):
    """Test pre-processing of NCS custom data.

    The patches read fixtures into _load_and_filter_ga_ids()
    and _load_cds_tables() by modifying the path.
    """

    ncs_cus = NCSCustomConfig(date_fixture)
    ncs_cus_runner = TargetsRunner(ncs_cus)

    transform_output = ncs_cus_runner.transform(ncs_custom_input_fixture)
    # The dataframe of differences between processed input and fixture should be empty
    num_diff_output_fixture = transform_output.exceptAll(
        ncs_custom_transform_fixture
    ).count()
    assert num_diff_output_fixture == 0


def test_ncs_custom_map_schema(
    ncs_custom_transform_fixture, ncs_custom_targets_fixture, date_fixture
):
    """Test mapping of processed data to targets schema."""
    ncs_cus = NCSCustomConfig(date_fixture)
    ncs_cus_runner = TargetsRunner(ncs_cus)

    targets_output = ncs_cus_runner.map_schema(ncs_custom_transform_fixture)

    num_diff_output_fixture = targets_output.exceptAll(
        ncs_custom_targets_fixture
    ).count()
    assert num_diff_output_fixture == 0


@patch("spire.targets.constants.CDS_ALT_URI", CDS_ALT_FIXTURES_PATH)
@patch("spire.targets.constants.GA_NORM_TABLE_URI", GA_IDS_FIXTURE_PATH)
def test_ncs_syn_transform(
    ncs_syn_input_fixture,
    ncs_syn_transform_fixture,
    date_fixture,
):
    """Test pre-processing of NCS syndicated data.

    The patches read fixtures into _load_and_filter_ga_ids()
    and _load_cds_tables() by modifying the path.
    """

    ncs_syn = NCSSyndicatedConfig(date_fixture)
    ncs_syn_runner = TargetsRunner(ncs_syn)

    transform_output = ncs_syn_runner.transform(ncs_syn_input_fixture)

    num_diff_output_fixture = transform_output.exceptAll(
        ncs_syn_transform_fixture
    ).count()
    assert num_diff_output_fixture == 0


def test_ncs_syn_map_schema(
    ncs_syn_transform_fixture, ncs_syn_targets_fixture, date_fixture
):
    """Test mapping of processed data to targets schema."""

    ncs_syn = NCSSyndicatedConfig(date_fixture)
    ncs_syn_runner = TargetsRunner(ncs_syn)

    targets_output = ncs_syn_runner.map_schema(ncs_syn_transform_fixture)

    num_diff_output_fixture = targets_output.exceptAll(ncs_syn_targets_fixture).count()
    assert num_diff_output_fixture == 0
