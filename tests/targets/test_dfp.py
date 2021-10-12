"""Tests processing of DFP data to generate targets."""

from spire.targets import TargetsRunner
from spire.targets import DFPClicksConfig
from spire.targets import DFPImpressionsConfig

# Constants

ORDER_ID = [2790192174]


# Tests


def test_dfp_clicks_transform(
    dfp_clicks_input_fixture, dfp_clicks_transform_fixture, date_fixture
):
    """Test pre-processing of dfp clicks data."""

    dfp_clicks = DFPClicksConfig(date_fixture)

    transform_output = dfp_clicks.transform(
        dfp_clicks_input_fixture, order_ids=ORDER_ID
    )

    # Convert to pandas b/c can't perform set operations on map type columns
    output_df = transform_output.toPandas()
    fixture_df = dfp_clicks_transform_fixture.toPandas()
    assert output_df.equals(fixture_df)


def test_dfp_clicks_map_schema(
    dfp_clicks_transform_fixture, dfp_clicks_targets_fixture, date_fixture
):
    """Test mapping of processed data to targets schema."""

    dfp_clicks = DFPClicksConfig(date_fixture)
    dfp_clicks_runner = TargetsRunner(dfp_clicks)

    targets_output = dfp_clicks_runner.map_schema(dfp_clicks_transform_fixture)

    # The dataframe of differences between computed targets and fixture should be empty
    diff_output_fixture = targets_output.exceptAll(dfp_clicks_targets_fixture).count()
    assert diff_output_fixture == 0


def test_dfp_impressions_transform(
    dfp_imp_input_fixture, dfp_imp_transform_fixture, date_fixture
):
    """Test pre-processing of dfp impressions data."""

    dfp_imp = DFPImpressionsConfig(date_fixture)
    transform_output = dfp_imp.transform(dfp_imp_input_fixture, order_ids=ORDER_ID)

    output_df = transform_output.toPandas()
    fixture_df = dfp_imp_transform_fixture.toPandas()
    assert output_df.equals(fixture_df)


def test_dfp_impressions_map_schema(
    dfp_imp_transform_fixture, dfp_imp_targets_fixture, date_fixture
):
    """Test mapping of processed data to targets schema."""
    dfp_imp = DFPImpressionsConfig(date_fixture)
    dfp_imp_runner = TargetsRunner(dfp_imp)

    targets_output = dfp_imp_runner.map_schema(dfp_imp_transform_fixture)

    diff_output_fixture = targets_output.exceptAll(dfp_imp_targets_fixture).count()
    assert diff_output_fixture == 0
