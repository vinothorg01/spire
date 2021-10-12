"""Tests transformation and mapping of Custom data to generate targets."""

import datetime
import pyspark.sql.functions as F
from spire.targets import CustomConfig
from spire.targets import TargetsRunner

# Constants
date = datetime.date(2020, 8, 14)
custom_source = "custom123"

# Tests


def test_custom_transform(custom_fixture):
    """Test transformation of custom data under various conditions."""
    # Case when data is present
    custom_config = CustomConfig(date)
    custom_runner = TargetsRunner(custom_config)
    targets_output = custom_runner.transform(custom_fixture)
    assert targets_output.collect() == custom_fixture.collect()

    # Case with custom source input
    custom_config = CustomConfig(date=date, source=custom_source)
    custom_runner = TargetsRunner(custom_config)
    targets_output = custom_runner.transform(custom_fixture)
    assert (
        targets_output.collect()
        == custom_fixture.filter(F.col("source") == custom_source).collect()
    )

    # Case when there's nothing to write
    custom_config = CustomConfig(datetime.date.today())
    custom_runner = TargetsRunner(custom_config)
    targets_output = custom_runner.transform(custom_fixture)
    assert targets_output.count() == 0


def test_custom_map_schema(custom_fixture, custom_mapped_fixture):
    """Test mapping of processed custom data to targets schema."""
    custom_config = CustomConfig(date)
    custom_runner = TargetsRunner(custom_config)
    mapped_output = custom_runner.map_schema(custom_fixture)
    assert mapped_output.collect() == custom_mapped_fixture.collect()
