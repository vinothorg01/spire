"""Tests mapping of DAR data to generate targets.
   Processing is not tested because data is just loaded and mapped."""

from spire.targets import DARConfig
from spire.targets import TargetsRunner
from tests.targets.conftest import date_fixture

# Initialize runner

dar = DARConfig(date_fixture)
dar_runner = TargetsRunner(dar)

# Tests


def test_dar_map_schema(dar_input_fixture, dar_targets_fixture):
    """Test mapping of processed data to targets schema."""
    targets_output = dar_runner.map_schema(dar_input_fixture)

    num_diff_output_fixture = targets_output.exceptAll(dar_targets_fixture).count()
    assert num_diff_output_fixture == 0
