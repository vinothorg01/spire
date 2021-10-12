from unittest.mock import MagicMock
from spire.utils import constants
from spire.utils.slack import SpireReporter


class TestReporter(SpireReporter):
    def __init__(self):
        self.slack_client = MagicMock()
        self.name = constants.SLACKBOT_NAME
        self.on_call = constants.ON_CALL
        self.env = "foo"
        self.channel_name = constants.SLACK_STD_OUT_CHANNEL
        self.channel_id = self.get_channel_id()


def test_reporter(reporter: TestReporter):
    # Crude test that the reporter can be instantiated correctly
    assert reporter.name == "Spire Reporter"
