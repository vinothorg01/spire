import os
import pytest

from spire.config import DefaultRemoteConfig


ENV = os.environ.get("DEPLOYMENT_ENV", "test")


def test_config():
    with pytest.raises(Exception):
        return DefaultRemoteConfig()
