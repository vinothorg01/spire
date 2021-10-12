import pytest
from tests.data.cluster_config import generic_config as sample_config


@pytest.fixture(scope="module")
def cluster_config():
    return sample_config
