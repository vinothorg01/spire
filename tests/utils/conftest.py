import pytest
from tests.utils import TestReporter


@pytest.fixture(scope="package")
def reporter() -> TestReporter:
    return TestReporter()
