import pytest
import datetime

from tests.integrations import utils


@pytest.fixture(scope="package")
def run_date() -> datetime.date:
    return datetime.date.today()


@pytest.fixture(scope="function")
def session():
    # NOTE: In effect this is function_session, but to change this would be to refactor
    # nearly every integrations test script
    session = utils.create_session()
    yield session
    session.close()


@pytest.fixture(scope="function")
def wf_def(run_date: datetime.date):
    return utils.create_wf_def(run_date)


@pytest.fixture(scope="function")
def wf_dataset():
    return {
        "family": "binary",
        "classes": {
            "positive": {
                "logic": "matches_any",
                "args": {
                    "behaviors": [
                        {"aspect": {}, "vendor": "adobe", "group": "16501879"}
                    ]
                },
            },
            "negative": {
                "logic": "matches_any",
                "args": {"behaviors": [{"aspect": {}, "vendor": "adobe"}]},
            },
        },
        "data_requirements": {
            "logic": "min_max_provided",
            "min_positive": 100,
            "min_negative": 100,
            "max_positive": 250000,
            "max_negative": 250000,
        },
    }
