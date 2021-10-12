import pytest
import pathlib
import datetime
from typing import Dict, Any

from sqlalchemy.orm import Session
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, IntegerType, LongType, StructField, StructType
from unittest.mock import patch, MagicMock

import spire
from spire.framework.workflows import Workflow
from spire.framework.assemble import assemble
from spire.framework.train import train
from tests.integrations import utils as integration_utils
from tests.utils import TestReporter
from spire.framework.workflows.schedule_rebalancer import ScheduleRebalancer

#############
# Constants #
#############

FEATURES_FIXTURE_PATH = "tests/integrations/stages/fixtures/features_fixture.csv"
TARGETS_FIXTURE_PATH = "tests/integrations/stages/fixtures/targets_fixture.csv"
SCORING_FEATURES_FIXTURE_PATH = (
    "tests/integrations/stages/fixtures/scoring_features_fixture.csv"
)
TARGETS_FIXTURE_SCHEMA = StructType(
    [
        StructField("xid", StringType()),
        StructField("target", LongType()),
        StructField("vendor", StringType()),
        StructField("group", StringType()),
    ]
)
SCORING_FEATURES_FIXTURE_SCHEMA = StructType(
    [StructField("infinity_id", StringType()), StructField("feature_1", IntegerType())]
)
FEATURES_FIXTURE_SCHEMA = StructType(
    [StructField("xid", StringType()), StructField("feature_1", IntegerType())]
)

BINARY_DATA_REQUIREMENTS_PATCH = {
    "max_positive": 10,
    "min_positive": 0,
    "min_negative": 0,
    "logic": "min_max_provided",
    "max_negative": 10,
}

############
# Fixtures #
############


@pytest.fixture(scope="module")
def run_date() -> datetime.date:
    return datetime.date.today()


@pytest.fixture(scope="module")
def features_fixture(spark: SparkSession) -> DataFrame:
    """Features for Assembly"""
    return spark.read.csv(
        FEATURES_FIXTURE_PATH, header=True, schema=FEATURES_FIXTURE_SCHEMA
    )


@pytest.fixture(scope="module")
def targets_fixture(spark: SparkSession) -> DataFrame:
    """Targets for Assembly"""
    return spark.read.csv(
        TARGETS_FIXTURE_PATH, header=True, schema=TARGETS_FIXTURE_SCHEMA
    )


@pytest.fixture(scope="module")
def scoring_features_fixture(spark: SparkSession) -> DataFrame:
    """Features for Scoring (matched to features_fixture)"""
    return spark.read.csv(
        SCORING_FEATURES_FIXTURE_PATH,
        header=True,
        schema=SCORING_FEATURES_FIXTURE_SCHEMA,
    )


@pytest.fixture(scope="module")
def session():
    # Used by test_stages to cache the test database across all stage tests in the
    # dependency chain.
    session = integration_utils.create_session()
    yield session
    session.close()


@pytest.fixture(scope="module")
def session_wf_def(run_date: datetime.date):
    # session_wf_def is used for package-scoped fixtures.
    return integration_utils.create_wf_def(run_date)


@pytest.fixture(scope="module")
@patch.dict(
    spire.framework.workflows.dataset.constants.BINARY_DATA_REQUIREMENTS,
    BINARY_DATA_REQUIREMENTS_PATCH,
)
def workflows(
    session_wf_def: Dict[str, Any], session: Session, run_date: datetime.date
) -> Dict[str, Workflow]:
    """
    Creates a dict of workflow fixtures, where the keys are the names of the workflows.
    Each name relates to its purpose:

    spire_foo_successful: Runs assembly, training, and scoring successfully.
    spire_foo_mde: Runs successfully through assembly and training but raises a
    MLFlowDependenceError (mde) on scoring.
    spire_foo_mhe: Skips assembly and training, and as a result raises
    MissingHistoryError (mhe) on scoring.
    spire_foo_unlaunched_scoring: Runs successfully through assembly and training and
    is scheduled to score but is force bypassed.
    """
    rb = ScheduleRebalancer()
    rb.START_DATE = run_date

    session_wf_def["enabled"] = True  # Enable workflows for downstream reports tests
    workflows = {
        "spire_foo_successful": None,
        "spire_foo_mde": None,
        "spire_foo_mhe": None,
        "spire_foo_mfe": None,
        "spire_foo_unlaunched_scoring": None,
    }
    for i, name in enumerate(workflows.keys()):
        session_wf_def["name"] = name
        # NOTE(Astha): Given that the run_date is the date we're testing,
        # we need the workflow schedule day to match the run_date.
        # NOTE(Astha): We need to reassign the schedule because utils.create_wf_def
        # only creates one schedule instance and each time it's attached
        # to a new workflow, it removes the relationship from the previous
        # workflow
        day = run_date.isoweekday() % 7
        session_wf_def["schedule"] = rb.create_schedule(day, 0)
        workflows[name] = Workflow.create_default_workflow(
            **session_wf_def, session=session
        )
        session.add(workflows[name])
        workflows[name].update_trait_id(session, i + 1)
    session.commit()
    return workflows


@pytest.fixture(scope="module")
def assembly_tmpdir(tmp_path_factory) -> pathlib.PosixPath:
    """
    Returns a package-scoped temporary directory for the assembled dataframe, which will
    be patched into assembly and referenced for training.
    """
    return tmp_path_factory.mktemp("test_dir")


@pytest.fixture(scope="module")
def reporter() -> TestReporter:
    return TestReporter()


@pytest.fixture(scope="module")
@patch.object(spire.utils.s3, "assemble_path")
def assembly_output(
    assemble_path_patch: MagicMock,
    workflows: Dict[str, Workflow],
    run_date: datetime.date,
    session: Session,
    features_fixture: DataFrame,
    targets_fixture: DataFrame,
    assembly_tmpdir: pathlib.PosixPath,
) -> Dict[str, Any]:
    """
    Runs assembly and returns the assembly_output dict as a fixture
    """
    # assembly_tmpdir is a pytest fixture so must be defined within the function rather
    # than adding return_value in the patch decorator
    assemble_path_patch.return_value = assembly_tmpdir
    responses = assemble(
        features_fixture,
        targets_fixture,
        # Pass list of workflows, excluding spire_foo_mhe which should not
        # assemble/train
        [wf for key, wf in workflows.items() if key != "spire_foo_mhe"],
        run_date,
        session,
    )
    return responses[0]  # Return only response for spire_foo_successful


@pytest.fixture(scope="module")
@patch.object(spire.utils.s3, "assemble_path")
def training_output(
    assemble_path_patch: MagicMock,
    spark: SparkSession,
    workflows: Dict[str, Workflow],
    run_date: datetime.date,
    session: Session,
    assembly_tmpdir: pathlib.PosixPath,
) -> Dict[str, Any]:
    """
    Runs training with reference to the temporary directory of the assembled dataset,
    and returns the training_output dict as a fixture.
    """
    # assembly_tmpdir is a pytest fixture so must be defined within the function rather
    # than adding return_value in the patch decorator
    assemble_path_patch.return_value = assembly_tmpdir
    # Exclude spire_foo_mhe which should not assemble/train
    responses = []
    for name, wf in workflows.items():
        if name != "spire_foo_mhe":
            responses.append(train(spark, wf, run_date, session))
    return responses[0]  # Return only response for spire_foo_successful
