# TODO(Max):
# 1. Move and/or reorganize fixtures
# 2. Rework tests into test classes (how should they be organized?)

import pytest
import datetime
from unittest.mock import MagicMock
from typing import Dict, Any
from unittest.mock import patch
from sqlalchemy.orm import Session
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

import spire
from spire.framework.workflows import Workflow, WorkflowStages
from spire.framework.score import score, errors
from spire.framework.execution import JobStatuses
from tests.utils import TestReporter


@pytest.mark.dependency()
def test_workflow_definition(
    session_wf_def: Dict[str, Any], workflows: Dict[str, Workflow]
):
    """
    Test that the attributes of workflow.dataset are correct.

    Parent Dependence: None
    Child Dependence: test_assembly
    """
    workflow_successful = workflows["spire_foo_successful"]
    # A very roundabout way to retrieve vendor since workflow_successful's name
    # violates assumptions of the workflow.vendor property method
    dataset = workflow_successful.dataset.definition
    vendor = dataset["classes"]["positive"]["args"]["behaviors"][0]["vendor"]
    group = dataset["classes"]["positive"]["args"]["behaviors"][0]["group"]
    assert session_wf_def["vendor"] == vendor
    assert session_wf_def["groups"][0] == group
    assert isinstance(workflow_successful.dataset.definition, dict)


@pytest.mark.dependency(depends=["test_workflow_definition"])
def test_assembly(assembly_output: Dict[str, Any]):
    """
    Test that assembly was successful.

    Parent Dependence: test_workflow_definition
    Child Dependence: test_training
    """
    assert assembly_output["error"] is None


@pytest.mark.dependency(depends=["test_assembly"])
def test_training(training_output: Dict[str, Any]):
    """
    Test that training was successful.

    Parent Dependence: test_assembly
    Child Dependence: test_scoring, test_unlaunched_training
    """
    assert training_output["error"] is None


@pytest.mark.dependency(depends=["test_training"])
@patch.object(spire.framework.score.scoring, "write_scores", MagicMock(return_value={}))
@patch.object(spire.framework.score.scoring, "load_features")
def test_scoring_successful(
    load_features_patch: MagicMock,
    session: Session,
    spark: SparkSession,
    workflows: Dict[str, Workflow],
    run_date: datetime.date,
    scoring_features_fixture: DataFrame,
):
    """
    Parent Dependence: test_training
    Child Dependence: test_get_info

    Test that scoring was successful. Uses scoring_features_fixture, a small fake
    dataset matched to the assembly dataset.
    """
    # scoring_features_fixture is a pytest fixture so must be defined within the
    # function rather than adding return_value in the patch decorator
    load_features_patch.return_value = scoring_features_fixture
    workflow_successful = workflows["spire_foo_successful"]
    successful_response = score(workflow_successful, run_date, session, spark=spark)
    assert successful_response["error"] is None


message = (
    "This training history report is missing an MLFlow run_id or"
    " experiment_id. Has this model been trained?"
)


@pytest.mark.dependency(depends=["test_training"])
@patch.object(
    spire.framework.score.scoring,
    "get_mlflow_ids",
    MagicMock(side_effect=errors.MLFlowDependenceError(message)),
)
@patch.object(spire.framework.score.scoring, "load_features")
def test_scoring_mde(
    load_features_patch: MagicMock,
    session: Session,
    workflows: Dict[str, Workflow],
    run_date: datetime.date,
    scoring_features_fixture: DataFrame,
):
    """
    Parent Dependence: test_training
    Child Dependence: test_get_info

    Test that when an MLFlowDependenceError is raised, `score` will proceed anyway,
    produce a response dict which gets written to the workflow's history, and has the
    MLFLOW_DEPENDENCE_ERROR status which will be used for reporting.
    """
    load_features_patch.return_value = scoring_features_fixture
    workflow_mde = workflows["spire_foo_mde"]
    mde_response = score(workflow_mde, run_date, session)
    assert mde_response["status"] == JobStatuses.MLFLOW_DEPENDENCE_ERROR


@pytest.mark.dependency(depends=["test_training"])
@patch.object(spire.framework.score.scoring, "load_features")
def test_scoring_mhe(
    load_features_patch: MagicMock,
    session: Session,
    spark: SparkSession,
    workflows: Dict[str, Workflow],
    run_date: datetime.date,
    scoring_features_fixture: DataFrame,
):
    """
    Parent Dependence: test_training
    Child Dependence: test_get_info

    Test that when a workflow has not been trained, it will fail to score and raise a
    MISSING_HISTORY_ERROR status which will be used for reporting.
    """
    load_features_patch.return_value = scoring_features_fixture
    workflow_mhe = workflows["spire_foo_mhe"]
    mhe_response = score(workflow_mhe, run_date, session, spark=spark)
    assert mhe_response["status"] == JobStatuses.MISSING_HISTORY_ERROR


@pytest.mark.dependency(depends=["test_training"])
@patch.object(spire.framework.score.scoring, "load_features")
def test_scoring_mfe(
    load_features_patch: MagicMock,
    session: Session,
    spark: SparkSession,
    workflows: Dict[str, Workflow],
    run_date: datetime.date,
):
    """
    Parent Dependence: test_training
    Child Dependence: test_get_info

    Test that when a workflow has no features, it will fail to score and raise a
    MISSING_FEATURES_ERROR status which will be used for reporting.
    """
    load_features_patch.return_value = spark.createDataFrame([], StructType([]))
    workflow_mfe = workflows["spire_foo_mfe"]
    mfe_response = score(workflow_mfe, run_date, session, spark=spark)
    assert mfe_response["status"] == JobStatuses.MISSING_FEATURES_ERROR


@pytest.mark.dependency(
    depends=[
        "test_scoring_successful",
        "test_scoring_mde",
        "test_scoring_mhe",
        "test_scoring_mfe",
    ]
)
def test_get_info(
    reporter: TestReporter,
    run_date: datetime.date,
    session: Session,
):
    """
    Uses scoring, but serves as a general test of how the reporter counts are calculated
    """
    # TODO(Max): Much of this logic should be moved to unit tests, basically everything
    # besides the 1-1 asserts between workflow and count
    counts, _, missing_dfs = reporter.get_info(session=session, arg_date=run_date)
    workflows = Workflow.load_all_enabled(session)
    scoring_counts = counts[str(WorkflowStages.SCORING)]
    # Consolidate into single assert
    assert scoring_counts["num_workflows"] == len(workflows)
    assert scoring_counts[str(JobStatuses.SUCCESS)] == 1
    assert scoring_counts[str(JobStatuses.MISSING_HISTORY_ERROR)] == 1
    assert scoring_counts[str(JobStatuses.MLFLOW_DEPENDENCE_ERROR)] == 1
    assert scoring_counts[str(JobStatuses.MISSING_FEATURES_ERROR)] == 1
    assert (
        len(missing_dfs[str(WorkflowStages.SCORING)])
        == scoring_counts["num_not_launched"]
    )
    num_launched_fixture = sum(
        [
            value
            for key, value in scoring_counts.items()
            if key not in ["num_workflows", "num_launched", "num_not_launched"]
        ]
    )
    assert scoring_counts["num_launched"] == num_launched_fixture
    num_not_launched_fixture = (
        scoring_counts["num_workflows"] - scoring_counts["num_launched"]
    )
    assert scoring_counts["num_not_launched"] == num_not_launched_fixture


@pytest.mark.dependency(depends=["test_training"])
def test_unlaunched_training(
    reporter: TestReporter, run_date: datetime.date, session: Session
):
    """
    Test that the query from get_info only selects workflows with the appropriate
    schedules. Otherwise, num_not_launched will end up including workflows with
    different schedules as if they were unlaunched.

    These stages are more complex to calculate than scoring, since generally all
    scoring schedules are the same, whereas assembly/training are not.
    """
    counts, _, _ = reporter.get_info(session=session, arg_date=run_date)
    del counts[str(WorkflowStages.SCORING)]  # Not needed for this test
    for stage_counts in counts.values():
        # num_launched_fixture must be hard-coded rather than derived from counts.
        # This fixture assumes workflow_successful, workflow_mde, workflow_mhe,
        # workflow_mfe, and workflow_unlaunched_scoring.
        # Only workflow_mhe should be not_launched.
        num_launched_fixture = 4
        assert stage_counts["num_launched"] == num_launched_fixture
        num_not_launched_fixture = (
            stage_counts["num_workflows"] - stage_counts["num_launched"]
        )
        assert stage_counts["num_not_launched"] == num_not_launched_fixture
        # TODO(Max): Create a new workflow that also is not assembled or trained,
        # with a different schedule. It should not get picked up in these counts.
