import datetime
from typing import List
import json
from spire.tasks.run_stage import run_stage
from spire.tasks.run_targets import run_targets
from spire.framework.workflows import WorkflowStages
from tests.data.cluster_config import test_task_config


def test_run_stage_no_wf_ids(wf_ids: List[str], run_date: datetime.datetime):
    """
    Test that the job definition for the workflows of wf_ids with scoring schedule of
    run_date are correctly assigned in the job definition
    """
    jobs = run_stage(stage=WorkflowStages.SCORING, run_dates=run_date, dry_run=True)
    job_wf_ids = json.loads(jobs[0][0].notebook_task.base_parameters["workflow_ids"])
    assert job_wf_ids == wf_ids


def test_run_stage_with_wf_ids(wf_ids: List[str], run_date: datetime.datetime):
    """
    Test that when passing a list of wf_ids over a list of dates, that the list of jobs
    is correct and the job definitions are correct, even given threadpool concurrency
    """
    # Case: list of dates and list of workflow ids
    run_dates = [run_date, (run_date - datetime.timedelta(days=1))]
    jobs = run_stage(
        stage=WorkflowStages.SCORING,
        run_dates=run_dates,
        wf_ids=wf_ids,
        dry_run=True,
    )
    assert len(jobs) == len(run_dates)
    for i, run_date in enumerate(run_dates):
        job_wf_ids = json.loads(
            jobs[i][0].notebook_task.base_parameters["workflow_ids"]
        )
        assert job_wf_ids == wf_ids


def test_run_targets_jobs(run_date: datetime.datetime):
    execution_dates = [run_date, (run_date - datetime.timedelta(days=1))]
    # This is not an integrations test as its just returning the job config,
    # unlike stages that does not require a database connection.
    jobs = run_targets(
        vendor="ncs",
        source="syndicated",
        task_config=test_task_config,
        execution_date=execution_dates,
        dry_run=True,
    )
    assert len(jobs) == len(execution_dates)
