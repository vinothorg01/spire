import click
import datetime
from typing import List
from sqlalchemy.orm.session import Session
from multiprocessing.pool import ThreadPool
from spire.framework.workflows.workflow import WorkflowStages
from spire.framework.execution import JobRunner, JobStatus, Job
from spire.framework.workflows import Workflow
from spire.integrations.postgres import connector
from spire.utils import get_logger


logger = get_logger(__name__)

# TODO: Make this configurable
THREAD_POOL_SIZE = 5


def _run_jobs(jobs: List[Job]):
    """Run and monitor job."""
    statuses = []
    for job in jobs:
        logger.info(f"Running job = {job}")
        runner = JobRunner.get_runner_for_job(job)
        job_status = runner.run()
        logger.info(f"{job_status}")
        statuses.append(job_status)
    JobStatus.monitor_statuses(statuses)


@connector.session_transaction
def _build_jobs(
    stage: WorkflowStages,
    run_date: datetime.date,
    wf_ids: List[str] = None,
    session: Session = None,
):
    """
    From the list of wf_ids or wf_ids of ready workflows, gets and returns the job
    """
    if not wf_ids:
        jobs = Workflow.get_scheduled_jobs(run_date=run_date, stage=stage)
    else:
        wfs = Workflow.get_by_ids(session, wf_ids)
        jobs = Workflow.get_jobs_for_workflows(wfs, stage, run_date)
    return jobs


def run_stage(
    stage: WorkflowStages,
    run_dates: List[datetime.date],
    wf_ids: List[str] = None,
    dry_run: bool = False,
):
    """Interface for manually launching clusters to run a stage."""

    # In case a single wf id is entered
    if wf_ids is not None and not isinstance(wf_ids, list):
        wf_ids = [wf_ids]

    # In case a single date is entered (eg: from Airflow)
    if not isinstance(run_dates, list):
        run_dates = [run_dates]

    # Build all jobs
    jobs = []
    for date in run_dates:
        jobs.append(_build_jobs(stage, date, wf_ids))

    # Case handling
    if len(jobs) == 0:
        logger.info("Nothing to run.")

    # Run multiple jobs if applicable
    if not dry_run:
        logger.info(f"Running jobs {jobs}")
        pool = ThreadPool(THREAD_POOL_SIZE)
        pool.map(lambda job: _run_jobs(job), jobs)
    return jobs


@click.command()
@click.option("--stage")
@click.option("--run_date")
@click.option("--dry_run", is_flag=True)
def click_main(stage=None, run_date=None, dry_run=False):
    stage = WorkflowStages(stage)
    run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
    run_stage(stage, run_date, dry_run)


if __name__ == "__main__":
    click_main()
