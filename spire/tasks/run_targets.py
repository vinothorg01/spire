from typing import Dict, Any, List
from multiprocessing.pool import ThreadPool
from spire.framework.execution import JobRunner, JobStatus, Job
from spire.framework.workflows.job_config import (
    OneNotebookJobConfig,
)

# TODO: Make this configurable here and via CLI
THREAD_POOL_SIZE = 5


def _run_jobs(job: List[Job]):
    """Run and monitor jobs."""

    job_status = JobRunner.get_runner_for_job(job).run()
    JobStatus.monitor_statuses([job_status])


def _build_jobs(date: str, vendor: str, source: str, task_config: Dict[str, Any]):
    """Build a target job config."""

    job_config = OneNotebookJobConfig(
        target_module="spire.tasks.execute_targets",
        target_method="main",
        new_cluster=task_config["cluster_configuration"],
        args={
            "vendor": vendor,
            "source": source,
            "execution_date": date,
        },
    )
    job = job_config.get_jobs()[0]
    return job


def run_targets(
    vendor: str,
    source: str,
    task_config: Dict[str, Any],
    dry_run: bool = False,
    **context
):
    """
    Interface for manually launching clusters to run a given target task.
    """

    execution_dates = context.pop("execution_date")
    if not isinstance(execution_dates, list):
        execution_dates = [execution_dates]

    execution_dates = [date.strftime("%Y-%m-%d") for date in execution_dates]

    # Build jobs
    jobs = []
    for date in execution_dates:
        jobs.append(_build_jobs(date, vendor, source, task_config))

    # Process multiple jobs at the same time
    if not dry_run:
        pool = ThreadPool(THREAD_POOL_SIZE)
        pool.map(lambda job: _run_jobs(job), jobs)
    return jobs
