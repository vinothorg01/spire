"""
The `execution` module implements different runtime for a Spire job.

The 3 fundamental classes in this module are the Job, JobRunner, and JobStatus.

    A `Job` is the specification for that job, meaning it contains everything you
        need to perform that task.

    A `JobRunner` knows how to run a corresponding job type.

    A `JobStatus` knows how to get information about that job once it was launched.

Note that these 3 classes are Abstract Base Classes which will be inherited for
each runtime that we want to support.

Basic Usage:

1. Instantiate a concrete Job implemented for the runtime the task will be run
    on. e.g. DatabricksRunSumitJob
2. Instantiate the runner object by calling JobRunner.get_runner_for_job(job)
3. Run the job by calling runner.run() (returns a JobStatus)
4. Keep track of the job by calling JobStatus.get_status()

"""
from .base import JobStatuses, JobStatus, JobRunner, Job
from .implementations import (
    DatabricksRunSubmitJob,
    DatabricksRunSubmitJobRunner,
    DatabricksRunSubmitJobStatus,
    MLFlowProjectRunJobRunner,
    MLFlowProjectRunJob,
    MLFlowProjectRunJobStatus,
)
from .implementations.databricks_run_submit import NotebookTask

__all__ = [
    "JobStatuses",
    "JobStatus",
    "JobRunner",
    "Job",
    "NotebookTask",
    "DatabricksRunSubmitJob",
    "DatabricksRunSubmitJobRunner",
    "DatabricksRunSubmitJobStatus",
    "MLFlowProjectRunJobRunner",
    "MLFlowProjectRunJob",
    "MLFlowProjectRunJobStatus",
]
