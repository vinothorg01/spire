import pytest
from spire.framework.execution import (
    Job,
    JobRunner,
    JobStatus,
    DatabricksRunSubmitJob,
    JobStatuses,
)
from spire.framework.execution.implementations.databricks_run_submit import (
    DatabricksRunSubmitJobRunner,
)
from tests.data.cluster_config import generic_config as sample_config


def test_get_runner_for_job_return_correct_runner():
    job = DatabricksRunSubmitJob(**sample_config)
    runner = JobRunner.get_runner_for_job(job)
    assert isinstance(runner, DatabricksRunSubmitJobRunner)


def test_cannot_find_runner():
    class UnknownJob(Job):
        id: int

    job = UnknownJob(id=1)

    with pytest.raises(Exception):
        JobRunner.get_runner_for_job(job)


class DummyJob(Job):
    id: int
    status: JobStatuses


class DummyJobStatus(JobStatus):
    def _get_status(self) -> JobStatuses:
        return self.job.status


class DummyJobRunner(JobRunner):
    def run(self) -> JobStatus:
        return DummyJobStatus(self.job)
