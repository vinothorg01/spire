from typing import Dict, Any, Optional
from pydantic import Field
import mlflow
from mlflow.entities import RunStatus
from mlflow.projects.databricks import DatabricksSubmittedRun
from spire.framework.execution.base import Job, JobRunner, JobStatus, JobStatuses
from spire.utils.logger import get_logger
from spire.config import config


logger = get_logger(__name__)


class MLFlowProjectRunJob(Job):
    """
    A mapping to the config of
    https://www.mlflow.org/docs/latest/python_api/mlflow.projects.html#mlflow.projects.run
    """

    uri: str
    entry_point: str
    experiment_id: str
    parameters: Dict[str, Any] = Field(default_factory=lambda: {})

    version: Optional[str]
    experiment_name: Optional[str]
    backend_config: Optional[Dict[str, Any]]
    docker_args: Optional[Dict[str, Any]]
    backend: str = Field(default="databricks")
    use_conda: bool = Field(default=True)
    synchronous: bool = Field(default=False)
    run_id: Optional[str]
    storage_dir: Optional[str]


class MLFlowProjectRunJobRunner(JobRunner):
    def _inject_gchr_password_into_uri(self) -> None:
        # Copy job before injecting password to keep original job intact
        self.compiled_job = self.job.copy(deep=True)
        self.compiled_job.uri = self.compiled_job.uri.replace(
            "https://", f"https://{config.GHCR_PASSWORD}@"
        )

    def run(self) -> JobStatus:

        self._inject_gchr_password_into_uri()
        # Use compiled_job because it has the password injected into the uri
        submitted_run = mlflow.projects.run(**self.compiled_job.dict())
        status = MLFlowProjectRunJobStatus(submitted_run, self.compiled_job)
        return status


class MLFlowProjectRunJobStatus(JobStatus):
    # https://github.com/mlflow/mlflow/blob/v1.14.0/mlflow/entities/run_status.py#L4
    _STATE_MAP = {
        RunStatus.to_string(RunStatus.SCHEDULED): JobStatuses.PENDING,
        RunStatus.to_string(RunStatus.RUNNING): JobStatuses.RUNNING,
        RunStatus.to_string(RunStatus.KILLED): JobStatuses.FAILED,
        RunStatus.to_string(RunStatus.FAILED): JobStatuses.FAILED,
        RunStatus.to_string(RunStatus.FINISHED): JobStatuses.SUCCESS,
    }

    def __init__(
        self, submitted_run: mlflow.projects.SubmittedRun, job: MLFlowProjectRunJob
    ) -> None:
        super().__init__(job)
        self.submitted_run = submitted_run

    def _get_status(self) -> JobStatuses:
        run_status = self.submitted_run.get_status()
        return self._STATE_MAP[run_status]

    def __str__(self):
        url = None
        if isinstance(self.submitted_run, DatabricksSubmittedRun):
            sr = self.submitted_run
            # Using the private method of mlflow to get the url
            # https://github.com/mlflow/mlflow/blob/feff8f9c411d641452ad8c58aa501d095613059a/mlflow/projects/databricks.py#L380
            run_info = sr._job_runner.jobs_runs_get(sr._databricks_run_id)
            url = run_info["run_page_url"]

        return f"{self.submitted_run.run_id}: {self.latest_status} - {url}"
