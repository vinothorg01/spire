from typing import List, Dict, Optional, Any
import requests
from pydantic import BaseModel, root_validator
import json
import copy

from spire.config import config
from spire.utils.databricks import get_databricks_auth_headers
from spire.utils.logger import get_logger
from spire.framework.execution.base import Job, JobRunner, JobStatus, JobStatuses
from spire.framework.execution.implementations.implementation_tasks import (
    ImplementationTasks,
)


logger = get_logger(__name__)


class BasicAuth(BaseModel):
    username: str
    password: str


class DatabricksDockerImageConfig(BaseModel):
    url: str
    basic_auth: BasicAuth


class NotebookTask(BaseModel):
    notebook_path: str
    revision_timestamp: Optional[int]
    base_parameters: Optional[Dict[str, Any]]


class DatabricksRunSubmitJob(Job):
    """
    A mapping to the config of
    https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit
    """

    # TODO: redact secrets in env on __str__, __repl__ etc.
    new_cluster: Dict[str, Any]

    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[Dict[str, Any]]
    spark_python_task: Optional[Dict[str, Any]]
    spark_submit_task: Optional[Dict[str, Any]]

    run_name: str
    libraries: Optional[List[Dict[str, Any]]]
    timeout_seconds: Optional[int]
    idempotency_token: Optional[str]

    @root_validator
    def validate_task_config(cls, values):
        "Check that 1 task is set."
        configs = []
        for task in ImplementationTasks:
            config = values.get(str(task))
            if config:
                configs.append(config)

        assert (
            len(configs) == 1
        ), f"Only 1 task type should have been set, got {len(configs)}"

        return values

    def _get_parameters(self) -> Dict[str, Any]:
        return self._parse_params()

    @property
    def parameters(self):
        return self._get_parameters()

    def _parse_params(self) -> Dict[str, Any]:
        for t in ImplementationTasks:
            task = str(t)
            if not hasattr(self, task):
                continue
            if task == str(ImplementationTasks.NOTEBOOK):
                return self._parse_notebook_params()
            elif task == str(ImplementationTasks.SPARK_PYTHON):
                # TODO(Max): Test if this requires parsing
                return self.spark_python_task.parameters
        else:
            e = """Only notebook_task and spark_python_task are currently supported for
                returning parameters"""
            raise Exception(e)

    def _parse_notebook_params(self) -> Dict[str, Any]:
        # NOTE(Max, 2021-06-21): I'm not sure why notebook_task has 'date'
        # instead of 'run_date' nor what the implications of changing it at a
        # lower level would entail, so I'm doing this for now, but ideally this
        # should be refactored to a lower level or input level, or made
        # irrelevant via a larger refactor of the entire "Job" architecture
        # NOTE(Max, 2021-06-22): Deep copy of self.notebook_task.base_parameters to
        # avoid mutation issues with parsing the dict
        parameters = copy.deepcopy(self.notebook_task.base_parameters)
        if parameters.get("run_date") is None:
            assert "date" in parameters.keys()
            parameters["run_date"] = parameters.pop("date")
        assert "workflow_ids" in parameters.keys()
        if isinstance(parameters["workflow_ids"], str):
            wf_id_list = json.loads(parameters["workflow_ids"])
            parameters["workflow_ids"] = ",".join(wf_id_list)
        return parameters


class DatabricksRunSubmitJobRunner(JobRunner):
    API_PATH = "/api/2.0/jobs/runs/submit"

    def _post_to_databricks(self, job):
        response = requests.post(
            f"{config.DATABRICKS_HOST}{self.API_PATH}",
            headers=get_databricks_auth_headers(),
            data=job.json(),
        )
        if response.ok:
            return response.json()["run_id"]
        else:
            logger.error(response.text)
            raise requests.exceptions.RequestException()

    def run(self) -> JobStatus:
        run_id = self._post_to_databricks(self.job)
        status = DatabricksRunSubmitJobStatus(run_id, self.job)
        return status


class DatabricksRunSubmitJobStatus(JobStatus):
    API_PATH = "/api/2.0/jobs/runs/get?run_id="

    # https://docs.databricks.com/dev-tools/api/latest/jobs.html#runlifecyclestate
    DB_LIFECYCLE_STATE_MAP = {
        "PENDING": JobStatuses.PENDING,
        "RUNNING": JobStatuses.RUNNING,
        "TERMINATING": JobStatuses.SUCCESS,
        "TERMINATED": JobStatuses.SUCCESS,
        "SKIPPED": JobStatuses.FAILED,
        "INTERNAL_ERROR": JobStatuses.FAILED,
    }

    # https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobsrunresultstate
    DB_RESULT_STATE_MAP = {
        "SUCCESS": JobStatuses.SUCCESS,
        "FAILED": JobStatuses.FAILED,
        "TIMEDOUT": JobStatuses.FAILED,
        "CANCELED": JobStatuses.FAILED,
    }

    def __init__(self, run_id, job) -> None:
        super().__init__(job)
        self.run_id = run_id
        self._data = None

    def _get_status(self) -> JobStatuses:
        resp = requests.get(
            f"{config.DATABRICKS_HOST}{self.API_PATH}{self.run_id}",
            headers=get_databricks_auth_headers(),
        )
        if resp.ok:
            self._data = resp.json()
            if "result_state" in self._data["state"]:
                # This will be available when the job terminated and it should
                # take precendence since it tells us whether the job ran
                # successfull or not.
                result_state = self._data["state"]["result_state"]
                return self.DB_RESULT_STATE_MAP[result_state]
            else:
                # This is available before the job terminated
                life_cycle_state = self._data["state"]["life_cycle_state"]
                return self.DB_LIFECYCLE_STATE_MAP[life_cycle_state]
        else:
            raise requests.exceptions.RequestException()

    def __str__(self):
        url = None
        if self._data:
            url = self._data["run_page_url"]

        return f"{self.run_id}: {self.latest_status} - {url}"
