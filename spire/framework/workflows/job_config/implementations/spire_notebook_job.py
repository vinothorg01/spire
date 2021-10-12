import datetime
import json
from typing import List, Type, Dict, Optional, Any
from pydantic import Field, validator

from spire.config import config
from spire.framework import constants
from spire.framework.execution import DatabricksRunSubmitJob, Job
from spire.framework.execution.implementations.databricks_run_submit import (
    DatabricksDockerImageConfig,
)
from spire.framework.workflows import workflow
from spire.utils.logger import get_logger
from spire.utils.general import chunk_into_groups

from ..base import JobContext, JobConfig

logger = get_logger()


class SpireNotebookJobContext(JobContext):
    workflow_ids: str = Field(alias="notebook_task.base_parameters.workflow_ids")

    date: str = Field(
        default_factory=datetime.date.today,
        alias="notebook_task.base_parameters.date",
    )
    threadpool_group_size: int = Field(
        default_factory=lambda: config.THREADPOOL_GROUP_SIZE,
        alias="notebook_task.base_parameters.threadpool_group_size",
    )
    run_name: str = Field(
        default_factory=lambda: f"run_{datetime.datetime.now().isoformat()}",
        alias="run_name",
    )

    docker_image: Optional[DatabricksDockerImageConfig] = Field(
        alias="new_cluster.docker_image",
        default_factory=lambda: constants.DOCKER_IMAGE_CONFIG,
    )
    envs: Dict[str, Any] = Field(
        alias="new_cluster.spark_env_vars",
        default_factory=lambda: {
            "VAULT_TOKEN": config.VAULT_TOKEN,
            "DB_HOSTNAME": config.DB_HOSTNAME,
            "DEPLOYMENT_ENV": config.DEPLOYMENT_ENV,
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
            "VAULT_ADDRESS": config.VAULT_ADDRESS,
        },
    )

    @validator("workflow_ids", pre=True)
    def convert_to_json_list(cls, workflow_ids):
        if isinstance(workflow_ids, str):
            return workflow_ids
        return json.dumps(workflow_ids)

    @validator("date", pre=True)
    def convert_to_date_string(cls, date):
        if isinstance(date, str):
            return date
        return date.strftime("%Y-%m-%d")


class SpireNotebookJobConfig(JobConfig):
    __mapper_args__ = {"polymorphic_identity": "SpireNotebookJobConfig"}
    MAX_WF_PER_CLUSTER = 50

    def group_workflows(self, workflows) -> List["Workflow"]:  # noqa
        return chunk_into_groups(workflows, self.MAX_WF_PER_CLUSTER)

    @staticmethod
    def get_context_class() -> Type[JobContext]:
        return SpireNotebookJobContext

    @staticmethod
    def get_job_class() -> Type[Job]:
        return DatabricksRunSubmitJob

    def validate_definition(self, definition):
        # TODO: validation definition
        return definition

    def get_jobs(
        self,
        workflows: List["workflow.Workflow"],
        run_date: datetime.date = datetime.date.today(),
    ) -> List[Job]:
        groups = self.group_workflows(workflows)
        jobs = []
        for workflows in groups:
            workflow_ids = [str(w.id) for w in workflows]
            context = self.get_context_class()(workflow_ids=workflow_ids, date=run_date)
            job_data = context.inject(self.definition)
            job = self.get_job_class()(**job_data)
            jobs.append(job)
        return jobs


class SpireScoringNotebookJobConfig(SpireNotebookJobConfig):
    __mapper_args__ = {"polymorphic_identity": "SpireScoringNotebookJobConfig"}
    MAX_WF_PER_CLUSTER = 50


class SpireTrainingNotebookJobConfig(SpireNotebookJobConfig):
    __mapper_args__ = {"polymorphic_identity": "SpireTrainingNotebookJobConfig"}
    MAX_WF_PER_CLUSTER = 50
