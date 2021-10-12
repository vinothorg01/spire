from datetime import date, datetime
import json
import copy
from typing import Dict, Type, List, Optional, Any
from pydantic import Field, validator

from spire.utils.logger import get_logger
from spire.config import config
from spire.framework.execution.implementations.databricks_run_submit import (
    DatabricksRunSubmitJob,
    DatabricksDockerImageConfig,
)
from spire.framework.workflows import workflow
from spire.framework.workflows.job_config.base import JobConfig, JobContext, Job
from spire.framework import constants


logger = get_logger(__name__)


class OneNotebookJobContext(JobContext):
    run_name: str = Field(default_factory=lambda: f"one_notebook_{datetime.now()}")
    run_date: str = Field(
        default_factory=lambda: str(date.today()),
        alias="notebook_task.base_parameters.args.run_date",
    )
    workflow_ids: List[str] = Field(
        alias="notebook_task.base_parameters.args.workflow_ids", default=None
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

    @validator("run_date", pre=True)
    def convert_to_date_string(cls, date):
        if isinstance(date, str):
            return date
        return date.strftime("%Y-%m-%d")

    def inject(self, defaults: Dict[str, Any]) -> Dict[str, Any]:
        # make sure args was deserialized before injecting, otherwise we can't
        # inject
        defaults = copy.deepcopy(defaults)
        args = defaults["notebook_task"]["base_parameters"]["args"]
        defaults["notebook_task"]["base_parameters"]["args"] = json.loads(args)
        injected = super().inject(defaults)
        # serialize args
        args = injected["notebook_task"]["base_parameters"]["args"]
        injected["notebook_task"]["base_parameters"]["args"] = json.dumps(args)
        return injected


class OneNotebookJobConfig(JobConfig):
    NOTEBOOK_PATH = "/Repos/Spire/spire-notebooks/the_notebook"
    __mapper_args__ = {"polymorphic_identity": "OneNotebookJobConfig"}

    def __init__(
        self,
        target_module,
        target_method,
        new_cluster,
        args: Optional[Dict] = None,
        **kwargs,
    ) -> None:

        if args is None:
            args = {}
        # reorganize the arguments into DatabricksRunSubmitJob format
        definition = {
            "new_cluster": new_cluster,
            "notebook_task": {
                "notebook_path": self.NOTEBOOK_PATH,
                "base_parameters": {
                    "target_module": target_module,
                    "target_method": target_method,
                    "args": json.dumps(args),
                },
            },
        }
        self.definition = definition
        super().__init__(**kwargs)

    @staticmethod
    def get_context_class() -> Type[JobContext]:
        return OneNotebookJobContext

    @staticmethod
    def get_job_class() -> Type[Job]:
        return DatabricksRunSubmitJob

    def validate_definition(self, definition):
        injected_def = self.Context().inject(definition)
        params = injected_def["notebook_task"]["base_parameters"]

        if "target_module" not in params or params["target_module"] is None:
            raise ValueError("target_module should be set.")

        if "target_method" not in params or params["target_method"] is None:
            raise ValueError("target_method should be set.")

        assert injected_def != definition
        # make sure the definition works with the context

        return definition

    def get_jobs(
        self, workflows: List["workflow.Workflow"] = [], run_date: date = None
    ) -> List[Job]:
        if run_date is None:
            run_date = date.today()
        workflow_ids = [str(w.id) for w in workflows]
        print("before injecting", self.definition)
        job_def = self.Context(workflow_ids=workflow_ids, run_date=run_date).inject(
            self.definition
        )
        job = self.Job(**job_def)
        return [job]
