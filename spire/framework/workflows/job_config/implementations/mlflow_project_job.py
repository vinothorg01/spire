import datetime
from typing import Dict, Optional, Any, List
from pydantic import Field, validator
from pydantic.main import BaseModel

from spire.config import config
from spire.utils.logger import get_logger
from spire.framework.execution import MLFlowProjectRunJob, Job
from ..base import JobContext, JobConfig
from spire.framework.score import constants as scoring_constants
from spire.framework.assemble import constants as assembly_constants
from spire.framework.workflows import workflow

logger = get_logger()


class MLFlowProjectJobContext(JobContext):
    input_uri: str = Field(alias="parameters.input_uri")
    output_uri: Optional[str] = Field(alias="parameters.output_uri")
    run_date: str = Field(alias="parameters.run_date")

    envs: Dict[str, Any] = Field(
        alias="backend_config.spark_env_vars",
        default_factory=lambda: {
            "VAULT_TOKEN": config.VAULT_TOKEN,
            "DB_HOSTNAME": config.DB_HOSTNAME,
            "DEPLOYMENT_ENV": config.DEPLOYMENT_ENV,
            "VAULT_ADDRESS": config.VAULT_ADDRESS,
        },
    )

    @validator("run_date", pre=True)
    def convert_date(cls, run_date):
        if isinstance(run_date, datetime.date):
            run_date = str(run_date)
        return run_date


class MLFlowProjectJobDefinition(BaseModel):
    # A https github URI with target subdirectory appended after the pound sign
    # e.g. https://github.com/CondeNast/spire-models#models/ncs_pdm
    uri: str
    entry_point: str
    version: Optional[str]
    parameters: Dict[str, Any] = Field(default_factory=lambda: {})
    experiment_id: str
    backend_config: Optional[Dict[str, Any]]


class _MLFlowProjectJobConfig(JobConfig):
    @staticmethod
    def get_context_class() -> MLFlowProjectJobContext:
        return MLFlowProjectJobContext

    @staticmethod
    def get_job_class() -> MLFlowProjectRunJob:
        return MLFlowProjectRunJob

    def validate_definition(self, definition):
        if isinstance(definition, MLFlowProjectJobDefinition):
            return definition.dict()
        MLFlowProjectJobDefinition.validate(**definition)
        return definition

    def get_input_uri(
        self, workflow: "workflow.Workflow", run_date: datetime.date
    ) -> str:
        raise NotImplementedError()

    def get_output_uri(
        self, workflow: "workflow.Workflow", run_date: datetime.date
    ) -> Optional[str]:
        return None

    def get_jobs(
        self,
        workflows: List["workflow.Workflow"],
        run_date: datetime.date = datetime.date.today(),
    ) -> List[Job]:  # noqa: F821
        jobs = []
        for wf in workflows:
            input_uri = self.get_input_uri(wf, run_date)
            output_uri = self.get_output_uri(wf, run_date)
            context = self.Context(
                input_uri=input_uri, output_uri=output_uri, run_date=run_date
            )
            job_data = context.inject(self.definition)
            job = self.Job(**job_data)
            jobs.append(job)
        return jobs


class MLFlowProjectAssemblyJobConfig(_MLFlowProjectJobConfig):
    __mapper_args__ = {"polymorphic_identity": "MLFlowProjectAssemblyJobConfig"}

    def get_input_uri(
        self, workflow: "workflow.Workflow", context: MLFlowProjectJobContext
    ) -> str:
        return (
            f"s3a://{scoring_constants.FEATURE_BUCKET}/{scoring_constants.FEATURE_KEY},"
            f"s3a://{config.SPIRE_ENVIRON}/{assembly_constants.IN_TARGET_KEY}"
        )

    def get_output_uri(
        self,
        workflow: "workflow.Workflow",
        context: MLFlowProjectJobContext,
    ) -> str:
        return (
            f"s3a://{config.SPIRE_ENVIRON}/{assembly_constants.OUT_KEY}"
            f"/workflow_id={workflow.id}"
        )


class MLFlowProjectTrainingJobConfig(_MLFlowProjectJobConfig):
    __mapper_args__ = {"polymorphic_identity": "MLFlowProjectTrainingJobConfig"}

    def get_input_uri(
        self, workflow: "workflow.Workflow", context: MLFlowProjectJobContext
    ) -> str:
        return (
            f"{config.SPIRE_ENVIRON}/datasets/training/temp/"
            f"workflow_id={workflow.id}"
        )


class MLFlowProjectScoringJobConfig(_MLFlowProjectJobConfig):
    __mapper_args__ = {"polymorphic_identity": "MLFlowProjectScoringJobConfig"}

    def get_input_uri(
        self, workflow: "workflow.Workflow", run_date: datetime.date
    ) -> str:
        return (
            f"s3a://{scoring_constants.FEATURE_BUCKET}/"
            f"{scoring_constants.FEATURE_KEY}"
        )

    def get_output_uri(
        self, workflow: "workflow.Workflow", run_date: datetime.date
    ) -> str:
        return (
            f"{scoring_constants.MLFLOW_INTERMEDIATE_OUTPUT_URI}"
            f"/config={self.name}/wid={workflow.id}/date={run_date}"
        )
