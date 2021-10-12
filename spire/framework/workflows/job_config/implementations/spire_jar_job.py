import datetime
from typing import List, Type
from pydantic import Field

from spire.framework.execution import DatabricksRunSubmitJob, Job
from spire.utils.logger import get_logger
from ..base import JobContext, JobConfig

logger = get_logger()


class SpireJarJobContext(JobContext):
    run_name: str = Field(
        default_factory=lambda: f"run_{datetime.datetime.now().isoformat()}",
        alias="run_name",
    )


class SpireJarJobConfig(JobConfig):
    __mapper_args__ = {"polymorphic_identity": "SpireJarJobConfig"}

    @staticmethod
    def get_context_class() -> Type[JobContext]:
        return SpireJarJobContext

    def validate_definition(self, definition):
        # TODO: validation definition
        return definition

    @staticmethod
    def get_job_class() -> Type[Job]:
        return DatabricksRunSubmitJob

    def get_jobs(self, workflows: List["Workflow"]) -> List[Job]:  # noqa
        ContextClass = self.get_context_class()
        context = ContextClass()
        job_data = context.inject(self.definition)
        job = self.get_job_class()(**job_data)
        return [job]
