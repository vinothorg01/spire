import datetime
import copy
from typing import Dict, Any, List, Type

from pydantic import BaseModel
from sqlalchemy import Column, ForeignKey, String, Enum, DateTime, JSON
from sqlalchemy.orm import relationship, backref, validates
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.dialects.postgresql import UUID

from spire.utils.logger import get_logger
from spire.framework.workflows.connectors import Base
from spire.framework.workflows.workflow_stages import WorkflowStages
from spire.framework.workflows import workflow
from spire.framework.execution import Job


logger = get_logger()


class JobContext(BaseModel):
    """A Pydantic model that defines the runtime argument for a Job, each
    argument should be a pydantic Field with an alias that is a dotted path
    mapping to the actual path the value should live in the JobConfig's
    definition.

    e.g. A context for FooConfig with 1 argument called `bar` that should be
    set at "arguments.greeting_message" in the FooConfig's definition with the
    default value of "Hello World" would look like

    class FooContext(JobContext):
        bar: str = Field(
            default_factory=lambda: "Hello World",
            alias="arguments.greeting_message"
        )
    """

    class Config:
        allow_population_by_field_name = True

    @staticmethod
    def set_path(data: Dict[Any, Any], path: str, value: Any):
        """Set value at the path in data
        E.g.
            Inputs: data = {}, path = "a.b", value = 1
            Outputs: None
            The data will become { "a": { "b" : 1 } }
        """
        path_components = path.split(".")
        obj = data
        for comp in path_components[:-1]:
            if comp not in obj:
                obj[comp] = {}
            obj = obj[comp]
        obj[path_components[-1]] = value

    def inject(self, defaults: Dict[str, Any]) -> Dict[str, Any]:
        """Add current context's attributes to defaults at path specified
        in the alias field of the attribute
        """
        output = copy.deepcopy(defaults)
        for path, value in self.dict(by_alias=True).items():
            # Only inject arguments with value that is not None
            if value is not None:
                self.__class__.set_path(output, path, value)
        return output


class JobConfig(Base):
    """A table that stores a partially filled Job and knows how to provide
    additional context to complete that job definition.
    """

    __tablename__ = "job_configs"
    name = Column(String, primary_key=True)
    type = Column(String, nullable=False)
    definition = Column(JSON, nullable=False)
    created_at = Column(DateTime(), default=datetime.datetime.now)
    modified_at = Column(
        DateTime(), default=datetime.datetime.now, onupdate=datetime.datetime.now
    )

    workflows = relationship("WorkflowJobConfig")  # , back_populates="job_config"
    workflows = association_proxy("jobconfig_workflows", "workflow")

    __mapper_args__ = {"polymorphic_identity": "JobConfig", "polymorphic_on": type}

    def get_jobs(
        self, workflows: List["workflow.Workflow"], run_date: datetime.date
    ) -> List[Job]:  # noqa
        """Create Jobs for the given workflows, note that this doesn't have
        to be a one to one mapping
        """
        raise NotImplementedError()

    @staticmethod
    def get_context_class() -> Type[JobContext]:
        """Define the context required for this context i.e. the parameters"""
        raise NotImplementedError()

    @staticmethod
    def get_job_class() -> Type[Job]:
        """Defines the job class associated with this config"""
        raise NotImplementedError()

    def validate_definition(self, definition):
        """Defines what should be in the definition"""
        raise NotImplementedError()

    @property
    def Job(self):
        return self.get_job_class()

    @property
    def Context(self):
        return self.get_context_class()

    @validates("definition")
    def _validate_definition(self, _, definition):
        return self.validate_definition(definition)


class WorkflowJobConfig(Base):
    """See https://docs.sqlalchemy.org/en/14/orm/extensions/associationproxy.html#proxying-to-dictionary-based-collections # noqa
    for a reference of how this was setup.
    """

    __tablename__ = "workflow_jobconfigs"

    workflow_id = Column(
        UUID(as_uuid=True), ForeignKey("workflows.id"), primary_key=True
    )
    job_config_name = Column(String, ForeignKey("job_configs.name"), primary_key=True)

    stage = Column(
        Enum(
            WorkflowStages,
            # set native_enum=False so that we can add Stages without having to
            # update the Type in Postgres
            native_enum=False,
        ),
        primary_key=True,
    )

    job_config = relationship(
        "JobConfig", backref=backref("jobconfig_workflows", cascade=False)
    )
    workflow = relationship(
        "Workflow",
        backref=backref(
            "workflow_jobconfigs",
            collection_class=attribute_mapped_collection("stage"),
            cascade="all, delete-orphan",
        ),
    )
