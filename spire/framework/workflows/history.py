import uuid
import datetime
from typing import Optional
from sqlalchemy.sql import and_
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import JSON
from sqlalchemy import String, Column, DateTime, ForeignKey, Date
from spire.framework.workflows.connectors import Base
from spire.utils.logger import get_logger


logger = get_logger()


class History(Base):
    __tablename__ = "history"
    id = Column(UUID(as_uuid=True), primary_key=True, default=lambda: uuid.uuid4())
    workflow_id = Column(
        UUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE")
    )

    # TODO: Change to WorkflowStages Enum
    stage = Column(String(100), nullable=False)
    # TODO: Change to JobStatuses Enum
    status = Column(String(100), nullable=False)
    arg_date = Column(Date(), nullable=False, default=lambda: datetime.date.today())

    # TODO: Change to execution_datetime
    execution_date = Column(
        DateTime(), nullable=True, default=lambda: datetime.datetime.now()
    )
    info = Column(JSON, nullable=False, default=lambda: {})
    stats = Column(JSON, nullable=False, default=lambda: {})
    warnings = Column(JSON, nullable=False, default=lambda: {})
    error = Column(String(5000), nullable=True)
    traceback = Column(String(5000), nullable=True)
    workflow = relationship("Workflow", uselist=False, back_populates="history")

    @classmethod
    def get_by_attrs(cls, session, **attrs) -> Optional["History"]:
        """
        Get the first history with the attributes given

        e.g.
        History.get_by_attrs(
            session,
            workflow_id="abc",
            arg_date=today,
            stage=WorkflowStages.ASSEMBLY
        )
        """
        expressions = []
        for key, value in attrs.items():
            if not hasattr(cls, key):
                raise ValueError(f"Unknown attribute `{key}` for class {cls.__name__}")
            expressions.append(getattr(cls, key) == value)
        criteria = and_(True, *expressions)

        return session.query(cls).filter(criteria).first()

    @classmethod
    def get_last_successful_train(cls, session, **attrs) -> Optional["History"]:
        """
        TODO: Consider making one function for get_by_attrs and
        get_last_successful_train
        Out of all models trained for a workflow, get the latest model.
        attrs = (session,
                 workflow_id=workflow.id,
                 arg_date=as_of,
                 stage=WorkflowStages.TRAINING,
                 status="success")
        """
        expressions = []
        for key, value in attrs.items():
            if not hasattr(cls, key):
                raise ValueError(f"Unknown attribute `{key}` for class {cls.__name__}")
            if key == "arg_date":
                # Look for models trained on or before the arg_date
                expressions.append(getattr(cls, key) <= value)
            else:
                expressions.append(getattr(cls, key) == value)
        criteria = and_(True, *expressions)

        return session.query(cls).filter(criteria).order_by(cls.arg_date.desc()).first()

    def __repr__(self):
        if self.workflow.trait is not None:
            trait = self.workflow.trait.trait_id
        else:
            trait = None
        return (
            "<History(workflow_id='{}', trait_id={}, stage='{}', "
            "status='{}', arg_date='{}', execution_date='{}')>".format(
                self.workflow_id,
                trait,
                self.stage,
                self.status,
                self.arg_date,
                self.execution_date,
            )
        )

    def to_dict(self):
        output = {
            "workflow_id": self.workflow_id,
            "name": self.workflow.name,
            "ui_name": self.workflow.description,
            "stage": self.stage,
            "arg_date": self.arg_date,
            "execution_date": self.execution_date,
            "status": self.status,
            "info": self.info,
            "stats": self.stats,
            "warnings": self.warnings,
            "error": self.error,
            "traceback": self.traceback,
        }
        if self.workflow.trait:
            output["trait_id"] = self.workflow.trait.trait_id
        return output
