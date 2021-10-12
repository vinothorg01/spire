from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Integer, Column, ForeignKey
from spire.framework.workflows.connectors import Base


class Trait(Base):
    __tablename__ = "trait_workflows"
    workflow_id = Column(
        UUID(as_uuid=True),
        ForeignKey("workflows.id", ondelete="CASCADE"),
        primary_key=True,
    )
    trait_id = Column(Integer(), nullable=True, unique=True)
    workflow = relationship("Workflow", uselist=False, back_populates="trait")
