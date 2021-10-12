from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import String, Column
from sqlalchemy.schema import UniqueConstraint
import uuid

from spire.framework.workflows.connectors import workflow_tags, Base


class Tag(Base):
    __tablename__ = "tags"
    id = Column(UUID(as_uuid=True), primary_key=True, unique=True)
    label = Column(String(100), nullable=False)
    workflows = relationship("Workflow", secondary=workflow_tags, back_populates="tags")
    __table_args__ = (UniqueConstraint("label"),)

    def __repr__(self):
        return "<Tag(label={})".format(self.label)

    def __init__(self, label):
        self.id = uuid.uuid4()
        self.label = label

    @classmethod
    def create_tag(cls, session, tag_label):
        try:
            tag = cls(label=tag_label)
            session.add(tag)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def add_workflow(self, workflow):
        self.workflows.append(workflow)

    def remove_workflow(self, workflow):
        self.workflows.remove(workflow)

    def modify_label(self, label):
        self.label = label
