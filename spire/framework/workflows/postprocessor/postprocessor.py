import uuid
from datetime import date
from pyspark.sql import DataFrame
from sqlalchemy import String, Column, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm.session import Session
from spire.framework.workflows.connectors import Base
from sqlalchemy.orm import relationship


class Postprocessor(Base):
    __tablename__ = "postprocess"
    workflow_id = Column(
        UUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE")
    )
    id = Column(UUID(as_uuid=True), unique=True, primary_key=True)
    type = Column(String(100))
    workflow = relationship("Workflow", back_populates="postprocessor")

    __mapper_args__ = {
        "polymorphic_identity": "postprocessor",
        "polymorphic_on": type,
        "with_polymorphic": "*",
    }

    def __init__(self):
        self.id = uuid.uuid4()

    def postprocess(self, score: DataFrame, date: date, session: Session):
        raise NotImplementedError(
            "Method postprocess of abstract class {} does not exist.".format(
                self.__class__.__name__
            )
        )

    def to_dict(self, df):
        output = {}
        output["postprocessor_id"] = self.id
        output["type"] = self.__mapper_args__
        output["workflow_id"] = self.workflow_id
        return output

    def __eq__(self, o: object) -> bool:
        return o.id == self.id
