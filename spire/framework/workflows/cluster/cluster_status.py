from sqlalchemy.orm import relationship
from spire.framework.workflows.connectors import Base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Column, String, ForeignKey


class ClusterStatus(Base):
    __tablename__ = "cluster_status"
    workflow_id = Column(
        UUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE")
    )
    workflow = relationship("Workflow", uselist=False, back_populates="cluster_status")
    stage = Column(String(100))
    id = Column(UUID(as_uuid=True), unique=True, primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "clusterstatus",
        "polymorphic_on": stage,
        "with_polymorphic": "*",
    }

    def __init__(self):
        raise NotImplementedError

    def update(self, cluster_id, run_id):
        self.cluster_id = cluster_id
        self.run_id = run_id

    def to_dict(self):
        output = {}
        output["stage"] = self.stage
        output["clusterstatus_object_id"] = self.id
        output["cluster_id"] = self.cluster_id
        output["run_id"] = self.run_id
        return output

    def _set_attributes(self, cluster_id=None, run_id=None):
        self.cluster_id = cluster_id
        self.run_id = run_id
