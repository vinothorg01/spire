import uuid

from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from . import ClusterStatus


class ScoringStatus(ClusterStatus):
    __tablename__ = "scoring_status"
    cluster_id = Column(String(100), nullable=True)
    run_id = Column(Integer(), nullable=True)
    id = Column(
        UUID(as_uuid=True),
        ForeignKey(ClusterStatus.__table__.c.id, ondelete="CASCADE"),
        primary_key=True,
    )

    def __init__(self, cluster_id=None, run_id=None):
        self.id = uuid.uuid4()
        self._set_attributes(cluster_id, run_id)

    __mapper_args__ = {"polymorphic_identity": "scoring_status"}
