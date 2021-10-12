from sqlalchemy import (
    Table,
    Column,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

workflow_features = Table(
    "workflow_features",
    Base.metadata,
    Column(
        "workflow_id",
        UUID(as_uuid=True),
        ForeignKey("workflows.id", ondelete="CASCADE"),
    ),
    Column(
        "features_id", UUID(as_uuid=True), ForeignKey("features.id", ondelete="CASCADE")
    ),
)

workflow_datasets = Table(
    "workflow_datasets",
    Base.metadata,
    Column(
        "workflow_id",
        UUID(as_uuid=True),
        ForeignKey("workflows.id", ondelete="CASCADE"),
    ),
    Column(
        "dataset_id", UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE")
    ),
)

workflow_tags = Table(
    "workflow_tags",
    Base.metadata,
    Column(
        "workflow_id",
        UUID(as_uuid=True),
        ForeignKey("workflows.id", ondelete="CASCADE"),
    ),
    Column("tag_id", UUID(as_uuid=True), ForeignKey("tags.id", ondelete="CASCADE")),
)
