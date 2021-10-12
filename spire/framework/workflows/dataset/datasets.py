import uuid

from spire.framework.workflows.connectors import Base, workflow_datasets

from sqlalchemy.orm import relationship
from sqlalchemy import orm

from sqlalchemy import Column, JSON
from sqlalchemy.dialects.postgresql import UUID


class Dataset(Base):
    """
    The abstract class for e.g. BinaryClassesDataset, MultiClassesDataset, or in theory
    non-Classification Dataset types

    For a higher level explanation, see `spire.framework.workflows.dataset.__init__.py`
    """

    __tablename__ = "datasets"
    id = Column(UUID(as_uuid=True), primary_key=True)
    definition = Column(JSON, nullable=True)
    workflow = relationship(
        "Workflow",
        viewonly=True,
        secondary=workflow_datasets,
        backref="workflow_datasets",
        uselist=False,
        single_parent=True,
        # because workflow is viewonly
        sync_backref=False,
    )

    __mapper_args__ = {
        "polymorphic_identity": "dataset",
        "polymorphic_on": definition["family"],
        "with_polymorphic": "*",
    }

    def __init__(self):
        # Generates a UUID when initializing a new Dataset instance
        self.id = uuid.uuid4()

    def target(self, target="target"):
        raise Exception(
            "Class {} has no implementation for 'target'".format(
                self.__class__.__name__
            )
        )

    def to_dict(self):
        raise Exception(
            "Class {} has no implementation for 'to_dict'".format(
                self.__class__.__name__
            )
        )

    def from_dict(self):
        raise Exception(
            "Class {} has no implementation for 'from_dict'".format(
                self.__class__.__name__
            )
        )

    def init_from_dict(self, d):
        raise Exception(
            "Class {} has no implementation for 'init_from_dict'".format(
                self.__class__.__name__
            )
        )

    @orm.reconstructor
    def init_on_load(self):
        # From a dataset definition, reconstructs / deserializes the dataset instance.
        # I.e. when calling workflow.dataset (or possibly when loading the workflow
        # itself as it may eager load...) this will automatically deserialize the row
        # from the database as defined by the definition.
        # NOTE(Max): It's been a while since I dug into how @orm.reconstructor and it's
        # possible I'm misremembering or explaining inaccurately, please confirm...
        self.init_from_dict(self.definition)
