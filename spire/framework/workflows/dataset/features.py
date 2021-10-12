import uuid

from pyspark.sql import DataFrame
from sqlalchemy.orm import relationship
from sqlalchemy import orm

from sqlalchemy import Column, JSON
from sqlalchemy.dialects.postgresql import UUID

from spire.framework.workflows.connectors import Base, workflow_features
from spire.utils.loaders import PandasLoader, SparkLoader, AlephV2Loader, AlephV3Loader

SUPPORTED_LOADERS = ["aleph_v2_loader", "aleph_v3_loader"]


class Features(Base):
    __tablename__ = "features"
    id = Column(UUID(as_uuid=True), primary_key=True)
    definition = Column(JSON, nullable=True)
    workflows = relationship(
        "Workflow", secondary=workflow_features, back_populates="features"
    )

    REQUIRED_DEFINITION_FIELDS = [
        "columns",
        "filters",
        "format",
        "loader",
        "location",
        "name",
        "stage",
    ]

    SUPPORTED_LOADERS = ["aleph_v2_loader", "aleph_v3_loader"]  # NB: temporary

    def __init__(self, **kwargs):
        self.id = uuid.uuid4()
        for attr in Features.REQUIRED_DEFINITION_FIELDS:
            setattr(self, attr, kwargs.get(attr))

    @orm.reconstructor
    def init_on_load(self):
        self.from_dict(**self.definition)

    @staticmethod
    def _validate_loader(**kwargs):
        passed_loader = kwargs.get("loader", None)
        if passed_loader not in Features.SUPPORTED_LOADERS:
            raise ValueError(
                """Data loader {} is not
            currently supported by spire. Valid loaders are: \n {}
            """.format(
                    passed_loader, Features.SUPPORTED_LOADERS
                )
            )

    def _cache_if_uncached(self, dataset):
        # TODO: add support for other types of datasets
        if not isinstance(dataset, DataFrame):
            return dataset
        if not dataset.storageLevel.useMemory:
            return dataset.cache()
        return dataset

    def get_loader(self):
        return {
            "spark_loader": SparkLoader,
            "pandas_loader": PandasLoader,
            "aleph_v2_loader": AlephV2Loader,
            "aleph_v3_loader": AlephV3Loader,
        }[self.loader](self.name)

    def _transform_if_filters(self, alias, **kwargs):
        if not self.filters:
            return None
        return self.filters.format(**kwargs) + f" as {alias}"

    def load(self, cache=False):
        loader = self.get_loader()
        df = loader.load(self.location, self.format)
        if cache:
            return self._cache_if_uncached(df)
        return df

    def apply_filters(self, df, filters, alias):
        loader = self.get_loader()
        return loader.apply_filters(df, filters, alias)

    def transform(self, dataset):
        loader = self.get_loader()
        return loader.transform(dataset, *self.columns)

    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_dict(cls, **kwargs):
        if (
            not sorted([x for x in kwargs.keys()])
            == Features.REQUIRED_DEFINITION_FIELDS
        ):
            raise ValueError(
                """Required keys are: \n {}""".format(
                    Features.REQUIRED_DEFINITION_FIELDS
                )
            )
        Features._validate_loader(**kwargs)
        return cls(**kwargs)
