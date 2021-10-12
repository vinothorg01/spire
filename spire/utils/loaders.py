from abc import ABC, abstractmethod
import functools
from pyspark.sql import functions as F

from spire.utils import singleton
from spire.utils.spark_helpers import spark_session
from spire.utils.transformers import (
    AdmantxTopicsFeatureTransformer,
    AlephV3FeatureTransformer,
    AlephV3DurationFeatureTransformer,
    AlephV3SessionFeatureTransformer,
    AlephV2FeatureTransformer,
    transform_arrays,
)
from deprecation import deprecated


class Loader(ABC):
    @staticmethod
    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def transform(self, dataset, *args):
        pass

    @abstractmethod
    def get_transformers(self):
        pass

    def _validate_feature_cols(self, passed_cols, transformers):
        for col in passed_cols:
            if col not in transformers:
                raise ValueError(
                    """
                Passed features column {} is not not present in valid
                transformers for loader {}. Valid feature columns are: {}
                """.format(
                        col, self.name, transformers
                    )
                )
        return True


class SparkLoader(Loader):
    @staticmethod
    @spark_session
    def load(location, format, spark=None):
        return spark.read.format(format).load(location)

    @staticmethod
    @spark_session
    def apply_filters(dataset, filters, alias, spark=None):
        return (
            dataset.selectExpr("*", filters)
            .filter(F.col(alias) == True)  # noqa: E712
            .drop(alias)
        )


class AlephLoader(SparkLoader):
    def __init__(self, name):
        self.name = name

    @deprecated(
        details="""# NOTE(Max): For Spire Global this is being largely deprecated by
        # the transform_array_features method on AlephV3Loader along with
        # several functions in transformers.py
        """
    )
    @functools.lru_cache(maxsize=128)
    def transform(self, dataset, *args):
        transformed_dfs = []
        transformers = self.get_transformers()
        self._validate_feature_cols(args, transformers.keys())

        for arg in args:
            try:
                transformer = transformers[arg]
                feature_df = transformer.preprocess_features(dataset)
                transformed_dfs.append(feature_df)
            except KeyError as e:
                raise e
        return functools.reduce(
            lambda df1, df2: df1.join(df2, on="infinity_id"), transformed_dfs
        )


@singleton
class AlephV2Loader(AlephLoader):
    def get_transformers(self):
        feature_categories = [
            "feature_brand",
            "feature_hal_topic",
            "feature_activity",
            "feature_dma",
            "feature_zip",
            "feature_lifespan",
            "feature_days_since_last_seen",
        ]
        return {
            feature: AlephV2FeatureTransformer(feature)
            for feature in feature_categories
        }


@singleton
class AlephV3Loader(AlephLoader):
    def get_transformers(self):
        # NOTE(Max): This is basically deprecated for Spire Global
        feature_categories = [
            "feature_brands",
            "feature_dma",
            "feature_zip",
            "feature_route",
            "feature_platform",
        ]
        return {
            "feature_admantx": AdmantxTopicsFeatureTransformer("feature_admantx"),
            "feature_duration": AlephV3DurationFeatureTransformer(
                "feature_duration"
            ),  # noqa
            "feature_session": AlephV3SessionFeatureTransformer("feature_session"),
            **{f: AlephV3FeatureTransformer(f) for f in feature_categories},
        }

    def transform_array_features(
        self, dataset, array_cols=None, column_index_path=None, column_list_path=None
    ):
        """
        This deprecates the transform method on AlephLoader and
        Transformer class functionalit for Spire Global.
        """
        return transform_arrays(
            dataset, array_cols, column_index_path, column_list_path
        )


class PandasLoader(Loader):
    @staticmethod
    def load(location, format):
        pass
