import yaml
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType

from spire.utils.s3 import read_json
from spire.config import config
from deprecation import deprecated

BUCKET = config.SPIRE_ENVIRON
ADMANTX_TOPICS_KEY = "data/admantx/topics.json"

logger = logging.getLogger()


class FeatureTransformer:
    def __init__(self):
        raise NotImplementedError

    def preprocess_features(self):
        raise NotImplementedError

    def select_and_explode(self, dataset):
        dataset = dataset.select("infinity_id", self.feature_name)
        return dataset.select(
            "infinity_id", F.explode(self.feature_name).alias(self.feature_name)  # noqa
        )


class AlephFeatureTransformer(FeatureTransformer):
    def __init__(self, feature_name, fields=["name", "count"]):
        self.feature_name = feature_name
        self.fields = fields


class AlephV3FeatureTransformer(AlephFeatureTransformer):
    def extract_col_name_values(self, dataset, return_col_names=True):
        cols_to_select = []
        for field in self.fields:
            field_col_name = f"{self.feature_name}_{field}"
            dataset = dataset.withColumn(
                field_col_name, F.col(self.feature_name)[field]
            )
            cols_to_select.append(field_col_name)
        if not return_col_names:
            return dataset.select("infinity_id", *cols_to_select)
        return dataset.select("infinity_id", *cols_to_select), cols_to_select

    @deprecated(
        details="# NOTE(Max): This is deprecated by the reformat_feature_names function"
    )
    def rename_cols(self, dataset):
        feature_cols = [c for c in dataset.columns if c != "infinity_id"]
        mapping = {c: f"{self.feature_name}_{c}" for c in feature_cols}
        return dataset.select(
            "infinity_id",
            *[F.col(c).alias(mapping.get(c, c)) for c in feature_cols],  # noqa
        )

    def pivot_on_cols(self, dataset, feature_name_col, feature_value_col, fillna=True):
        pivoted_dataset = (
            dataset.groupBy("infinity_id")
            .pivot(feature_name_col)
            .agg(F.first(feature_value_col))
        )
        if fillna:
            return pivoted_dataset.fillna(0)
        return pivoted_dataset

    def preprocess_features(self, dataset):
        exploded = self.select_and_explode(dataset)
        extracted_cols_df, col_names = self.extract_col_name_values(exploded)
        name_col, value_col = col_names[0], col_names[1]
        pivoted = self.pivot_on_cols(extracted_cols_df, name_col, value_col)
        return self.rename_cols(pivoted)


class AlephV3DurationFeatureTransformer(AlephV3FeatureTransformer):
    def __init__(
        self,
        feature_name="feature_duration",
        fields=["lifespan", "days_since_last_seen"],
    ):
        super(AlephV3FeatureTransformer, self).__init__(
            feature_name=feature_name, fields=fields
        )

    def preprocess_features(self, dataset):
        return self.extract_col_name_values(dataset, return_col_names=False)


class AlephV3SessionFeatureTransformer(AlephV3FeatureTransformer):
    def __init__(
        self,
        feature_name="feature_session",
        fields=["unique_sessions", "unique_pageviews", "total_pageviews"],  # noqa
    ):
        super(AlephV3FeatureTransformer, self).__init__(
            feature_name=feature_name, fields=fields
        )

    def preprocess_features(self, dataset):
        return self.extract_col_name_values(dataset, return_col_names=False)


class AlephV2FeatureTransformer(AlephFeatureTransformer):
    def preprocess_features(self, dataset):
        cols_to_select = [x for x in dataset.columns if self.feature_name in x]
        if not cols_to_select:
            raise ValueError(
                """No columns exist for category {}
                             in passed dataset {}
                             """.format(
                    self.feature_category, dataset
                )
            )
        return dataset.select("infinity_id", *cols_to_select)


class AdmantxTopicsFeatureTransformer(AlephFeatureTransformer):
    def load_topics(self):
        return [
            self.transform_topics_row(x)
            for x in read_json(BUCKET, ADMANTX_TOPICS_KEY)  # noqa
        ]

    def get_topics_transformer_udf(self):
        return F.udf(lambda x: self.transform_topics_row(x))

    def transform_topics_row(self, row):
        try:
            text = row.replace("::", "_").replace(" ", "_")
            transformed = "".join(
                [ch for ch in text if ch.isalnum() or ch in [" ", "_"]]
            )
            return f"{self.feature_name}_{transformed}".lower()
        except Exception as e:
            raise e

    def transform_topics_df(self, topics_df):
        udf = self.get_topics_transformer_udf()
        return topics_df.withColumn(
            "topic_features", udf(F.col(self.feature_name)["name"])
        ).withColumn("topic_score", F.col(self.feature_name)["score_sum"])

    def group_by_xid_and_pivot(self, dataset, topics_map):
        return (
            dataset.groupBy("infinity_id")
            .pivot("topic_features", values=topics_map)
            .agg(F.first("topic_score"))
        )

    def preprocess_features(self, dataset):
        topics_map = self.load_topics()
        topics_exploded = self.select_and_explode(dataset)
        transformed = self.transform_topics_df(topics_exploded)
        result = self.group_by_xid_and_pivot(transformed, topics_map)
        return result.select(
            "infinity_id", *[x for x in result.columns if "topics_" in x]
        )


def extract_array_features(col_list, lookup_dict):
    """
    Unpacks a pyspark array into a dense form, where
    each feature in the array becomes a column. Uses
    the lookup_dict for the index of the feature in the
    col_list. TODO(MAX): It is still not totally clear
    to me how to use this without the columns_df in
    transform_arrays. Need to figure that out...
    """
    col_list = col_list or []
    features = [0 for i in range(len(lookup_dict))]
    for feature in col_list:
        try:
            index = lookup_dict[feature["name"]]
            features[index] = feature["count"]
        except Exception as e:
            logger.warning(e)
            logger.warning(f"missing key: {feature['name']}")
    return features


def extract_struct_features(dataset, struct_col, features):
    """
    extract a list of pyspark dataframe column objects from a
    StructType column. These columns can then be selected from
    the dataset.
    """
    feature_cols = []
    for f in features:
        feature = F.col(struct_col)[f].alias(f)
        feature_cols.append(feature)
    return feature_cols


def reformat_feature_names(dataset):
    new_cols = [
        f"{x.lower().replace('::', '__').replace(' ', '_').replace('.', '')}"
        for x in dataset.columns
    ]
    dataset = dataset.toDF(*new_cols)
    return dataset


def remove_features(dataset, feature_names):
    return dataset.select(
        *[F.col(x) for x in dataset.columns if x not in feature_names]
    )


def rename_features(dataset, old_names, new_names):
    """
    This function is slow and should not be used for many features, or
    for features that can be renamed systematically such as with the
    reformat_feature_names function.
    """
    for i in range(len(old_names)):
        dataset = dataset.withColumnRenamed(old_names[i], new_names[i])
    return dataset


def transform_arrays(
    dataset, array_cols=None, column_index_lookup=None, column_list=None
):
    """
    Transform array features from Aleph_v3 such that each feature
    within the array becomes its own column in the dataframe.

    The column_index_lookups is a dictionary of manually-picked
    features as keys, with the value their index in the column_list.
    The column_list is nested by the number of feature arrays and should
    correspond to the length of array_cols.

    The array_cols is a list of the feature arrays from Aleph that
    correspond to the features in column_list and column_index_lookup.

    column_index_lookup and column_list can be a string path to a yaml
    file, or the dict and list respectively.
    """
    if isinstance(column_index_lookup, str):
        column_index_lookup = yaml.safe_load(open(column_index_lookup))
    if isinstance(column_list, str):
        column_list = yaml.safe_load(open(column_list))
    columns_udf = F.udf(
        lambda col_list: extract_array_features(col_list, column_index_lookup),
        ArrayType(IntegerType()),
    )
    return dataset.select(
        "*",
        *[
            columns_udf(array_cols[c])[i].alias(f"{array_cols[c]}_{column_list[c][i]}")
            for c in range(len(array_cols))
            for i in range(len(column_list[c]))
        ],
    )
