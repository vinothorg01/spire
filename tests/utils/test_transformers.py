import json
import pytest
from itertools import chain

from pyspark.sql import Row, functions as F
from pyspark.sql.types import ArrayType

from spire.utils.s3 import read_json

# TODO(Max): Add tests for commented-out Transformers
from spire.utils.transformers import (
    # AdmantxTopicsFeatureTransformer,
    FeatureTransformer,
    AlephV2FeatureTransformer,
    AlephV3FeatureTransformer,
    # AlephV3DurationFeatureTransformer,
    # AlephV3SessionFeatureTransformer,
    transform_arrays,
    # extract_array_features,
    extract_struct_features,
    reformat_feature_names,
    remove_features,
    rename_features,
)
from spire.utils.spark_helpers import spark_session
from tests.data.aleph_spire_features import aleph_v2_data, aleph_v3_data


def load_aleph_data(version):
    ref = {"v2": aleph_v2_data, "v3": aleph_v3_data}
    return ref[version]


BUCKET = "cn-spire"
ADMANTX_TOPICS_STORAGE_KEY = "data/admantx/topics.json"


def test_feature_transformer_base_is_abstract():
    with pytest.raises(NotImplementedError):
        FeatureTransformer()


@pytest.mark.skip(reason="tries to access s3 wihout a mock")
def test_s3_admantx_features_match_local_cfg():
    # setup
    with open("./cfg/admantx_topics.json") as f:
        local_cfg_topics = json.loads(f.read())

    # exercise
    remote_topics = read_json(BUCKET, ADMANTX_TOPICS_STORAGE_KEY)

    # assert
    assert local_cfg_topics == remote_topics


def test_admantx_transformer_transforms_df():
    pass


@spark_session
def test_aleph_v2_transformers_match_aleph_columns(spark=None):

    # setup
    aleph_v2_features_raw = load_aleph_data("v2")
    aleph_v2_feature_categories = [
        "feature_brand",
        "feature_hal_topic",
        "feature_activity",
        "feature_dma",
        "feature_zip",
        "feature_lifespan",
        "feature_days_since_last_seen",
    ]
    excluded_cols = ["feature_admantx_all", "first_seen_date", "last_seen_date"]
    aleph_v2_df = spark.createDataFrame(aleph_v2_features_raw)
    aleph_v2_transformers = {
        feature: AlephV2FeatureTransformer(feature)
        for feature in aleph_v2_feature_categories
    }
    transformed_cols = []

    # exercise
    for feature, transformer in aleph_v2_transformers.items():
        preprocessed = transformer.preprocess_features(aleph_v2_df)
        transformed_cols.append([x for x in preprocessed.columns])
    flattened = [c for feature_cols in transformed_cols for c in feature_cols]

    # assert dataframe columns are identical
    assert not set(flattened) - set(
        [c for c in aleph_v2_df.columns if c not in excluded_cols]
    )


@pytest.mark.skip("Skipping until aleph v3 status is resolved")
def test_standard_aleph_v3_transformers_match_aleph_columns(spark_session):

    # setup
    aleph_v3_features_raw = load_aleph_data("v3")
    aleph_v3_features_df = spark_session.createDataFrame(aleph_v3_features_raw)
    standard_feature_categories = [
        "feature_brands",
        "feature_dma",
        "feature_zip",
        "feature_route",
        "feature_platform",
    ]
    standard_feature_transformers = {
        f: AlephV3FeatureTransformer(f) for f in standard_feature_categories
    }

    brands_feature_transformed = [
        Row(
            infinity_id="0000e07f-abf0-4fb4-8e80-9aa1cf5cbdf3",
            feature_brands_glamour=4,
            feature_brands_wired=0,
        ),
        Row(
            infinity_id="00036604-35a6-4fda-a9ca-d318ce28b6c0",
            feature_brands_glamour=0,
            feature_brands_wired=1,
        ),
    ]
    dma_features_transformed = [
        Row(
            infinity_id="0000e07f-abf0-4fb4-8e80-9aa1cf5cbdf3",
            feature_dma_612=0,
            feature_dma_753=5,
        ),
        Row(
            infinity_id="00036604-35a6-4fda-a9ca-d318ce28b6c0",
            feature_dma_612=1,
            feature_dma_753=0,
        ),
    ]
    # TODO(Max): Implement test with zip features if eventually needed...
    # zip_features_transformed = [
    #     Row(infinity_id='0000e07f-abf0-4fb4-8e80-9aa1cf5cbdf3',
    #         feature_zip_74728=0, feature_zip_85147=5),
    #     Row(infinity_id='00036604-35a6-4fda-a9ca-d318ce28b6c0',
    #         feature_zip_74728=1, feature_zip_85147=0)
    # ]
    route_features_transformed = [
        Row(
            infinity_id="0000e07f-abf0-4fb4-8e80-9aa1cf5cbdf3",
            feature_route_internal=1,
            feature_route_referral=1,
            feature_route_search=2,
            feature_route_social=0,
        ),
        Row(
            infinity_id="00036604-35a6-4fda-a9ca-d318ce28b6c0",
            feature_route_internal=0,
            feature_route_referral=0,
            feature_route_search=0,
            feature_route_social=1,
        ),
    ]
    platform_features_transformed = [
        Row(
            infinity_id="00036604-35a6-4fda-a9ca-d318ce28b6c0",
            feature_platform_desktop=1,
            feature_platform_mobile=0,
        ),
        Row(
            infinity_id="0000e07f-abf0-4fb4-8e80-9aa1cf5cbdf3",
            feature_platform_desktop=0,
            feature_platform_mobile=4,
        ),
    ]

    # exercise
    results = {}
    for feature in standard_feature_categories:
        transformed = standard_feature_transformers[feature].preprocess_features(
            aleph_v3_features_df
        )
        results[feature] = transformed.collect()

    # assert
    assert brands_feature_transformed == results["feature_brands"]
    assert dma_features_transformed == results["feature_dma"]
    assert route_features_transformed == results["feature_route"]
    assert platform_features_transformed == results["feature_platform"]


@spark_session
def test_reformat_feature_names(spark=None):
    # TODO(Max): Make a fixture or in some other way streamline this
    features_df = spark.createDataFrame(load_aleph_data("v3"))
    features = [
        "feature_duration",
        "feature_session",
        "feature_brands",
        "feature_admantx",
        "feature_route",
        "feature_platform",
    ]
    column_list_all = []
    column_index_lookup = {}
    ARRAY_COLUMNS = []
    exploded_arrays = []
    for feature in features:
        if isinstance(features_df.schema[feature].dataType, ArrayType):
            ARRAY_COLUMNS.append(feature)
            exploded_array = (
                features_df.select(F.explode(feature))
                .select(F.col("col").getItem("name").alias("col"))
                .filter(F.col("col").isNotNull())
                .distinct()
                .collect()
            )
            exploded_arrays.append(exploded_array)
            column_list = sorted(list(map(lambda x: x[0], exploded_array)))
            column_list_all.append(column_list)
            column_index_lookup.update({c: i for i, c in enumerate(column_list)})
    transformed_df = transform_arrays(
        features_df,
        array_cols=ARRAY_COLUMNS,
        column_index_lookup=column_index_lookup,
        column_list=column_list_all,
    )
    x_feats = [
        "feature_dma",
        "feature_duration",
        "feature_session",
        "feature_topics",
        "feature_zip",
        "infinity_id",
        "last_seen_date",
    ]
    transformed_df = remove_features(transformed_df, x_feats)
    renamed_cols_fixture = [
        "feature_brands_glamour",
        "feature_brands_wired",
        "feature_admantx_brand_protection__parental_protection",
        "feature_admantx_feelings__emotions",
        "feature_admantx_topics__automotive",
        "feature_route_internal",
        "feature_route_search",
        "feature_platform_desktop",
        "feature_platform_mobile",
    ]
    renamed_df = reformat_feature_names(transformed_df)
    for col in renamed_cols_fixture:
        assert col in renamed_df.columns


@spark_session
def test_transform_arrays(spark=None):
    # NOTE(Max): This is effectively also a test of extract_array_features.
    # Struggling to figure out if/how to test extract_array_features without
    # the columns_udf in transform_arrays.
    features_df = spark.createDataFrame(load_aleph_data("v3"))
    features = [
        "feature_duration",
        "feature_session",
        "feature_brands",
        "feature_admantx",
        "feature_route",
        "feature_platform",
    ]
    column_list_all = []
    column_index_lookup = {}
    ARRAY_COLUMNS = []
    exploded_arrays = []
    for feature in features:
        if isinstance(features_df.schema[feature].dataType, ArrayType):
            ARRAY_COLUMNS.append(feature)
            exploded_array = (
                features_df.select(F.explode(feature))
                .select(F.col("col").getItem("name").alias("col"))
                .filter(F.col("col").isNotNull())
                .distinct()
                .collect()
            )
            exploded_arrays.append(exploded_array)
            column_list = sorted(list(map(lambda x: x[0], exploded_array)))
            column_list_all.append(column_list)
            column_index_lookup.update({c: i for i, c in enumerate(column_list)})
    transformed_df = transform_arrays(
        features_df,
        array_cols=ARRAY_COLUMNS,
        column_index_lookup=column_index_lookup,
        column_list=column_list_all,
    )
    transformed_df = remove_features(transformed_df, ARRAY_COLUMNS)
    x_feats = [
        "feature_dma",
        "feature_duration",
        "feature_session",
        "feature_topics",
        "feature_zip",
        "infinity_id",
        "last_seen_date",
    ]
    transformed_df = remove_features(transformed_df, x_feats)
    column_list_flat = list(chain.from_iterable(column_list_all))
    columns_ungrouped = [
        col.replace(f"{f_name}_", "")
        for f_name in ARRAY_COLUMNS
        for col in transformed_df.columns
        if f_name in col
    ]
    assert len(ARRAY_COLUMNS) == len(column_list_all)
    assert len(column_list_flat) == len(transformed_df.columns)
    assert column_list_flat == columns_ungrouped


@spark_session
def test_extract_struct_features(spark=None):
    features_df = spark.createDataFrame(load_aleph_data("v3"))
    old_columns = features_df.columns
    struct_col = "feature_duration"
    features = ["lifespan", "days_since_last_seen"]
    feature_cols = extract_struct_features(features_df, struct_col, features)
    features_df = features_df.select("*", *feature_cols)
    assert (len(old_columns) + len(feature_cols)) == len(features_df.columns)
    for name in features:
        assert name in features_df.columns


@spark_session
def test_remove_features(spark=None):
    features_df = spark.createDataFrame(load_aleph_data("v3"))
    old_columns = features_df.columns
    x_feats = ["feature_dma", "feature_zip"]
    features_df = remove_features(features_df, x_feats)
    assert len(old_columns) == (len(features_df.columns) + len(x_feats))
    for name in x_feats:
        assert name not in features_df.columns


@spark_session
def test_rename_features(spark=None):
    features_df = spark.createDataFrame(load_aleph_data("v3"))
    old_columns = features_df.columns
    old_names = ["infinity_id", "last_seen_date"]
    new_names = ["xid", "date"]
    features_df = rename_features(features_df, old_names, new_names)
    assert len(old_columns) == len(features_df.columns)
    for name in old_names:
        assert name not in features_df.columns
    for name in new_names:
        assert name in features_df.columns
