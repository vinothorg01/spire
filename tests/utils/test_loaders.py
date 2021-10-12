import pytest
from spire.utils.loaders import Loader, AlephV2Loader, AlephV3Loader
from spire.utils.transformers import (
    AdmantxTopicsFeatureTransformer,
    AlephV2FeatureTransformer,
    AlephV3DurationFeatureTransformer,
    AlephV3SessionFeatureTransformer,
)


def test_base_loader_is_abstract():
    with pytest.raises(TypeError):
        Loader()


def test_aleph_loader_is_singleton():
    # setup
    aleph_loader_1 = AlephV3Loader("aleph_v3_loader")
    aleph_loader_2 = AlephV3Loader("aleph_v3_loader")

    # assert
    assert aleph_loader_1 == aleph_loader_2


def test_column_validation():
    # setup
    aleph_v2_with_valid_cols = [
        "feature_brand",
        "feature_hal_topic",
        "feature_activity",
        "feature_dma",
        "feature_zip",
        "feature_lifespan",
        "feature_days_since_last_seen",
    ]
    aleph_v2_with_invalid_cols = [
        "feature_brand",
        "feature_hal_topic",
        "feature_activity",
        "feature_dma",
        "feature_zip",
        "feature_lifespan",
        "feature_days_since_last_seen",
        "boooop",
    ]
    aleph_v3_with_valid_cols = [
        "feature_brands",
        "feature_admantx",
        "feature_duration",
        "feature_dma",
        "feature_zip",
        "feature_route",
        "feature_platform",
    ]

    aleph_v3_with_invalid_cols = [
        "feature_brands",
        "feature_admantx",
        "feature_duration",
        "feature_dma",
        "feature_zip",
        "feature_route",
        "feature_platform",
        "booop",
    ]

    # exercise
    aleph_v2_loader = AlephV2Loader("aleph_v2_loader")
    aleph_v3_loader = AlephV3Loader("aleph_v3_loader")
    aleph_v2_transformers = aleph_v2_loader.get_transformers()
    aleph_v3_transformers = aleph_v3_loader.get_transformers()

    # assert
    with pytest.raises(ValueError):
        aleph_v2_loader._validate_feature_cols(
            aleph_v2_with_invalid_cols, aleph_v2_transformers.keys()
        )
    with pytest.raises(ValueError):
        aleph_v3_loader._validate_feature_cols(
            aleph_v3_with_invalid_cols, aleph_v3_transformers.keys()
        )
    assert aleph_v2_loader._validate_feature_cols(
        aleph_v2_with_valid_cols, aleph_v2_transformers.keys()
    )
    assert aleph_v3_loader._validate_feature_cols(
        aleph_v3_with_valid_cols, aleph_v3_transformers.keys()
    )


def test_correct_transformers_are_loaded():
    # setup
    aleph_v2_features = [
        "feature_brand",
        "feature_hal_topic",
        "feature_duration",
        "feature_activity",
        "feature_dma",
        "feature_zip",
        "feature_lifespan",
        "feature_days_since_last_seen",
    ]
    aleph_v3_features = [
        "feature_brands",
        "feature_dma",
        "feature_zip",
        "feature_route",
        "feature_platform",
        "feature_duration",
        "feature_session",
        "feature_admantx",
    ]
    aleph_v2_loader = AlephV2Loader("aleph-v2-loader")
    aleph_v3_loader = AlephV3Loader("aleph-v3-loader")

    # exercise
    aleph_v2_transformers = aleph_v2_loader.get_transformers()
    aleph_v3_transformers = aleph_v3_loader.get_transformers()

    # assert
    assert all([f in aleph_v2_features for f in aleph_v2_transformers.keys()])
    assert all([f in aleph_v3_features for f in aleph_v3_transformers.keys()])
    assert all(
        [
            isinstance(f, AlephV2FeatureTransformer)
            for f in aleph_v2_transformers.values()
        ]
    )
    assert isinstance(
        aleph_v3_transformers["feature_admantx"], AdmantxTopicsFeatureTransformer
    )
    assert isinstance(
        aleph_v3_transformers["feature_duration"], AlephV3DurationFeatureTransformer
    )
    assert isinstance(
        aleph_v3_transformers["feature_session"], AlephV3SessionFeatureTransformer
    )
