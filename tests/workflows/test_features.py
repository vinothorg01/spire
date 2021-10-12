import pytest
import datetime
from spire.framework.workflows import Features, Workflow

DEFAULT_ASSEMBLY_DEFINITION = {
    "columns": [
        "xidFeatures",
        "sessionFeatures",
        "brandFeatures",
        "halFeatures",
        "geoFeatures",
        "censusFeatures",
        "routingFeatures",
        "platformFeatures",
    ],
    "format": "delta",
    "loader": "aleph_v2_loader",
    "location": "s3a://cn-aleph-dev/featurestore/rolling_spire_v2",
    "name": "aleph-v2-training-dataset",
    "stage": "assembly",
    "filters": None,
}

DEFAULT_TRAINING_DEFINITION = {
    "columns": ["*"],
    "format": "delta",
    "loader": "aleph_v2_loader",
    "location": "s3a://cn-aleph-dev/featurestore/rolling_spire_v2",
    "name": "aleph-v2-training-dataset",
    "stage": "training",
    "filters": None,
}

DEFAULT_SCORING_DEFINITION = {
    "columns": [
        "xidFeatures",
        "sessionFeatures",
        "brandFeatures",
        "halFeatures",
        "geoFeatures",
        "censusFeatures",
        "routingFeatures",
        "platformFeatures",
    ],
    "format": "delta",
    "loader": "aleph_v2_loader",
    "location": "s3a://cn-aleph-dev/featurestore/rolling_spire_v2/global",
    "name": "aleph-v2-training-dataset",
    "stage": "scoring",
    "filters": None,
}

DEFAULT_DEFINITIONS = [
    DEFAULT_ASSEMBLY_DEFINITION,
    DEFAULT_TRAINING_DEFINITION,
    DEFAULT_SCORING_DEFINITION,
]


def add_stage_definitions(workflow):
    for definition in DEFAULT_DEFINITIONS:
        feature = Features.from_dict(**definition)
        workflow.features.append(feature)
    return workflow


def test_workflow_can_have_features_for_all_stages():
    # setup
    wf = Workflow(name="test", description="features_test_multi_stage")

    # exercise
    wf_with_features = add_stage_definitions(wf)

    # assert
    assert len(wf_with_features.features) == 3


def test_workflow_feature_definitions_stage_labels():
    # setup
    wf = Workflow(name="test", description="test_stage_names")
    wf_with_features = add_stage_definitions(wf)

    # exercise
    assembly_features = wf_with_features.get_feature_definition("assembly")
    training_features = wf_with_features.get_feature_definition("training")
    scoring_features = wf_with_features.get_feature_definition("scoring")

    # assert
    assert assembly_features.stage == "assembly"
    assert training_features.stage == "training"
    assert scoring_features.stage == "scoring"


def test_workflows_load_different_data_for_stages():
    # setup
    wf = Workflow(name="test", description="test_load_locations")
    wf_with_features = add_stage_definitions(wf)

    # exercise
    assembly_dataset_location = wf_with_features.get_feature_definition(
        "assembly"
    ).location
    scoring_dataset_location = wf_with_features.get_feature_definition(
        "scoring"
    ).location
    assert assembly_dataset_location == DEFAULT_ASSEMBLY_DEFINITION.get("location")
    assert scoring_dataset_location == DEFAULT_SCORING_DEFINITION.get("location")


def test_workflows_must_use_existing_loader():
    # setup
    INVALID_LOADER_DEFINITION = {
        "columns": [
            "feature_brand",
            "feature_hal_topic",
            "feature_duration",
            "feature_activity",
            "feature_dma",
            "feature_zip",
            "feature_lifespan",
            "feature_days_since_last_seen",
        ],
        "format": "delta",
        "loader": "boop_loader",
        "location": "s3a://cn-aleph-dev/featurestore/rolling_spire_v2",
        "name": "aleph-v2-training-dataset",
        "stage": "assembly",
    }

    # exercise and assert
    with pytest.raises(ValueError):
        feature_definition = Features.from_dict(**INVALID_LOADER_DEFINITION)


def test_filter_transformers_single_arg():
    definition = DEFAULT_ASSEMBLY_DEFINITION
    definition["filters"] = "date >= DATE('{date}')"
    single_filter_def = Features.from_dict(**definition)
    kwargs = {"date": datetime.date(2020, 8, 10)}
    transformed = single_filter_def._transform_if_filters("test", **kwargs)
    assert transformed == "date >= DATE('2020-08-10') as test"


def test_filter_transformers_no_args():
    definition = DEFAULT_TRAINING_DEFINITION
    no_filter_def = Features.from_dict(**definition)
    kwargs = {"date": datetime.date(2014, 8, 8)}
    transformed = no_filter_def._transform_if_filters("test", **kwargs)
    assert not transformed


def test_filter_transformers_multiple_args():
    definition = DEFAULT_SCORING_DEFINITION
    definition["filters"] = "date >= DATE('{date}') AND " "o_origin = '{o_origin}'"
    multi_filter_def = Features.from_dict(**definition)
    kwargs = {"date": datetime.date(2020, 8, 1), "o_origin": "vogue-jp"}
    transformed = multi_filter_def._transform_if_filters("test", **kwargs)
    assert (
        transformed == "date >= DATE('2020-08-01') AND " "o_origin = 'vogue-jp' as test"
    )
