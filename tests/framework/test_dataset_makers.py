from spire.framework.workflows.dataset.dataset_makers import (
    make_dataset_definition,
    custom_negative_behavior,
    custom_positive_behavior,
    custom_dataset_definition,
)


CUSTOM_DATASET_DEFINITION_FIXTURE = {
    "family": "binary",
    "classes": {
        "positive": {
            "logic": "matches_any",
            "args": {
                "behaviors": [
                    {
                        "aspect": {},
                        "vendor": "custom",
                        "source": "custom123",
                        "group": "18-24",
                        "value": 1,
                    }
                ]
            },
        },
        "negative": {
            "logic": "matches_any",
            "args": {
                "behaviors": [
                    {
                        "aspect": {},
                        "vendor": "custom",
                        "source": "custom123",
                        "group": "18-24",
                        "value": 0,
                    }
                ]
            },
        },
    },
    "data_requirements": {
        "logic": "min_max_provided",
        "min_positive": 100,
        "min_negative": 100,
        "max_positive": 250000,
        "max_negative": 250000,
    },
}

CUSTOM_NEGATIVE_BEHAVIOR_FIXTURE = {
    "vendor": "custom",
    "source": "custom345",
    "group": ["18-24"],
    "value": 0,
}
CUSTOM_POSTIVE_BEHAVIOR_FIXTURE = {
    "vendor": "custom",
    "source": "custom345",
    "group": ["18-24"],
    "value": 1,
}


def test_custom_dataset_definition():
    dataset_output = make_dataset_definition(
        vendor="custom", source="custom123", groups=["18-24"]
    )
    assert dataset_output.definition == CUSTOM_DATASET_DEFINITION_FIXTURE
    # Note that custom_dataset_definition is called within make_dataset_definition
    # Hence, they have the same output
    dataset_output = custom_dataset_definition(source="custom123", groups=["18-24"])
    assert dataset_output.definition == CUSTOM_DATASET_DEFINITION_FIXTURE


def test_custom_behaviors():
    neg_behaviors = custom_negative_behavior("custom345", ["18-24"])
    assert neg_behaviors.attributes == CUSTOM_NEGATIVE_BEHAVIOR_FIXTURE
    pos_behaviors = custom_positive_behavior("custom345", ["18-24"])
    assert pos_behaviors.attributes == CUSTOM_POSTIVE_BEHAVIOR_FIXTURE
