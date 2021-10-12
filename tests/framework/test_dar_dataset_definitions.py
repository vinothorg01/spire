import os

from spire.framework.workflows.dataset.dataset_makers import (
    make_dataset_definition,
    dar_dataset_definition,
    dar_behavior,
    dar_negative_behavior,
)

from unittest.mock import MagicMock, call


ENVIRONMENT = os.environ.get("DEPLOYMENT_ENV", "test")


def test_dar_make_dataset_definition():
    # setup
    session = MagicMock()
    dataset = make_dataset_definition(vendor="nielsen", source="dar", groups=["25-54"])

    # exercise
    session.add(dataset)
    session.commit(dataset)

    # assert
    session.add.assert_has_calls([call(dataset)])
    session.commit.assert_called_once()


def test_dar_dataset_definition():
    groups = ["Female"]
    try:
        dataset = dar_dataset_definition(groups)
    except Exception as e:
        raise e


def test_dar_behaviors_length():
    groups = ["18-34", "Female"]
    pos_behavior = [dar_behavior(group) for group in [groups[0]]]
    pos_behaviors = [dar_behavior(group) for group in groups]
    assert len(pos_behavior) == 1
    assert len(pos_behaviors) == 2


def test_dar_negative_behaviors_length():
    groups = ["18-34", "Female"]
    neg_behavior = [dar_negative_behavior(group) for group in [groups[0]]]
    neg_behaviors = [dar_negative_behavior(group) for group in groups]
    assert len(neg_behavior) == 1
    assert len(neg_behaviors) == 2


def test_dar_behavior_attributes():
    groups = ["35-44", "Male"]
    pos_behaviors = [dar_behavior(group) for group in groups]
    attributes = {
        "35-44": {"vendor": "nielsen", "source": "dar", "group": "35-44", "value": 1},
        "Male": {"vendor": "nielsen", "source": "dar", "group": "Male", "value": 1},
    }
    for i in range(len(pos_behaviors)):
        behav_dict = pos_behaviors[i].attributes
        keys, values = zip(*behav_dict.items())
        attribute_keys = tuple(attributes[groups[i]].keys())
        attirubte_values = tuple(attributes[groups[i]].values())
        assert keys == attribute_keys
        assert values == attirubte_values


def test_dar_negative_behavior_attributes():
    groups = ["35-44", "Male"]
    neg_behaviors = [dar_negative_behavior(group) for group in groups]
    attributes = {
        "35-44": {"vendor": "nielsen", "source": "dar", "group": "35-44", "value": 0},
        "Male": {"vendor": "nielsen", "source": "dar", "group": "Male", "value": 0},
    }
    for i in range(len(neg_behaviors)):
        behav_dict = neg_behaviors[i].attributes
        keys, values = zip(*behav_dict.items())
        attribute_keys = tuple(attributes[groups[i]].keys())
        attirubte_values = tuple(attributes[groups[i]].values())
        assert keys == attribute_keys
        assert values == attirubte_values
