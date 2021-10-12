import pytest
from spire.framework.workflows.dataset.target_classes import Behavior, MatchesAnyTarget
from spire.framework.workflows.dataset import MultiClassesDataset


@pytest.fixture(scope="module")
def multiclass_dataset_definition():
    """Old Acxiom code to create multiclass dataset definition"""

    def _multiclass_income_behavior(income_class):
        return Behavior(
            {"vendor": "multiclass_test", "group": "income"},
            {"income_class": income_class},
        )

    INCOME_DICT = {"<75k": [1, 2, 3, 4, 5, 6], "75k-<125k": [7, 8], "125k+": [9]}

    return MultiClassesDataset.from_classes(
        {
            key: MatchesAnyTarget([_multiclass_income_behavior(i) for i in val])
            for key, val in INCOME_DICT.items()
        }
    )
