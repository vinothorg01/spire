import os
from spire.framework.workflows.dataset.dataset_makers import make_dataset_definition
from spire.framework.workflows.dataset.target_classes import DatasetFamilies


ENVIRONMENT = os.environ.get("DEPLOYMENT_ENV", "test")


# Test DatasetFamilies.MULTICLASS
def test_multiclass_dataset_families(multiclass_dataset_definition):
    multiclass_dataset = multiclass_dataset_definition
    assert multiclass_dataset.family == DatasetFamilies.MULTICLASS.value


# Test DatasetFamilies.BINARY
def test_binary_dataset_families():
    binary_dataset = make_dataset_definition(vendor="adobe", groups=["test"])
    assert binary_dataset.family == DatasetFamilies.BINARY.value


# Test ORM reconstructor multiclass dataset
def test_reconstructor_multiclass(multiclass_dataset_definition):
    # setup
    dataset = multiclass_dataset_definition
    # exercise
    testset = dataset.init_from_dict(dataset.definition)
    # assert
    dataset.to_dict() == testset.to_dict()


# Test ORM reconstructor binary dataset
def test_reconstructor_binary():
    # setup
    dataset = make_dataset_definition(vendor="adobe", groups=["test"])

    # exercise
    testset = dataset.init_from_dict(dataset.definition)

    # assert
    dataset.to_dict() == testset.to_dict()
