import copy
from . import Dataset, constants
from spire.framework.workflows.dataset.target_classes import (
    DataRequirements,
    DatasetFamilies,
    TargetClass,
)
from spire.framework.workflows.exceptions import InvalidDefinitionException
import pyspark.sql.functions as F


class BinaryClassesDataset(Dataset):
    """
    A type of Dataset that is used to filter an assembled dataframe for a given
    workflow in the assembly stage.

    See `spire.framework.workflows.dataset.__init__.py` for a higher level explanation.
    """

    # NOTE(Max): Inline polymorphism is, I believe, related to the fact that it
    # serializes from a definition, as explained with init_on_load in `datasets.py`
    # The polymorphic identity is how the datasets table is serialized into e.g.
    # BinaryClassesDataset as opposed to the abstract base class, without the need for
    # separate join tables for each subclass.
    __mapper_args__ = {
        "polymorphic_load": "inline",
        "polymorphic_identity": DatasetFamilies.BINARY.value,
    }

    def __init__(self):
        # Runs the Dataset base class init, sets the class type (family),
        # deserializes requirements given the constants and class type, and
        # re-serializes itself into the definition attribute
        super(BinaryClassesDataset, self).__init__()
        self.family = DatasetFamilies.BINARY.value
        self.requirements = DataRequirements.from_dict(
            constants.BINARY_DATA_REQUIREMENTS, self.family
        )
        self.definition = self.to_dict()

    @classmethod
    def from_classes(cls, classes):
        # Returns a new dataset instance, not yet added to the database session or tied
        # to a workflow, created from a positive and negative TargetClass instance
        dataset = cls()
        dataset.classes = classes
        dataset.family = DatasetFamilies.BINARY.value
        dataset.definition = dataset.to_dict()
        return dataset

    @classmethod
    def from_dict(cls, definition_dict):
        # See explanation of init_on_load in `datasets.py`
        return cls().init_from_dict(definition_dict)

    def init_from_dict(self, definition_dict):
        # See explanation of init_on_load in `datasets.py`
        definition_copy = copy.deepcopy(definition_dict)
        if not definition_copy:
            return None
        self.family = definition_copy.get("family")
        if "classes" in definition_copy.keys():
            self.classes = definition_copy.get("classes")
            for target_class, class_recipe in self.classes.items():
                self.classes[target_class] = TargetClass.from_dict(class_recipe)
        if "data_requirements" not in definition_copy.keys():
            raise InvalidDefinitionException(
                "data_requirements not " "found in dataset."
            )
        data_requirements = definition_copy["data_requirements"]
        if "logic" not in data_requirements.keys():
            raise InvalidDefinitionException("data_requirement " "missing logic key.")
        logic = data_requirements.pop("logic")
        if logic not in DataRequirements.REQUIREMENT_TYPES.keys():
            raise InvalidDefinitionException(
                "data_requirement logic "
                "not recognized, "
                "must be one of {}".format(DataRequirements.REQUIREMENT_TYPES.keys())
            )
        self.requirements = DataRequirements.REQUIREMENT_TYPES[logic].from_dict(
            data_requirements, self.family
        )
        return self

    def target(self, name="target"):
        """
        This method is the most important part of a Dataset!

        It creates a "mask", a pyspark.sql dataframe column query, creating and
        filtering the positive and negative target values for each workflow from the
        combined targets and features dataset in assembly.
        """
        if hasattr(self, "classes") and hasattr(self, "family"):
            class_masks = [
                F.when(self.classes["positive"].mask(), 1).otherwise(None),
                F.when(self.classes["negative"].mask(), 0).otherwise(None),
            ]
            return F.coalesce(*class_masks).alias(str(name))
        if hasattr(self, "target_col"):
            return self.target_col.mask().alias(str(name))

    def to_dict(self):
        """
        See comment for to_dict method in `spire.framework.workflows.workflow`
        """
        output = {}
        if hasattr(self, "family"):
            output["family"] = self.family
        if hasattr(self, "classes"):
            output["classes"] = {
                class_name: target_class.to_dict()
                for class_name, target_class in self.classes.items()
            }
        if hasattr(self, "target_col"):
            output.update(self.target_col.to_dict())
        if hasattr(self, "requirements"):
            output.update({"data_requirements": self.requirements.to_dict()})
        return output
