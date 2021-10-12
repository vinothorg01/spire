import copy
from . import Dataset, constants
from spire.framework.workflows.dataset.target_classes import (
    DataRequirements,
    DatasetFamilies,
    TargetClass,
)
from spire.framework.workflows.exceptions import InvalidDefinitionException
import pyspark.sql.functions as F


class MultiClassesDataset(Dataset):
    """
    This Dataset type is not in active use, but is functionally similar to
    BinaryClassesDataset, which is given more detailed documentation
    """

    __mapper_args__ = {
        "polymorphic_load": "inline",
        "polymorphic_identity": DatasetFamilies.MULTICLASS.value,
    }

    def __init__(self):
        super(MultiClassesDataset, self).__init__()
        self.family = DatasetFamilies.MULTICLASS.value
        self.requirements = DataRequirements.from_dict(
            constants.MULTICLASS_DATA_REQUIREMENTS, self.family
        )
        self.definition = self.to_dict()

    @classmethod
    def from_classes(cls, classes):
        dataset = cls()
        dataset.classes = classes
        dataset.family = DatasetFamilies.MULTICLASS.value
        dataset.definition = dataset.to_dict()
        return dataset

    @classmethod
    def from_dict(cls, definition_dict):
        return cls().init_from_dict(definition_dict)

    def init_from_dict(self, definition_dict):
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
        if hasattr(self, "classes") and hasattr(self, "family"):
            class_masks = [
                F.when(target_class.mask(), class_name).otherwise(None)
                for class_name, target_class in self.classes.items()
            ]
            return F.coalesce(*class_masks).alias(str(name))
        if hasattr(self, "target_col"):
            return self.target_col.mask().alias(str(name))

    def to_dict(self):
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
