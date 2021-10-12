import copy
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, FloatType
from spire.framework.workflows.exceptions import (
    InvalidDefinitionException,
    InsufficientDataException,
)
from spire.framework.workflows import constants
from enum import Enum

"""
NOTE(Max) This script is a bit of a jumble, there's no getting around that without
refactoring. This docstring will provide a high-level overview but you will likely need
dig into the code with the tooling e.g. as described in
`spire.framework.workflows.dataset.__init__` in order to properly understand

The base classes `TargetClass` and `DataRequirements` are used to facilitate
serialization, deserialization, constructing he `dataset.target` mask, and basic
data validation, and are in various ways called by the `Dataset` class or instances,
added as attributes onto them, or constructed from them.
"""


class TargetClass:
    """
    TargetClass can be broken down into the matching logic e.g. MatchesAnyTarget,
    MatchesPercentage, etc., which define how the mask is constructed, and Behavior
    which serves more as a schema manager for serialization and deserialization.
    """

    def __init__(self):
        pass

    @classmethod
    def from_dict(cls, cls_recipe_dict):
        if "logic" not in cls_recipe_dict.keys():
            raise InvalidDefinitionException("Logic key not in " "target class dataset")
        logic = cls_recipe_dict["logic"]

        if "args" not in cls_recipe_dict.keys():
            raise InvalidDefinitionException("Args key not in " "target class dataset")
        args = cls_recipe_dict["args"]

        if logic not in TARGET_TYPES.keys():
            raise InvalidDefinitionException(
                "Target class logic must be specified"
                " and be one of {}.".format(TARGET_TYPES.keys())
            )

        return TARGET_TYPES[logic].from_dict(args)

    def to_dict(self):
        raise NotImplementedError()

    def mask(self):
        raise NotImplementedError()


class MatchesAnyTarget(TargetClass):
    # See TargetClass docstring
    NAME = "matches_any"

    def __init__(self, behaviors, classes=None):
        super(MatchesAnyTarget, self).__init__()
        self.behaviors = behaviors
        self.inner_classes = classes
        if not self.inner_classes:
            self.inner_classes = []

    @classmethod
    def from_dict(cls, args_dict):
        if "behaviors" not in args_dict:
            raise InvalidDefinitionException(
                "arguments not provided " "for dataset of target class"
            )
        behavior_dicts = args_dict["behaviors"]

        behaviors = [
            Behavior.from_dict(behavior_dict) for behavior_dict in behavior_dicts
        ]

        if "classes" in args_dict:
            classes = [
                TargetClass.from_dict(class_dict) for class_dict in args_dict["classes"]
            ]
        else:
            classes = []
        return cls(behaviors, classes)

    def to_dict(self):
        output = {
            "logic": MatchesAnyTarget.NAME,
            "args": {"behaviors": [behavior.to_dict() for behavior in self.behaviors]},
        }

        if len(self.inner_classes) > 0:
            output["args"]["classes"] = [
                inner_class.to_dict() for inner_class in self.inner_classes
            ]
        return output

    def mask(self):
        behaviors_matched = [behavior.mask() for behavior in self.behaviors]
        inner_classes_matched = [
            inner_class.mask() for inner_class in self.inner_classes
        ]
        any_conditions_matched = reduce(
            lambda x, y: x | y, behaviors_matched + inner_classes_matched
        )
        return any_conditions_matched


class MatchesAllExceptTarget(TargetClass):
    # See TargetClass docstring
    NAME = "matches_all_except"

    def __init__(self, behaviors, excluded_behaviors):
        super(MatchesAllExceptTarget, self).__init__()
        self.behaviors = behaviors
        self.excluded_behaviors = excluded_behaviors

    @classmethod
    def from_dict(cls, args_dict):
        if "behaviors" not in args_dict:
            raise InvalidDefinitionException(
                "arguments not provided for " "dataset of target class"
            )
        behavior_dicts = args_dict["behaviors"]

        behaviors = []
        for behavior_dict in behavior_dicts:
            behaviors.append(Behavior.from_dict(behavior_dict))

        if "excluded_behaviors" not in args_dict:
            raise InvalidDefinitionException(
                "arguments not provided for " "dataset of target class"
            )
        excluded_behavior_dicts = args_dict["excluded_behaviors"]

        excluded_behaviors = []
        for excluded_behavior_dict in excluded_behavior_dicts:
            excluded_behaviors.append(Behavior.from_dict(excluded_behavior_dict))
        return cls(behaviors, excluded_behaviors)

    def to_dict(self):

        return {
            "logic": MatchesAllExceptTarget.NAME,
            "args": {
                "behaviors": [behavior.to_dict() for behavior in self.behaviors],
                "excluded_behaviors": [
                    behavior.to_dict() for behavior in self.excluded_behaviors
                ],
            },
        }

    def mask(self):
        behaviors_matched = [behavior.mask() for behavior in self.behaviors]
        any_behaviors_matched = reduce(lambda x, y: x | y, behaviors_matched)

        excluded_behaviors_matched = [
            behavior.mask() for behavior in self.excluded_behaviors
        ]
        any_excluded_behaviors_matched = reduce(
            lambda x, y: x | y, excluded_behaviors_matched
        )

        w = Window.partitionBy(constants.ID_VAR)

        return F.min(
            F.coalesce(any_excluded_behaviors_matched, any_behaviors_matched)
        ).over(w)


class MatchesPercentage(TargetClass):
    # See TargetClass docstring
    NAME = "matches_percentage"

    def __init__(
        self, behavior, percentage, value_col="value", direction="greater_than"
    ):
        super(MatchesPercentage, self).__init__()
        self.behavior = behavior
        self.percentage = percentage
        self.value_col = value_col
        self.direction = direction

    @classmethod
    def from_dict(cls, args_dict):
        if "behavior" not in args_dict:
            raise InvalidDefinitionException(
                "No behavior provided in dataset of" " {} target class".format(cls.NAME)
            )
        behavior_dict = args_dict["behavior"]

        behavior = Behavior.from_dict(behavior_dict)
        if "value_col" not in args_dict:
            raise InvalidDefinitionException(
                "No value_col provided in dataset of"
                " {} target class".format(cls.NAME)
            )
        value_col = args_dict["value_col"]

        if "percentage" not in args_dict:
            raise InvalidDefinitionException(
                "No percentage provided in dataset of"
                " {} target class".format(cls.NAME)
            )
        percentage = args_dict["percentage"]

        if "direction" not in args_dict:
            raise InvalidDefinitionException(
                "No direction provided in dataset of"
                " {} target class".format(cls.NAME)
            )
        direction = args_dict["direction"]
        if direction not in ("greater_than", "less_than"):
            raise InvalidDefinitionException(
                "direction must be greater_than or less_than"
            )

        target_class = cls(behavior, percentage, value_col, direction)

        return target_class

    def to_dict(self):
        return {
            "logic": self.NAME,
            "args": {
                "behavior": self.behavior.to_dict(),
                "value_col": self.value_col,
                "percentage": self.percentage,
                "direction": self.direction,
            },
        }

    def mask(self):
        behavior_matched = self.behavior.mask()
        w = Window.partitionBy(
            F.when(behavior_matched, behavior_matched.cast(IntegerType())).otherwise(
                (F.rand() * (-(constants.NUM_PARTITIONS - 1))).cast(IntegerType())
            )
        ).orderBy(self.value_col)
        w2 = Window.partitionBy(
            F.when(behavior_matched, behavior_matched.cast(IntegerType())).otherwise(
                (F.rand() * (-(constants.NUM_PARTITIONS - 1))).cast(IntegerType())
            )
        )
        perc = F.when(
            behavior_matched, (F.row_number().over(w) / F.count("*").over(w2))
        )

        if self.direction == "greater_than":
            mask = (perc >= self.percentage) & behavior_matched
        elif self.direction == "less_than":
            mask = (perc <= self.percentage) & behavior_matched
        else:
            raise Exception("direction not initialized to a valid value.")
        return mask


class ColumnAsTarget(TargetClass):
    NAME = "column_as_target"

    def __init__(self):
        super(ColumnAsTarget, self).__init__()

    @classmethod
    def from_dict(cls, args_dict):
        target_class = cls()
        if "behavior" not in args_dict.keys():
            raise InvalidDefinitionException(
                "behavior key not found in args of {}".format(cls.NAME)
            )
        target_class.target_behavior = Behavior.from_dict(args_dict["behavior"])
        if "target" not in args_dict.keys():
            raise InvalidDefinitionException(
                "target key not found in args of {}".format(cls.NAME)
            )
        target_class.target_var = args_dict["target"]
        return target_class

    def to_dict(self):
        output = {}
        output["logic"] = self.NAME
        output["args"] = {
            "behavior": self.target_behavior.to_dict(),
            "target": self.target_var,
        }
        return output

    def mask(self):
        behavior_matched = self.target_behavior.mask()
        return F.when(behavior_matched, F.col(self.target_var)).otherwise(None)


class Behavior(TargetClass):
    # See TargetClass docstring
    # Additionally, note how this is used in `dataset_makers.py`
    def __init__(self, attributes=None, aspects=None):
        super(Behavior, self).__init__()
        if attributes:
            self.attributes = attributes
        else:
            self.attributes = {}
        if aspects:
            self.aspects = aspects
        else:
            self.aspects = {}

    @classmethod
    def from_dict(cls, behavior_dict):

        b = copy.deepcopy(behavior_dict)
        try:
            aspects = b.pop("aspect")
        except KeyError:
            aspects = {}
        attributes = b
        if b.get("logic") == "general_behavior":
            behavior = GeneralBehavior(attributes, aspects)
        else:
            behavior = cls(attributes, aspects)
        return behavior

    def to_dict(self):
        output = {"aspect": {key: value for key, value in self.aspects.items()}}
        output.update({key: value for key, value in self.attributes.items()})
        return output

    def mask(self):
        aspects_matched = [
            F.get_json_object("aspect", "$.{}".format(key)) == value
            for key, value in self.aspects.items()
        ]

        attributes_matched = [
            F.col(col) == value
            for col, value in self.attributes.items()
            if value is not None
        ]
        null_attributes_matched = [
            F.col(col).isNull()
            for col, value in self.attributes.items()
            if value is None
        ]
        attributes_matched = (
            attributes_matched + null_attributes_matched + aspects_matched
        )
        return reduce(lambda x, y: x & y, attributes_matched)


class GeneralBehavior(TargetClass):
    def __init__(self, attributes=None, aspects=None):
        super(GeneralBehavior, self).__init__()
        # TODO(Max): This approach to deserialization,
        #  popping the general_behavior logic attribute,
        #  is confusing and not pythonic.
        if "logic" in attributes.keys():
            assert attributes.pop("logic") == "general_behavior"
        if attributes:
            self.attributes = attributes
        else:
            self.attributes = {}
        if aspects:
            self.aspects = aspects
        else:
            self.aspects = {}

    @classmethod
    def from_dict(cls, behavior_dict):

        b = copy.deepcopy(behavior_dict)
        assert b.pop("logic") == "general_behavior"
        try:
            aspects = b.pop("aspect")
        except KeyError:
            aspects = {}
        attributes = b
        behavior = cls(attributes, aspects)
        return behavior

    def to_dict(self):
        output = {"aspect": {key: value for key, value in self.aspects.items()}}
        output.update({key: value for key, value in self.attributes.items()})
        output["logic"] = "general_behavior"
        return output

    @staticmethod
    def _matches_attribute(key, value):
        if isinstance(value, dict) and {"value", "direction"}.issubset(
            set(value.keys())
        ):
            if value["direction"] == "greater_than":
                return F.col(key) >= value["value"]
            if value["direction"] == "less_than":
                return F.col(key) <= value["value"]
            raise Exception("direction must be one of " "['less_than', 'greater_than']")
        return F.col(key) == value

    @staticmethod
    def _matches_aspect(key, value):
        if isinstance(value, dict) and {"value", "direction"}.issubset(
            set(value.keys())
        ):
            if value["direction"] == "greater_than":
                return F.get_json_object("aspect", "$.{}".format(key)).cast(
                    FloatType()
                ) >= float(value["value"])
            if value["direction"] == "less_than":
                return F.get_json_object("aspect", "$.{}".format(key)).cast(
                    FloatType()
                ) <= float(value["value"])
            raise Exception("direction must be one of " "['less_than', 'greater_than']")
        return F.get_json_object("aspect", "$.{}".format(key)) == value

    def mask(self):
        aspects_matched = [
            self._matches_aspect(key, value) for key, value in self.aspects.items()
        ]

        attributes_matched = [
            self._matches_attribute(key, value)
            for key, value in self.attributes.items()
            if value is not None
        ]
        null_attributes_matched = [
            F.col(key).isNull()
            for key, value in self.attributes.items()
            if value is None
        ]
        all_attributes_matched = (
            attributes_matched + null_attributes_matched + aspects_matched
        )
        return reduce(lambda x, y: x & y, all_attributes_matched)


"""This must be defined after the above class dataset.
   This also prevents it from being split off into another file.
   The optimal solution to this is unknown."""
TARGET_TYPES = {
    MatchesAnyTarget.NAME: MatchesAnyTarget,
    MatchesAllExceptTarget.NAME: MatchesAllExceptTarget,
    MatchesPercentage.NAME: MatchesPercentage,
    ColumnAsTarget.NAME: ColumnAsTarget,
}


class DataRequirements:
    """
    Abstract class from which data requirements are defined, in part as a function of
    the specific sub-class logic, but also minimum and maximum number positive and
    negative targets in the dataset
    """

    def __init__(self):
        pass

    @classmethod
    def default(cls):
        # TODO(Max): Deprecate default. This only
        #  made sense when we only had one model class
        default_config = {
            "max_positive": 250000,
            "min_positive": 100,
            "min_negative": 100,
            "logic": "min_max_provided",
            "max_negative": 250000,
        }
        return cls.from_dict(default_config, "binary")

    @classmethod
    def from_dict(cls, args, family):
        if "logic" not in args.keys():
            raise InvalidDefinitionException("logic must be in keys")
        return DataRequirements.REQUIREMENT_TYPES[args["logic"]].from_dict(args, family)

    def to_dict(self):
        raise NotImplementedError()

    def check(self, df, target_col="target"):
        raise NotImplementedError()

    def meet(self, df, target_col="target"):
        raise NotImplementedError()


class MinMaxProvided(DataRequirements):
    NAME = "min_max_provided"

    def __init__(self):
        super(MinMaxProvided, self).__init__()

    @classmethod
    def from_dict(cls, args, family):
        data_reqs = cls()
        data_reqs.family = family
        data_reqs.min_requirements = {}
        data_reqs.max_requirements = {}
        for key in args.keys():
            if key[:4] == "min_":
                data_reqs.min_requirements[key[4:]] = args[key]
            if key[:4] == "max_":
                data_reqs.max_requirements[key[4:]] = args[key]
        return data_reqs

    def to_dict(self):
        output = {"logic": self.NAME}
        output.update(
            {
                "min_{}".format(key): value
                for key, value in self.min_requirements.items()
            }
        )
        output.update(
            {
                "max_{}".format(key): value
                for key, value in self.max_requirements.items()
            }
        )
        return output

    def check(self, target_df, target_col="target"):
        counts = dict(target_df.groupby(target_col).count().collect())
        if None in counts.keys():
            raise Exception("Null values should be " "removed from target column.")
        if self.family == DatasetFamilies.BINARY.value:
            if 1 not in counts.keys():
                raise InsufficientDataException("No positives found in file")
            if 0 not in counts.keys():
                raise InsufficientDataException("No negatives found in file")
            counts["positive"] = counts.pop(1)
            counts["negative"] = counts.pop(0)

        for value_name, min_value in self.min_requirements.items():
            if counts[value_name] < min_value:
                raise InsufficientDataException(
                    "number of {}: {} less than required minimum value: {}".format(
                        value_name, counts[value_name], min_value
                    )
                )
        return counts

    def meet(self, target_df, target_col="target"):
        counts = dict(target_df.groupby(target_col).count().collect())
        if None in counts.keys():
            raise Exception("Null values should be " "removed from target column.")
        if self.family == DatasetFamilies.BINARY.value:
            if 1 not in counts.keys():
                raise InsufficientDataException("No positives after join.")
            if 0 not in counts.keys():
                raise InsufficientDataException("No negatives after join.")
            counts["positive"] = counts.pop(1)
            counts["negative"] = counts.pop(0)

        for value_name, min_value in self.min_requirements.items():
            if counts[value_name] < min_value:
                raise InsufficientDataException(
                    "number of {}: {} less than required minimum value: {}".format(
                        value_name, counts[value_name], min_value
                    )
                )
        sampling_ratios = {}
        for value_name, max_value in self.max_requirements.items():
            if counts[value_name] > max_value:
                sampling_ratios[value_name] = max_value / counts[value_name]
            else:
                sampling_ratios[value_name] = 1.0

        if self.family == DatasetFamilies.BINARY.value:
            sampling_ratios[1] = sampling_ratios.pop("positive")
            sampling_ratios[0] = sampling_ratios.pop("negative")
        elif self.family == DatasetFamilies.MULTICLASS:
            for i, class_name in enumerate(counts.keys()):
                sampling_ratios[i] = sampling_ratios.pop(class_name)

        output_df = target_df.sampleBy(target_col, sampling_ratios)
        output_df.cache()

        counts = dict(output_df.groupby(target_col).count().collect())

        if self.family == DatasetFamilies.BINARY.value:
            counts["positive"] = counts.pop(1)
            counts["negative"] = counts.pop(0)

        return output_df, counts


class NoRequirements(DataRequirements):
    NAME = "no_requirements"

    def __init__(self):
        super(NoRequirements, self).__init__()

    @classmethod
    def from_dict(cls, args, family):
        data_reqs = cls()
        data_reqs.family = family
        return data_reqs

    def to_dict(self):
        output = {"logic": self.NAME}
        return output

    def check(self, target_df, target_col="target"):
        counts = dict(target_df.groupby(target_col).count().collect())
        if None in counts.keys():
            raise Exception("Null values should be" " removed from target column.")
        if self.family == DatasetFamilies.BINARY.value:
            if 1 not in counts.keys():
                raise InsufficientDataException("No positives found in file")
            if 0 not in counts.keys():
                raise InsufficientDataException("No negatives found in file")
            counts["positive"] = counts.pop(1)
            counts["negative"] = counts.pop(0)
        return counts

    def meet(self, target_df, target_col="target"):
        output_df = target_df
        counts = dict(output_df.groupby(target_col).count().collect())
        if self.family == DatasetFamilies.BINARY.value:
            counts["positive"] = counts.pop(1)
            counts["negative"] = counts.pop(0)
        return output_df, counts


DataRequirements.REQUIREMENT_TYPES = {
    MinMaxProvided.NAME: MinMaxProvided,
    NoRequirements.NAME: NoRequirements,
}


class LoadHelper:
    def __init__(self):
        pass

    @classmethod
    def from_dict(cls, args):
        loading_helper = cls()
        if "vendor" in args.keys():
            loading_helper.vendor = args["vendor"]
        if "load_ratio" in args.keys():
            loading_helper.load_ratio = args["load_ratio"]
        return loading_helper

    def to_dict(self):
        output = {}
        if hasattr(self, "vendor"):
            output["vendor"] = self.vendor
        if hasattr(self, "load_ratio"):
            output["load_ratio"] = self.load_ratio
        return output


class DatasetFamilies(Enum):
    BINARY = "binary"
    MULTICLASS = "multiclass"
