from spire.framework.workflows.dataset.target_classes import (
    Behavior,
    MatchesPercentage,
    MatchesAnyTarget,
)

from spire.framework.workflows.dataset import BinaryClassesDataset


"""
This script contains serialization logic to automate the creation of new datasets for a
given vendor and source, or in some cases other logics. In effect, these are dataset
configs, and if new target data vendors or sources are added to Spire or new kinds of
logics on existing targets need to be extended, likely they should be created here.

Most targets require a positive and negative behavior function to serialize the
respective Behaviors, which are then used to serialize a new dataset instance from the
definiion functions.
"""


def ncs_custom_positive_behavior(group, behavior=None, threshold=0.95):
    if behavior:
        behavior = Behavior(
            {"vendor": "ncs", "source": "custom", "group": group},
            {"behavior": behavior},
        )
    else:
        behavior = Behavior({"vendor": "ncs", "source": "custom", "group": group})
    return MatchesPercentage(behavior, threshold)


def ncs_custom_negative_behavior(group, behavior=None, threshold=0.1):
    if behavior:
        behavior = Behavior(
            {"vendor": "ncs", "source": "custom", "group": group},
            {"behavior": behavior},
        )
    else:
        behavior = Behavior({"vendor": "ncs", "source": "custom", "group": group})
    return MatchesPercentage(behavior, threshold, direction="less_than")


def new_ncs_custom_positive_behavior(group, behavior=None, value=1):
    if behavior:
        behavior = Behavior(
            {"vendor": "ncs", "source": "custom", "group": group, "value": value},
            {"behavior": behavior},
        )
    else:
        behavior = Behavior(
            {"vendor": "ncs", "source": "custom", "group": group, "value": value}
        )
    return behavior


def new_ncs_custom_negative_behavior(group, behavior=None, value=None):
    if behavior:
        behavior = Behavior(
            {"vendor": "ncs", "source": "custom", "group": group, "value": value},
            {"behavior": behavior},
        )
    else:
        behavior = Behavior(
            {"vendor": "ncs", "source": "custom", "group": group, "value": value}
        )
    return behavior


def adobe_behavior(sid):
    behavior = Behavior({"vendor": "adobe", "group": str(sid)})
    return behavior


def adobe_universe():
    return Behavior({"vendor": "adobe"})


def ncs_behavior(group):
    behavior = Behavior({"vendor": "ncs", "group": group, "value": 1})
    return behavior


def ncs_negative_behavior(group):
    behavior = Behavior({"vendor": "ncs", "group": group, "value": 0})
    return behavior


def dfp_positive_behavior(group):
    return Behavior({"vendor": "dfp", "source": "network_clicks", "group": group})


def dfp_negative_behavior(group):
    return Behavior({"vendor": "dfp", "source": "network_impressions", "group": group})


def condenast_behavior(group):
    behavior = Behavior({"vendor": "condenast", "group": group, "value": 1})
    return behavior


def condenast_negative_behavior(group):
    behavior = Behavior({"vendor": "condenast", "group": group, "value": 0})
    return behavior


def dar_behavior(group):
    behavior = Behavior(
        {"vendor": "nielsen", "source": "dar", "group": group, "value": 1}
    )
    return behavior


def dar_negative_behavior(group):
    behavior = Behavior(
        {"vendor": "nielsen", "source": "dar", "group": group, "value": 0}
    )
    return behavior


def ncs_custom_dataset_definition(
    groups, values_by_group=None, neg_values_by_group=None
):
    if values_by_group:
        pos_classes = [
            new_ncs_custom_positive_behavior(group, value=value)
            for group in groups
            for value in values_by_group[group]
        ]
    else:
        pos_classes = [new_ncs_custom_positive_behavior(group) for group in groups]
    if neg_values_by_group:
        neg_classes = [
            new_ncs_custom_negative_behavior(group, value=value)
            for group in groups
            for value in neg_values_by_group[group]
        ]
    else:
        neg_classes = [new_ncs_custom_negative_behavior(group) for group in groups]

    positive_class = MatchesAnyTarget(pos_classes)
    negative_class = MatchesAnyTarget(neg_classes)
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def old_ncs_custom_dataset_definition(
    groups, behaviors_by_group=None, pos_threshold=0.95, neg_threshold=0.1
):
    if behaviors_by_group:
        pos_classes = [
            ncs_custom_positive_behavior(
                group, behavior=behavior, threshold=pos_threshold
            )
            for group in groups
            for behavior in behaviors_by_group[group]
        ]
        neg_classes = [
            ncs_custom_negative_behavior(
                group, behavior=behavior, threshold=neg_threshold
            )
            for group in groups
            for behavior in behaviors_by_group[group]
        ]
    else:
        pos_classes = [
            ncs_custom_positive_behavior(group, threshold=pos_threshold)
            for group in groups
        ]
        neg_classes = [
            ncs_custom_negative_behavior(group, threshold=neg_threshold)
            for group in groups
        ]

    positive_class = MatchesAnyTarget([], classes=pos_classes)
    negative_class = MatchesAnyTarget([], classes=neg_classes)
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def adobe_dataset_definition(groups):
    behaviors = [adobe_behavior(sid) for sid in groups]
    positive_class = MatchesAnyTarget(behaviors)
    negative_class = MatchesAnyTarget([adobe_universe()])
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def ncs_dataset_definition(groups):
    behaviors = [ncs_behavior(group) for group in groups]
    positive_class = MatchesAnyTarget(behaviors)
    negative_class = MatchesAnyTarget(
        [ncs_negative_behavior(group) for group in groups]
    )
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def dfp_dataset_definition(groups):
    positive_behaviors = [dfp_positive_behavior(order_id) for order_id in groups]
    negative_behaviors = [dfp_negative_behavior(order_id) for order_id in groups]
    positive_class = MatchesAnyTarget(positive_behaviors)
    negative_class = MatchesAnyTarget(negative_behaviors)
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def condenast_dataset_definition(groups):
    behaviors = [condenast_behavior(group) for group in groups]
    positive_class = MatchesAnyTarget(behaviors)
    negative_class = MatchesAnyTarget(
        [condenast_negative_behavior(group) for group in groups]
    )
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def dar_dataset_definition(groups):
    pos_behaviors = [dar_behavior(group) for group in groups]
    neg_behaviors = [dar_negative_behavior(group) for group in groups]
    positive_class = MatchesAnyTarget(pos_behaviors)
    negative_class = MatchesAnyTarget(neg_behaviors)
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def custom_dataset_definition(source, groups):
    pos_behaviors = [custom_positive_behavior(source, group) for group in groups]
    neg_behaviors = [custom_negative_behavior(source, group) for group in groups]
    positive_class = MatchesAnyTarget(pos_behaviors)
    negative_class = MatchesAnyTarget(neg_behaviors)
    return BinaryClassesDataset.from_classes(
        {"positive": positive_class, "negative": negative_class}
    )


def custom_positive_behavior(source, group):
    behavior = Behavior(
        {"vendor": "custom", "source": source, "group": group, "value": 1}
    )
    return behavior


def custom_negative_behavior(source, group):
    behavior = Behavior(
        {"vendor": "custom", "source": source, "group": group, "value": 0}
    )
    return behavior


def make_dataset_definition(vendor, source=None, **kwargs):
    """
    This function gets called by Workflow.create_default_workflow to automatically
    create and add a dataset to a workflow at creation if the necessary fields are
    provided, but can also be called manually.
    """
    if (vendor == "ncs") and (source == "custom"):
        if kwargs.get("old", False):
            return old_ncs_custom_dataset_definition(**kwargs)
        return ncs_custom_dataset_definition(**kwargs)
    if (vendor == "ncs") and (source == "syndicated"):
        return ncs_dataset_definition(**kwargs)
    if vendor == "adobe":
        return adobe_dataset_definition(**kwargs)
    if vendor == "dfp":
        return dfp_dataset_definition(**kwargs)
    if vendor == "condenast":
        return condenast_dataset_definition(**kwargs)
    if vendor == "nielsen" and source == "dar":
        return dar_dataset_definition(**kwargs)
    if vendor == "custom":
        return custom_dataset_definition(source, **kwargs)
