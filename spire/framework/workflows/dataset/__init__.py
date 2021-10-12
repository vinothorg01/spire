from spire.framework.workflows.dataset.datasets import Dataset
from spire.framework.workflows.dataset.binaryclasses import BinaryClassesDataset
from spire.framework.workflows.dataset.multiclasses import MultiClassesDataset
from spire.framework.workflows.dataset.features import Features

"""
Spire Dataset Framework:

`Dataset` is a class that inherits from a SQLAlchemy Base Model, mapping the attributes
of the class to our Postgres Database, in what is called an Object Relational Mapping,
or (ORM). For more information on the ORM see `spire.framework.workflows.__init__.py`

Conceptually, the Dataset is a set of attributes and criteria such as the vendor,
source, group IDs, matching logic, and data(set) requirements, which is used to filter
the assembled features and target data in the assembly process for each workflow.

It is important to note that, somewhat confusingly, there is no logic to load, view, or
directly interface with the dataset itself via the Dataset ORM. As it currently exists,
it is strictly defining the criteria for how to filter the dataset, as described below
with `dataset.target`.

NOTE the call to `dataset.target` in `spire.framework.assemble.__init__.py`, and how it
is a column query mask on the targets dataframe.

In theory the functionality of Dataset should include targets and features, but this is
not currently the case.

Where dataset is the abstract class, specific dataset instances will be
BinaryClassesDataset or MultiClassesDataset (although the latter is not currently being
used and so may be out of date).

`target_classes.py` contains various orchestration logic for datasets, such as the
DataRequirements, TargetClass, and Behaviors, which are explained further in their
respective scripts.

Example Usage:

```python
from spire.framework.workflows import Workflow
from spire.integrations.postgres import connector

session = connector.make_session()

# Get dataset from a single Workflow instance
workflow = Workflow.get_by_name(session, name)
dataset = workflow.dataset

# Serialize and print dataset
dataset.to_dict()

# Get dataset from Dataset ORM
The `from_dict` method serializes a dataset definition, but this creates a new dataset
instance. It is generally better to use the Workflow.update_dataset method insead if
using the ORM, or instead use the API or CLI.

from spire.framework.workflows.dataset import BinaryClassesDataset

dataset = BinaryClassesDataset.from_dict(definition)

# Query datasets table directly
from spire.framework.workflows.dataset import Dataset

dataset = session.query(Dataset).filter(Dataset.workflow == workflow).one()
"""

__all__ = ["Dataset", "BinaryClassesDataset", "Features", "MultiClassesDataset"]
