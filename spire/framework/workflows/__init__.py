from spire.framework.workflows.workflow import Workflow
from spire.framework.workflows.trait import Trait
from spire.framework.workflows.schedule import Schedule
from spire.framework.workflows.history import History
from spire.framework.workflows.dfp_order import DFPOrder
from spire.framework.workflows.dataset.datasets import Dataset
from spire.framework.workflows.dataset.features import Features
from spire.framework.workflows.cluster.cluster_status import ClusterStatus
from spire.framework.workflows.postprocessor.postprocessor import Postprocessor
from spire.framework.workflows.tag import Tag
from spire.framework.workflows.workflow_utils_mixin import WorkflowUtilsMixin
from spire.framework.workflows.workflow_stages import WorkflowStages


"""
Spire Workflow Framework:

`Workflow` is a class that inherits from a SQLAlchemy Base Model, mapping the attributes
of the class to our Postgres Database, in what is called an Object Relational Mapping,
or (ORM).

This is the "lowest level" of the Core codebase for the model work flow logic, short of
the actual database connection and base model itself. In most cases as a user, it is
more straightforward to use the API, which eschews the ORM domain objects for
dictionaries and automates much of the SQLAlchemy session management.

The __tablename__ references the table in the database, and the attributes are the
columns of the table, such that a workflow instance represents a deserialization of a
row from that table.

The relationships and association proxies are essentially joins on other tables keyed
by workflow id, and there are also various class, static, or instance Workflow methods.

Most (but admittedly not all) of the other Classes in this directory are likewise ORMs,
such as the aforementioned relationhsips and association proxies, and can be
instantiated individually, or by calling workflow.<relationship> e.g. `workflow.dataset`
on a given workflow instance.

Example Usage:

```python
from spire.framework.workflows import Workflow
from spire.integrations.postgres import connector

session = connector.make_session()

# Single Workflow instance
workflow = Workflow.get_by_name(session, name)

# List of Workflow instances
workflows = Workflow.load_all_enabled(session)
```
"""

__all__ = [
    "Workflow",
    "Trait",
    "Schedule",
    "History",
    "Dataset",
    "Features",
    "ClusterStatus",
    "Postprocessor",
    "Tag",
    "WorkflowUtilsMixin",
    "DFPOrder",
    "WorkflowStages",
]
