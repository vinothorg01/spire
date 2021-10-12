"""
Spire API:

A set of interfaces on top of the core code such as the Workflow ORM. Takes plain
arguments such as strings, dicts, or ints instead of domain objects, and likewise
returns dicts (as a matter of standardization). Additionally, the API automates much of
the SQLAlchemy session management.

Serves as an intermediary layer that may be used directly, but is also leveraged by
even higher level tooling such as the CLI. So long as the assumptions of the interfaces
are themselves not violated, developers may make changes to the core code, without
having to change all higher-level use cases for users, avoiding a major anti-pattern.

Example Usage:

```python
import spire

# Dictionary serialization of a single Workflow instance
workflow = spire.workflow.get(wf_name=name)
```
"""


class ConnectorException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
