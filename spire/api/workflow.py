from datetime import datetime
from typing import Dict, Union, Any

from sqlalchemy.orm.session import Session

from spire.integrations import connector
from spire.framework.workflows import Workflow
from spire.framework.workflows.schedule import Schedule
from spire.api import utils


@connector.session_transaction
def create(
    name: str,
    description: str,
    vendor: str,
    source: str = None,
    trait_id: str = None,
    enabled: bool = False,
    start_date: datetime = None,
    schedule: Dict[str, Any] = None,
    session: Session = None,
    **kwargs
) -> Dict[str, Union[str, int, bool]]:
    """
    Given the args type hinted by this function or in some cases certain optional
    keyword arguments, creates a new Workflow, creates any relations, commits it to the
    database, and returns a dict the workflow

    Args:
        Various workflow-related parameters
        kwargs: Additional keyword arguments used in certain cases,
                such as list of groups for dataset
        session: SQLAlchemy session
    Returns:
        Dict of workflow
    """
    if not utils.is_workflow_exist(name, session):
        if schedule:
            schedule = Schedule.from_dict(schedule)
        workflow = Workflow.create_default_workflow(
            name,
            description,
            vendor,
            source=source,
            trait_id=trait_id,
            enabled=enabled,
            start_date=start_date,
            schedule=schedule,
            session=session,
            **kwargs
        )
        session.add(workflow)
        return workflow.to_dict()
    else:
        return {}


@connector.session_transaction
def get(
    wf_id: str = None,
    wf_name: str = None,
    session: Session = None,
) -> Dict[str, Union[str, int, bool]]:
    """
    From a wf_id or wf_name, returns a dict of the workflow

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        Dict of workflow
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    return workflow.to_dict()


@connector.session_transaction
def update(
    wf_id: str = None,
    wf_name: str = None,
    definition: Dict[str, Union[str, bool]] = None,
    session: Session = None,
) -> Dict[str, Union[str, int, bool]]:
    """
    From a wf_id or wf_name, updates the workflow attributes in the definition dict and
    returns a dict of the workflow. This can be used to update certain attributes on
    the workflow table: name, description, or enabled, but not relationship attributes
    such as dataset or schedule

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        Dict of workflow
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    workflow.name = definition.get("name", workflow.name)  # type: ignore
    workflow.description = definition.get(  # type: ignore
        "description", workflow.description
    )
    workflow.enabled = definition.get("enabled", workflow.enabled)  # type: ignore
    session.autoflush = False
    return workflow.to_dict()
