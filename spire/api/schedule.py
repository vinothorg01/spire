from sqlalchemy.orm.session import Session

from spire.integrations import connector
from spire.api import utils


@connector.session_transaction
def get(
    wf_id: str = None,
    wf_name: str = None,
    session: Session = None,
) -> dict:
    """
    From a wf_id or wf_name, returns a dict of the schedule definition

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        Schedule dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    return workflow.schedule.to_dict()


@connector.session_transaction
def update(
    wf_id: str = None,
    wf_name: str = None,
    definition: dict = None,
    schedule_date: str = None,
    vendor: str = None,
    session: Session = None,
) -> dict:
    """
    From a wf_id or wf_name, updates the schedule in the database and returns
    the updated dict

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        definition: schedule definition dict
        session: SQLAlchemy session
    Returns:
        Schedule dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    if schedule_date and vendor:
        workflow.add_workflow_to_balanced_schedules(
            session, vendor=vendor, start_date=schedule_date
        )
    else:
        workflow.update_schedule(session, definition)
    return workflow.schedule.to_dict()
