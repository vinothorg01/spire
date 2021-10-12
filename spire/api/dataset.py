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
    From a wf_id or wf_name, returns a dict of the dataset definition

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        definition: dataset definition dict
        session: SQLAlchemy session
    Returns:
        Dataset dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    return workflow.dataset.to_dict()


@connector.session_transaction
def update(
    wf_id: str = None,
    wf_name: str = None,
    vendor: str = None,
    source: str = None,
    groups: str = None,
    definition: dict = None,
    session: Session = None,
) -> dict:
    """
    From a wf_id or wf_name, updates the dataset in the database and return
    the updated dict.

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        definition: dataset definition dict
        session: SQLAlchemy session
    Returns:
        Dataset dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    if vendor:
        dataset_obj = utils.create_dataset(vendor, source, groups)
        if dataset_obj:
            workflow.update_dataset(session, dataset_obj=dataset_obj)
    elif definition:
        workflow.update_dataset(session, definition=definition)
    return workflow.dataset.to_dict()
