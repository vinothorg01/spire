from typing import Dict

from sqlalchemy.orm.session import Session

from spire.integrations import connector
from spire.api import utils


@connector.session_transaction
def get(
    wf_id: str = None,
    wf_name: str = None,
    session: Session = None,
) -> Dict[str, int]:
    """
    From a wf_id or wf_name, returns a dict of the trait id

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        Trait id dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    return {"trait_id": workflow.trait.trait_id}


@connector.session_transaction
def update(
    wf_id: str = None,
    wf_name: str = None,
    trait_id: int = None,
    session: Session = None,
) -> Dict[str, int]:
    """
    From a wf_id or wf_name, updates the trait in the database and
    returns a dict of the trait id

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        trait_id: trait_id as int
        session: SQLAlchemy session
    Returns:
        Trait id dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    workflow.update_trait_id(session, trait_id)
    return {"trait_id": workflow.trait.trait_id}
