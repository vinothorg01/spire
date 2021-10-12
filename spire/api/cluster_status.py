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
    From a wf_id or wf_name, returns a dict of the cluster_status

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        cluster_status dict
    """
    workflow = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    return {cs.stage: cs.to_dict() for cs in workflow.cluster_status}
