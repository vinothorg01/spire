import uuid
from sqlalchemy.orm.session import Session
from spire.framework.workflows import Workflow
from spire.framework.workflows.dataset.dataset_makers import make_dataset_definition


def get_by_id_or_name(
    wf_id: str = None,
    wf_name: str = None,
    session: Session = None,
) -> Workflow:
    """
    Helper function to get workflow by id or name
    """
    if wf_id:
        return Workflow.get_by_id(session, uuid.UUID(wf_id))
    return Workflow.get_by_name(session, wf_name)


def is_workflow_exist(
    wf_name: str = None,
    session: Session = None,
) -> bool:
    q = session.query(Workflow).filter(Workflow.name == wf_name)
    exists = session.query(q.exists()).scalar()
    if exists:
        print("Given workflow already exist")
    return exists


def create_dataset(vendor: str = None, source: str = None, groups: str = None) -> dict:
    try:
        dataset_args = {}
        if groups:
            dataset_args = {"groups": groups.split(",")}
        dataset = make_dataset_definition(vendor=vendor, source=source, **dataset_args)
        if dataset:
            return dataset
        else:
            print("Dataset is not created due to insufficient data")
            return {}
    except Exception as e:
        print(e)
