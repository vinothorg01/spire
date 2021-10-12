import pytest
import datetime
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from spire.framework.workflows import Workflow
from spire.framework.workflows.schedule_rebalancer import ScheduleRebalancer
from tests.integrations import utils


@pytest.fixture(scope="module")
def session():
    session = utils.create_session()
    yield session
    session.close()


@pytest.fixture(scope="module")
def run_date() -> datetime.datetime:
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    return yesterday.replace(hour=6, minute=0, second=0, microsecond=0)


@pytest.fixture(scope="module")
def wf_def(run_date: datetime.date):
    return utils.create_wf_def(run_date)


@pytest.fixture(scope="module")
def wf_ids(
    wf_def: Dict[str, Any], run_date: datetime.datetime, session: Session
) -> List[str]:
    wf_def["enabled"] = True
    del wf_def["schedule"]
    # NOTE(Max): The default schedule instance passed in from utils.create_wf_def
    # doesn't work for multiple workflows- it just passes the object between them, so
    # we need to create a new schedule instance for each workflow instead. Since we did
    # not previously have tests on spire.tasks, this was not an issue in the stages
    # functional tests that also define the schedule this way.
    workflows = []
    for i in range(5):
        name = f"spire_foo_{i+1}"
        wf_def["name"] = name
        workflow = Workflow.create_default_workflow(**wf_def, session=session)
        rb = ScheduleRebalancer()
        rb.START_DATE = run_date
        schedule = rb.create_schedule(0, 0)
        workflow.schedule = schedule
        workflow.update_trait_id(session, i + 1)
        session.add(workflow)
        workflows.append(workflow)
    session.commit()
    workflow_ids = [str(wf.id) for wf in workflows]
    return workflow_ids
