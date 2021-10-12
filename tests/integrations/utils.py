import datetime
from spire.framework.workflows.connectors import Base
from spire.framework.workflows.job_config import SpireNotebookJobConfig
from spire.framework.workflows.constants import (
    DEFAULT_ASSEMBLY_CONFIG_NAME,
    DEFAULT_SCORING_CONFIG_NAME,
    DEFAULT_TRAINING_CONFIG_NAME,
)
from spire.framework.workflows.schedule_rebalancer import ScheduleRebalancer
from spire.integrations.postgres import connector
from tests.data.cluster_config import generic_config as sample_config


def create_session():
    Base.metadata.drop_all(connector.engine)
    Base.metadata.create_all(connector.engine)
    session = connector.make_scoped_session()

    default_assembly_config = SpireNotebookJobConfig(
        name=DEFAULT_ASSEMBLY_CONFIG_NAME,
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )
    default_training_config = SpireNotebookJobConfig(
        name=DEFAULT_TRAINING_CONFIG_NAME,
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )
    default_scoring_config = SpireNotebookJobConfig(
        name=DEFAULT_SCORING_CONFIG_NAME,
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )
    session.add(default_assembly_config)
    session.add(default_training_config)
    session.add(default_scoring_config)
    session.commit()
    return session


def create_wf_def(run_date: datetime.date):
    rb = ScheduleRebalancer()
    rb.START_DATE = run_date
    wf_def = {
        "name": "spire_foo_test",
        "description": "test > test",
        "vendor": "adobe",
        "groups": ["16501879"],
        "schedule": rb.create_schedule(0, 0),  # day, hour
    }
    return wf_def
