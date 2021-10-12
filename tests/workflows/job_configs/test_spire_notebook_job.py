import datetime
import json
from spire.framework.workflows import Workflow, WorkflowStages, workflow
from spire.framework.workflows.job_config import (
    SpireNotebookJobConfig,
    SpireNotebookJobContext,
)

from tests.data.cluster_config import generic_config as sample_config


def test_get_jobs(dummy_workflows):

    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )
    jobs = default_config.get_jobs(dummy_workflows)

    assert len(jobs) == 1
    assert isinstance(jobs[0], SpireNotebookJobConfig.get_job_class())


def test_get_jobs_group_workflow(dummy_workflows):
    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )
    default_config.MAX_WF_PER_CLUSTER = 2
    jobs = default_config.get_jobs(dummy_workflows)

    assert len(jobs) == 2


def test_context_encode_fields():
    context = SpireNotebookJobContext(
        workflow_ids=["a", "b"], date=datetime.date(2020, 1, 1)
    )
    json_string = context.json()
    print(json_string)
    data = json.loads(json_string)
    assert data["workflow_ids"] == '["a", "b"]'
    assert data["date"] == "2020-01-01"
