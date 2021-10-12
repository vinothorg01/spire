import json
from datetime import date
from spire.framework.workflows import Workflow, WorkflowStages
from spire.framework.workflows.job_config import (
    SpireNotebookJobConfig,
    SpireVendorNotebookJobConfig,
)

from tests.data.cluster_config import generic_config as sample_config


def test_get_jobs_for_workflows(session):
    wf1 = Workflow("test1", description="")
    wf2 = Workflow("test2", description="")
    wf3 = Workflow("test3", description="")

    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )

    wf1.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf2.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf3.job_configs[WorkflowStages.TRAINING.value] = default_config

    session.add(wf1)
    session.add(wf2)
    session.add(wf3)
    session.commit()

    wfs = list(session.query(Workflow).all())

    jobs = Workflow.get_jobs_for_workflows(wfs, WorkflowStages.TRAINING, date.today())

    assert len(jobs) == 1
    assert isinstance(jobs[0], SpireNotebookJobConfig.get_job_class())


def test_get_jobs_multiple_job_types():
    wf1 = Workflow("spire_vendor1_1", description="")
    wf2 = Workflow("spire_vendor1_2", description="")
    wf3 = Workflow("spire_vendor2_3", description="")

    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )

    vendor_config = SpireVendorNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/vendor"},
        },
    )

    wf1.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf2.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf3.job_configs[WorkflowStages.TRAINING.value] = vendor_config

    jobs = Workflow.get_jobs_for_workflows(
        [wf1, wf2, wf3], stage=WorkflowStages.TRAINING, run_date=date.today()
    )
    assert len(jobs) == 2

    def filter_jobs_by_notebook_path(jobs, path):
        return list(
            filter(
                lambda job: job.notebook_task.notebook_path == path,
                jobs,
            )
        )

    vendor_job = filter_jobs_by_notebook_path(jobs, "/foo/vendor")[0]
    default_job = filter_jobs_by_notebook_path(jobs, "/foo/bar")[0]

    vendor_wf_ids = json.loads(vendor_job.notebook_task.base_parameters["workflow_ids"])
    assert len(vendor_wf_ids) == 1
    assert vendor_wf_ids[0] == str(wf3.id)

    default_wf_ids = json.loads(
        default_job.notebook_task.base_parameters["workflow_ids"]
    )
    assert len(default_wf_ids) == 2
    assert str(wf1.id) in default_wf_ids
    assert str(wf2.id) in default_wf_ids
