from spire.framework.workflows.job_config.implementations.spire_vendor_notebook_job import (
    SpireVendorNotebookJobConfig,
)
from spire.framework.workflows import Workflow
from spire.framework.workflows.job_config import (
    SpireVendorNotebookJobConfig,
)

from tests.data.cluster_config import generic_config as sample_config


def test_get_jobs_group_workflow(dummy_workflows):
    default_config = SpireVendorNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )
    jobs = default_config.get_jobs(dummy_workflows)

    assert len(jobs) == 2
