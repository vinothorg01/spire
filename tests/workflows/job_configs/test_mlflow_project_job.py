from unittest.mock import patch
from spire.framework.execution.implementations import MLFlowProjectRunJobRunner
from spire.framework.workflows.job_config import (
    MLFlowProjectAssemblyJobConfig,
    MLFlowProjectJobDefinition,
)
from spire.config import config


@patch.object(config, "GHCR_PASSWORD", "SAMPLE_PWD")
@patch("mlflow.projects.run")
def test_injects_gchr_password_into_uri(mock_run, dummy_workflows):

    definition = MLFlowProjectJobDefinition(
        uri="https://github.com/foo/bar",
        entry_point="assemble.py",
        experiment_id="1",
        backend_config={},
    )
    job_config = MLFlowProjectAssemblyJobConfig(name="test", definition=definition)
    jobs = job_config.get_jobs(dummy_workflows)

    assert len(jobs) == 3

    sample_job_run = MLFlowProjectRunJobRunner(definition)
    status = sample_job_run.run()

    status.job.uri == "https://SAMPLE_PWD@github.com/foo/bar"
