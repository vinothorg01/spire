from spire.framework.execution.base import JobStatuses
from unittest.mock import patch
import pytest
import pydantic
from spire.framework.execution.implementations.databricks_run_submit import (
    DatabricksRunSubmitJob,
    DatabricksRunSubmitJobRunner,
    DatabricksRunSubmitJobStatus,
)


def test_validate_task_config(config):
    # Should raise error from pydantic validation if config doesn't work
    DatabricksRunSubmitJob(**config)


def test_validate_task_config_two_tasks(config, notebook_config):
    # sample_config has spark_jar_task, should only have one type of ImplementationTasks
    config["notebook_task"] = notebook_config["notebook_task"]
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        DatabricksRunSubmitJob(**config)


def test_validate_task_config_no_task(config):
    # Removing the task should raise a pydantic validation error
    del config["spark_jar_task"]
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        DatabricksRunSubmitJob(**config)


@patch("spire.framework.execution.implementations.databricks_run_submit.requests.get")
@patch("spire.framework.execution.implementations.databricks_run_submit.requests.post")
def test_runner_and_job_status(patch_post, patch_get, config):
    post_resp = patch_post.return_value
    post_resp.ok = True
    post_resp.json.return_value = {"run_id": 1}

    get_resp = patch_get.return_value
    get_resp.ok = True
    get_resp.json.return_value = {
        "state": {"life_cycle_state": "TERMINATED"},
        "run_page_url": "https://foo.bar",
    }

    job = DatabricksRunSubmitJob(**config)
    runner = DatabricksRunSubmitJobRunner(job)

    assert patch_post.called is False
    job_status = runner.run()
    assert patch_post.called is True

    assert isinstance(job_status, DatabricksRunSubmitJobStatus)
    status = job_status.get_status()
    assert isinstance(status, JobStatuses)
    assert status == JobStatuses.SUCCESS


@patch("spire.framework.execution.implementations.databricks_run_submit.requests.get")
@patch("spire.framework.execution.implementations.databricks_run_submit.requests.post")
def test_result_state_take_precendence(patch_post, patch_get, config):
    post_resp = patch_post.return_value
    post_resp.ok = True
    post_resp.json.return_value = {"run_id": 1}

    get_resp = patch_get.return_value
    get_resp.ok = True
    get_resp.json.return_value = {
        # this is not a valid combination, but used to test that result_state
        # was actually used
        "state": {"life_cycle_state": "PENDING", "result_state": "SUCCESS"},
        "run_page_url": "https://foo.bar",
    }

    job = DatabricksRunSubmitJob(**config)
    runner = DatabricksRunSubmitJobRunner(job)
    job_status = runner.run()

    assert isinstance(job_status, DatabricksRunSubmitJobStatus)
    status = job_status.get_status()
    assert isinstance(status, JobStatuses)
    assert status == JobStatuses.SUCCESS


def test_parameters_notebook_task(notebook_config):
    workflow_ids_parsed = (
        "995a8b35-4d55-4fda-8ce6-cf902f9d4588," "e083c0db-bc69-4471-b1bd-373816046409"
    )
    date = notebook_config["notebook_task"].base_parameters["date"]
    job = DatabricksRunSubmitJob(**notebook_config)
    assert job.parameters["run_date"] == date
    assert job.parameters["workflow_ids"] == workflow_ids_parsed


def test_parse_params_not_implemented(config):
    # The config fixture is a spark_jar_task, which is not supported by _parse_params
    job = DatabricksRunSubmitJob(**config)
    with pytest.raises(Exception):
        job.parameters


def test_parse_notebook_params_no_date(notebook_config):
    del notebook_config["notebook_task"].base_parameters["date"]
    job = DatabricksRunSubmitJob(**notebook_config)
    with pytest.raises(Exception):
        job.parameters


def test_parse_notebook_params_no_workflow_ids(notebook_config):
    del notebook_config["notebook_task"].base_parameters["workflow_ids"]
    job = DatabricksRunSubmitJob(**notebook_config)
    with pytest.raises(Exception):
        job.parameters


@pytest.mark.skip("TODO")
def test_parameters_spark_python_task():
    pass
