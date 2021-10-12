import pytest
from tests.data.cluster_config import generic_config as sample_config
from spire.framework.execution.implementations.databricks_run_submit import NotebookTask


@pytest.fixture(scope="function")
def config():
    config = sample_config.copy()
    return config


@pytest.fixture(scope="function")
def notebook_config():
    config = sample_config.copy()
    date = "2021-06-19"
    workflow_ids = (
        '["995a8b35-4d55-4fda-8ce6-cf902f9d4588",'
        '"e083c0db-bc69-4471-b1bd-373816046409"]'
    )
    notebook_task_dict = {
        "date": date,
        "threadpool_group_size": 20,
        "workflow_ids": workflow_ids,
    }
    notebook_task = NotebookTask(
        notebook_path="foo/bar", base_parameters=notebook_task_dict
    )
    config["notebook_task"] = notebook_task
    del config["spark_jar_task"]
    return config
