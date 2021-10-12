from spire.framework.execution.base import JobStatuses
from unittest.mock import patch
import mlflow
import pytest
import pydantic
from spire.framework.execution.implementations.mlflow_project_run import (
    MLFlowProjectRunJob,
    MLFlowProjectRunJobRunner,
    MLFlowProjectRunJobStatus,
)
from spire.framework.execution import JobStatuses
from pathlib import Path


@pytest.fixture()
def mlflow_exp_id(tmp_path: Path):
    tmp_path = tmp_path / "tracking"
    tmp_path.mkdir()
    mlflow.set_tracking_uri(str(tmp_path))
    exp_id = mlflow.create_experiment("foo")
    return exp_id


@pytest.fixture()
def mlflow_projects(tmp_path: Path):
    tmp_path = tmp_path / "project"
    tmp_path.mkdir()
    with open(tmp_path / "train.py", "w") as f:
        f.write("print('Hello World')\n")

    return {"path": tmp_path, "entry": "train.py"}


def test_mlflow_projects_runner(mlflow_projects, mlflow_exp_id):
    job = MLFlowProjectRunJob(
        uri=str(mlflow_projects["path"]),
        entry_point=mlflow_projects["entry"],
        backend="local",
        use_conda=False,
        experiment_id=mlflow_exp_id,
    )
    runner = MLFlowProjectRunJobRunner(job)
    status = runner.run()
    assert isinstance(status, MLFlowProjectRunJobStatus)

    MLFlowProjectRunJobStatus.monitor_statuses([status], interval=0.1)

    assert status.get_status() == JobStatuses.SUCCESS
