from spire.config import config
from spire.framework.runners import JobRunner
from spire.framework.runners import (
    AssemblyRunner,
    TrainingRunner,
    ScoringRunner,
    PostProcessingRunner,
)
import os
from unittest import mock
import pytest


def test_cluster_type_is_set():
    task_name = "test_run_name_1"
    task_type = "notebook_task"
    timeout_seconds = 100
    libraries = ["pandas", "numpy"]
    runner_script = "test_runner_script_3"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        libraries=libraries,
        runner_script=runner_script,
        timeout_seconds=timeout_seconds,
    )
    with pytest.raises(ValueError):
        runner.prepare_json()


def test_cluster_types_limit_test():
    task_name = "test_run_name_2"
    task_type = "notebook_task"
    timeout_seconds = 100
    cluster_type = "existing_cluster"
    cluster_id = "test_id_2"
    libraries = ["pandas", "numpy"]
    runner_script = "test_runner_script_3"
    runner = JobRunner(
        task_name,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        task_type=task_type,
        libraries=libraries,
        runner_script=runner_script,
        timeout_seconds=timeout_seconds,
    )
    with pytest.raises(ValueError):
        runner.prepare_json()


def test_init_job_runner_existing_cluster():
    task_name = "test_run_name_3"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_3"
    libraries = ["pandas", "numpy"]
    task_type = "notebook_task"
    runner_script = "test_runner_script_3"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    json = runner.prepare_json()
    assert json["existing_cluster_id"] == "test_id_3"


def test_default_keys_on_init():
    task_name = "test_run_name_4"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_4"
    libraries = ["pandas", "numpy"]
    task_type = "notebook_task"
    runner_script = "test_runner_script_4"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    json = runner.prepare_json()
    assert all([x in json.keys() for x in json.keys()])


def test_python_spark_task():
    task_name = "test_python_spark_task"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_5"
    libraries = ["pandas", "numpy"]
    task_type = "spark_python_task"
    runner_script = "test_runner_script_5"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    json = runner.prepare_json()

    assert json["spark_python_task"]


def test_configure_runner_overrides_init_params():
    task_name = "test_configure_json_overrides_init_params"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_6"
    libraries = ["pandas", "numpy"]
    task_type = "spark_python_task"
    runner_script = "test_runner_script_6"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    original = runner.prepare_json()
    assert original["spark_python_task"]
    runner.set_attributes(task_type="notebook_task")
    updated = runner.prepare_json()
    assert updated["notebook_task"]


def test_job_param_configuring_catches_wrong_param_types():
    task_name = "test_param_type_type_checking"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_7"
    libraries = ["pandas", "numpy"]
    task_type = "spark_python_task"
    runner_script = "test_runner_script_7"
    runner = JobRunner(
        task_name=task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    json = runner.prepare_json()
    with pytest.raises(TypeError):
        runner.parse_task_params(json, ["howdy", "doo"])


def test_spark_python_task_param_setting():
    task_name = "test_param_type_type_checking"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_8"
    libraries = ["pandas", "numpy"]
    task_type = "spark_python_task"
    runner_script = "test_runner_script_8"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    json = runner.prepare_json()
    configured = runner.parse_task_params(json, {"test_data": 1})

    expected = {"python_file": runner_script, "parameters": [1]}
    assert configured["spark_python_task"] == expected


def test_spark_notebook_task_param_setting():
    task_name = "test_param_type_type_checking"
    timeout_seconds = 100
    cluster_type = "existing_cluster_id"
    cluster_id = "test_id_8"
    libraries = ["pandas", "numpy"]
    task_type = "notebook_task"
    runner_script = "test_runner_script_8"
    runner = JobRunner(
        task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        cluster_id=cluster_id,
        libraries=libraries,
        timeout_seconds=timeout_seconds,
        runner_script=runner_script,
    )
    json = runner.prepare_json()
    configured = runner.parse_task_params(json, params={"test": 1})

    expected = {"notebook_path": runner_script, "base_parameters": {"test": 1}}
    assert configured["notebook_task"] == expected


def task_existing_cluster_task_has_cluster_id():
    task_name = "test_cluster_config_settings"
    cluster_type = "existing_cluster_id"
    libraries = ["pandas", "numpy"]
    task_type = "notebook_task"
    runner_script = "test_runner_script_8"
    runner = JobRunner(
        task_name=task_name,
        task_type=task_type,
        cluster_type=cluster_type,
        libraries=libraries,
        runner_script=runner_script,
    )

    json = runner.prepare_json()
    with pytest.raises(ValueError):
        runner.set_task_params(json, ["howdy", "doo"])


def test_runner_task_name_configurability():
    task_name_1 = "test_runner_name_configurability_1"
    task_name_2 = "test_runner_name_configurability_2"
    r = JobRunner(task_name_1)
    assert r.task_name == task_name_1
    r.set_attributes(task_name=task_name_2)
    assert r.task_name == task_name_2


def test_assembly_runners_have_run_names_for_env():
    # setup
    runners = [AssemblyRunner, TrainingRunner, ScoringRunner]
    for runner in runners:
        runner.params["cluster_type"] = "existing_cluster_id"
        runner.params["cluster_id"] = "test"
    ar = AssemblyRunner()
    tr = TrainingRunner()
    sr = ScoringRunner()

    # exercise
    ar_json_dev = ar.prepare_json()
    tr_json_dev = tr.prepare_json()
    sr_json_dev = sr.prepare_json()

    # assert
    env = config.DEPLOYMENT_ENV
    assert ar_json_dev["run_name"] == f"assembly-{env}-run"
    assert tr_json_dev["run_name"] == f"training-{env}-run"
    assert sr_json_dev["run_name"] == f"scoring-{env}-run"
