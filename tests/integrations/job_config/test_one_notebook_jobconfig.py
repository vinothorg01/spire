import pytest
import json
from spire.framework.workflows.job_config import OneNotebookJobConfig


class TestOneNotebookJobConfig:
    @pytest.fixture(scope="module")
    def job_config(self, cluster_config):
        job_config = OneNotebookJobConfig(
            target_module="spire.tasks.execute_targets",
            target_method="main",
            new_cluster=cluster_config["new_cluster"],
            args={"foo": "bar", "baz": 1},
        )
        return job_config

    @pytest.fixture(scope="module")
    def job_config_no_args(self, cluster_config):
        # See:
        # - https://github.com/CondeNast/spire/pull/571
        # - https://github.com/CondeNast/spire/pull/569
        job_config = OneNotebookJobConfig(
            target_module="spire.tasks.execute_targets",
            target_method="main",
            new_cluster=cluster_config["new_cluster"],
        )
        return job_config

    def test_returns_single_job(self, job_config):
        jobs = job_config.get_jobs()
        assert len(jobs) == 1

    def test_set_env_vars(self, job_config):
        jobs = job_config.get_jobs()
        job = jobs[0]

        assert "spark_env_vars" in job.new_cluster

        required_envs = [
            "VAULT_TOKEN",
            "DB_HOSTNAME",
            "DEPLOYMENT_ENV",
            "VAULT_ADDRESS",
        ]
        for env in required_envs:
            assert env in job.new_cluster["spark_env_vars"]

    def test_set_notebook_params(self, job_config, job_config_no_args):
        configs_list = [job_config, job_config_no_args]
        for config in configs_list:
            jobs = config.get_jobs()
            job = jobs[0]
            params = job.notebook_task.base_parameters

            assert "target_module" in params
            assert "target_method" in params
            assert "args" in params
            assert isinstance(params["args"], str)

            args = json.loads(params["args"])

            assert "run_date" in args
            assert "workflow_ids" in args
            # if args is empty, only the default arguments
            # should be in the dictionary
            # i.e. args = {'run_date': '2021-06-29', 'workflow_ids': []}
            if len(args.keys()) > 2:
                assert "foo" in args
                assert args["foo"] == "bar"
                assert "baz" in args
                assert args["baz"] == 1
