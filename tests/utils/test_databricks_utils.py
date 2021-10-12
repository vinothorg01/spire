from spire.utils.databricks import check_run_state, check_currently_running

TERMINATED_STATUS_FAILED = {
    "job_id": 887432,
    "run_id": 1054771,
    "number_in_job": 1,
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "FAILED",
        "state_message": "",
    },
    "task": {
        "notebook_task": {
            "notebook_path": "/Production/spire/targets/aam",
            "base_parameters": {"date": "2020-07-06"},
        }
    },
    "cluster_spec": {
        "new_cluster": {
            "spark_version": "6.4.x-scala2.11",
            "aws_attributes": {
                "zone_id": "us-east-1b",
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "instance_profile_arn": "arn:aws:iam::802965631854:instance-profile/"
                "Databricks-EC2Role",
                "spot_bid_price_percent": 100,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_count": 3,
                "ebs_volume_size": 100,
            },
            "node_type_id": "i3.8xlarge",
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                "DB_HOSTNAME": "spire-db-prod.cys4lyo4wwr5.us-east-1.rds.amazonaws.com",
                "VAULT_READ_PATH": "secret/data-innovation/spire/production/db",
                "VAULT_ADDRESS": "https://prod.vault.conde.io:443",
                "SPIRE_ENVIRON": "cn-spire",
            },
            "enable_elastic_disk": False,
            "autoscale": {"min_workers": 1, "max_workers": 8},
        },
        "libraries": [
            {"pypi": {"package": "PyYaml"}},
            {"pypi": {"package": "mlflow"}},
            {"pypi": {"package": "hyperopt"}},
            {"pypi": {"package": "hvac"}},
            {"pypi": {"package": "croniter"}},
            {"pypi": {"package": "pendulum"}},
            {"pypi": {"package": "pymongo"}},
        ],
    },
    "cluster_instance": {
        "cluster_id": "0707-000044-skunk746",
        "spark_context_id": "3004692827536184697",
    },
    "start_time": 1594080043608,
    "setup_duration": 115000,
    "execution_duration": 128000,
    "cleanup_duration": 1000,
    "creator_user_name": "James_Evers@condenast.com",
    "run_name": "Untitled",
    "run_page_url": "https://dbc-f4a7a189-e72a.cloud.databricks.com#job/887432/run/1",
    "run_type": "SUBMIT_RUN",
}
TERMINATED_STATUS_SUCCESS = {
    "job_id": 901642,
    "run_id": 1072401,
    "number_in_job": 1,
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS",
        "state_message": "",
    },
    "task": {
        "notebook_task": {
            "notebook_path": "/Production/spire/targets/aam",
            "base_parameters": {"date": "2020-07-06"},
        }
    },
    "cluster_spec": {
        "new_cluster": {
            "spark_version": "6.4.x-scala2.11",
            "aws_attributes": {
                "zone_id": "us-east-1b",
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "instance_profile_arn": "arn:aws:iam::802965631854:instance-profile/"
                "Databricks-EC2Role",
                "spot_bid_price_percent": 100,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_count": 3,
                "ebs_volume_size": 100,
            },
            "node_type_id": "i3.8xlarge",
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                "DB_HOSTNAME": "spire-db-prod.cys4lyo4wwr5.us-east-1.rds.amazonaws.com",
                "VAULT_READ_PATH": "secret/data-innovation/spire/production/db",
                "VAULT_ADDRESS": "https://prod.vault.conde.io:443",
                "SPIRE_ENVIRON": "cn-spire",
            },
            "enable_elastic_disk": False,
            "autoscale": {"min_workers": 1, "max_workers": 8},
        },
        "libraries": [
            {"pypi": {"package": "PyYaml"}},
            {"pypi": {"package": "mlflow"}},
            {"pypi": {"package": "hyperopt"}},
            {"pypi": {"package": "hvac"}},
            {"pypi": {"package": "croniter"}},
            {"pypi": {"package": "pendulum"}},
            {"pypi": {"package": "pymongo"}},
        ],
    },
    "cluster_instance": {
        "cluster_id": "0711-215911-lager357",
        "spark_context_id": "1608911107421240454",
    },
    "start_time": 1594504750457,
    "setup_duration": 111000,
    "execution_duration": 2734000,
    "cleanup_duration": 14000,
    "creator_user_name": "James_Evers@condenast.com",
    "run_name": "Untitled",
    "run_page_url": "https://dbc-f4a7a189-e72a.cloud.databricks.com#job/901642/run/1",
    "run_type": "SUBMIT_RUN",
}
TERMINATED_RUNS = {
    1054771: {
        "result_state": "FAILED",
        "notebook_url": "https://dbc-f4a7a189-e72a.cloud.databricks.com#job/887432/run/1",
    },
    1054772: {
        "result_state": "FAILED",
        "notebook_url": "https://dbc-f4a7a189-e72a.cloud.databricks.com#job/887433/run/1",
    },
}
ONE_TERMINED_ONE_RUNNING = {
    1054771: {
        "result_state": "FAILED",
        "notebook_url": "https://dbc-f4a7a189-e72a.cloud.databricks.com#job/887432/run/1",
    },
    1072397: {
        "state": {"life_cycle_state": "RUNNING", "state_message": "In run"},
        "result_state": None,
        "notebook_url": "https://dbc-f4a7a189-e72a.cloud.databricks.com#job/901638/run/1",
    },
}


def test_check_run_status_returns_run_status():
    failed_run_state = check_run_state(1054771, run_status=TERMINATED_STATUS_FAILED)
    successful_run_state = check_run_state(
        1054772, run_status=TERMINATED_STATUS_SUCCESS
    )
    assert not any([None in failed_run_state.keys()])
    assert not any([None in successful_run_state.keys()])
    assert failed_run_state["result_state"] == "FAILED"
    assert successful_run_state["result_state"] == "SUCCESS"


def test_currently_running_helper_function():
    none_currently_running = check_currently_running(TERMINATED_RUNS)
    one_currently_running = check_currently_running(ONE_TERMINED_ONE_RUNNING)
    assert none_currently_running == False
    assert one_currently_running == True
