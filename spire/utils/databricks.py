import time
import json
import requests
import datetime
from base64 import standard_b64encode
from spire.utils import constants
from spire.config import config as spire_config
from spire.utils.logger import get_logger

logger = get_logger()


def submit_job(config, run_date, **kwargs):
    job_json = prepare_job_json(
        config,
        run_date,
        workflows=kwargs.get("workflows", None),
        vendor=kwargs.get("vendor", None),
    )
    return post_to_databricks(job_json)


def prepare_job_json(config, run_date, workflows=None, vendor=None):
    if workflows:
        job_json = {
            "run_name": config["job_submit_json"]["run_name"],
            "timeout_seconds": config["timeout_seconds"],
            "new_cluster": config["new_cluster"],
            "notebook_task": {
                "notebook_path": config["job_submit_json"]["notebook_path"],
                "base_parameters": {
                    "workflow_ids": json.dumps(workflows),
                    "date": datetime.datetime.strftime(run_date, "%Y-%m-%d"),
                    "vendor": str(vendor),
                    "threadpool_group_size": config["threadpool_group_size"],
                },
            },
        }
    else:
        job_json = {
            "run_name": config["run_name"],
            "new_cluster": config["cluster_configuration"],
            "notebook_task": {
                "notebook_path": config["notebook_path"],
                "base_parameters": {
                    "date": datetime.datetime.strftime(run_date, "%Y-%m-%d"),
                },
            },
        }
    job_json["new_cluster"]["spark_env_vars"]["VAULT_TOKEN"] = spire_config.VAULT_TOKEN
    return json.dumps(job_json)


def get_run_url(run_id):
    run_state = get_run_status(run_id)
    return "{url}{run_id}".format(
        url=constants.DATABRICKS_URL, run_id=run_state["run_page_url"].split("#")[-1]
    )


def collect_run_statuses(run_ids):
    run_results = {}
    for run_id in run_ids:
        current_status = check_run_state(run_id)
        run_results[run_id] = current_status
    return run_results


def check_currently_running(run_states):
    if any(v["result_state"] is None for v in run_states.values()):
        return True
    return False


def display_run_states(run_statuses):
    for run_id, run_status in run_statuses.items():
        display_run_state(run_id, run_status)
        print()
    return


def display_run_state(run_id, run_status):
    if not run_status["result_state"]:
        life_cycle_state = run_status["state"]
        notebook_url = run_status["notebook_url"]
        logger.info(f"{run_id}: {life_cycle_state}")
        if notebook_url:
            logger.info(f"{run_id}: {notebook_url}")
        print()
    return


def monitor_runs(run_ids, sleep_time=120):
    if isinstance(run_ids, int):
        run_ids = [run_ids]
    if not isinstance(run_ids, list):
        raise TypeError(
            """A single run id or a list of run ids are
                        the only valid arguments -- received type {}
                        """.format(
                type(run_ids)
            )
        )
    run_statuses = collect_run_statuses(run_ids)
    currently_running = check_currently_running(run_statuses)
    while currently_running:
        display_run_states(run_statuses)
        run_statuses, currently_running = collect_and_check_runs(run_ids)
        if not currently_running:
            break
        time.sleep(sleep_time)
    return run_statuses


def collect_and_check_runs(run_ids):
    run_statuses = collect_run_statuses(run_ids)
    currently_running = check_currently_running(run_statuses)
    return run_statuses, currently_running


def monitor_run(run_id):
    run_page_url = get_run_url(run_id)
    logger.info(f"View run status, Spark UI, and logs at {run_page_url}")
    run_status = get_run_status(run_id)
    currently_running = run_status["result_state"] is None
    while currently_running:
        try:
            life_cycle_state = run_status["state"]["life_cycle_state"]
            logger.info(f"{run_id} in run state: {life_cycle_state}")
            logger.info(
                f"View run status, Spark UI, and logs at {run_page_url}"
            )  # noqa
            print()
            if life_cycle_state == constants.RUN_TERMINATED:
                break
            run_status = get_run_status(run_id)
            time.sleep(constants.POLLING_PERIOD_INTERVAL)
        except ConnectionRefusedError:
            time.sleep(constants.POLLING_PERIOD_INTERVAL)
    terminal_state = run_status["result_state"]
    if terminal_state != constants.RUN_SUCCESS:
        e = f"Run {run_id} failed with terminal state: {terminal_state}"
        raise RuntimeError(e)
    return run_status


def check_run_state(run_id, run_status=None):
    if not run_status:
        run_status = get_run_status(run_id)
    run_state = run_status["state"]
    logger.info("run id {}: {}".format(run_id, run_state))
    if "result_state" in run_state:
        return {
            "result_state": run_state["result_state"],
            "notebook_url": run_status["run_page_url"],
        }
    return {
        "state": run_status["state"],
        "result_state": None,
        "notebook_url": run_status.get("run_page_url", None),
    }


def get_cluster_id(run_id):
    sleep_time = constants.INTER_API_CALL_SLEEP_TIME
    for i in range(0, constants.API_CALL_TRY_COUNT):
        try:
            run_state = get_run_status(run_id)
            cluster_instance = run_state["cluster_instance"]
        except KeyError:
            logger.info("cluster not yet available!")
            time.sleep(sleep_time)
            sleep_time = sleep_time * 2
    return cluster_instance["cluster_id"]


def get_databricks_auth_headers():
    AUTH_ENCODING = b"Basic " + standard_b64encode(
        f"token:{spire_config.DATABRICKS_TOKEN}".encode("ascii")
    )
    DATABRICKS_AUTH_HEADERS = {"Authorization": AUTH_ENCODING}
    return DATABRICKS_AUTH_HEADERS


def get_cluster_state(cluster_id):
    cluster_data = json.dumps({"cluster_id": cluster_id})
    response = requests.get(
        spire_config.DATABRICKS_HOST.encode("utf-8") + constants.GET_CLUSTERS_API,
        headers=get_databricks_auth_headers(),
        data=cluster_data,
    )
    if response.ok:
        return response.json()["state"]
    else:
        raise requests.exceptions.RequestException


def get_run_status(run_id):
    run_data = json.dumps({"run_id": run_id})
    response = requests.get(
        spire_config.DATABRICKS_HOST.encode("utf-8") + constants.RUNS_STATUS_API,
        headers=get_databricks_auth_headers(),
        data=run_data,
    )
    if response.ok:
        return response.json()
    else:
        print(response.json())
        raise requests.exceptions.RequestException


def start_cluster(cluster_name):
    cluster_id = cluster_name["cluster_id"]
    start_cluster_json = json.dumps({"cluster_id": cluster_id})
    response = requests.post(
        spire_config.DATABRICKS_HOST.encode("utf-8") + constants.START_CLUSTER_API,
        headers=get_databricks_auth_headers(),
        data=start_cluster_json,
    )
    if response.ok:
        return cluster_id
    else:
        print(response.json())
        raise requests.exceptions.RequestException


def list_clusters():
    response = requests.get(
        spire_config.DATABRICKS_HOST.encode("utf-8") + constants.LIST_CLUSTERS_API,
        headers=get_databricks_auth_headers(),
    )
    if response.ok:
        data = response.json()
        return data["clusters"]
    else:
        raise requests.exceptions.RequestException


def post_to_databricks(job_json):
    response = requests.post(
        spire_config.DATABRICKS_HOST.encode("utf-8") + constants.SUBMIT_RUN_API,
        headers=get_databricks_auth_headers(),
        data=job_json,
    )
    if response.ok:
        return response.json()["run_id"]
    else:
        print(response.json())
        raise requests.exceptions.RequestException
