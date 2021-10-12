import yaml
import datetime
import csv
from deprecation import deprecated
from typing import List
import spire
from spire.tasks import run_assembly_legacy, run_training_legacy, run_scoring_legacy
from spire.config import config
from spire.integrations.postgres import connector
from spire.framework.workflows import Workflow


# TODO(Max): Remove these after removing deprecating functions
ASSEMBLY_TASK_ID = "assembly"
TRAINING_TASK_ID = "training"
SCORING_TASK_ID = "scoring"


def get_dates(
    run_date: str = None, days_to_backfill: int = None, specific_dates: str = None
) -> List[datetime.date]:
    if run_date and specific_dates:
        message = "the arguments run-date and specific-dates can't be used together"
        raise Exception(message)
    if run_date:
        run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
        dates = [run_date - datetime.timedelta(days=x) for x in range(days_to_backfill)]
    elif specific_dates:
        dates = [
            datetime.datetime.strptime(val, "%Y-%m-%d").date()
            for val in specific_dates.split(",")
        ]
    return dates


def get_list_workflow_ids(wf_ids: str = None, filepath: str = None) -> List[str]:
    if wf_ids and filepath:
        message = "the arguments wf-ids and filepath can't be used together"
        raise Exception(message)
    if wf_ids:
        return parse_wf_ids_list(wf_ids)
    if filepath:
        return get_wf_ids_from_csv(filepath)


def parse_wf_ids_list(wf_ids: str) -> List[str]:
    wf_ids_list = []
    for wf_id in wf_ids.split(","):
        wf = spire.workflow.get(wf_id=wf_id)
        wf_ids_list.append(wf["id"])
    return wf_ids_list


def get_wf_ids_from_csv(filepath: str) -> List[str]:
    wf_ids_list = []
    reader = csv.DictReader(open(filepath, "r"))
    for data in reader:
        wf = spire.workflow.get(wf_id=data["workflow_id"])
        wf_ids_list.append(wf["id"])
    return wf_ids_list


@deprecated(details="This is only used by deprecated stage_runners run_legacy")
@connector.session_transaction
def get_list_workflows(filepath, session=None):
    reader = csv.DictReader(open(filepath, "r"))
    wfs = []
    for data in reader:
        workflow = (
            session.query(Workflow).filter(Workflow.id == data["workflow_id"]).one()
        )
        wfs.append(workflow)
    return wfs


@deprecated(details="This is only used by deprecated stage_runners run_legacy")
def load_yaml_config(path, task_id):
    with open(path) as f:
        return yaml.safe_load(f)[task_id]


@deprecated(details="This is only used by deprecated stage_runners run_legacy")
def load_task_config(stage_id):
    """
    Load Databricks job config based on environment
    and whether the stage is an orchestration task
    """
    orchestration_stage_ids = [
        ASSEMBLY_TASK_ID,
        TRAINING_TASK_ID,
        SCORING_TASK_ID,
    ]
    env = config.DEPLOYMENT_ENV
    if env in ["default", "test"]:
        env = "development"
    if stage_id in orchestration_stage_ids:
        config_path = f"include/spire-astronomer/include/cfg/{env}/orchestration.yml"
    return load_yaml_config(config_path, stage_id)


@deprecated(details="This is only used by deprecated stage_runners run_legacy")
def run_helper(run_date, wfs, task, docker_url=None, dry_run=True):
    kwargs = {
        "workflows": wfs,
        "docker_url": docker_url,
        "dry_run": dry_run,
        "cli": True,
    }
    if task == "assembly":
        config = load_task_config(ASSEMBLY_TASK_ID)
        run_id, workflow_ids = run_assembly_legacy(run_date, config, **kwargs)
    elif task == "training":
        config = load_task_config(TRAINING_TASK_ID)
        run_id, workflow_ids = run_training_legacy(run_date, config, **kwargs)
    elif task == "scoring":
        config = load_task_config(SCORING_TASK_ID)
        run_id, workflow_ids = run_scoring_legacy(run_date, config, **kwargs)
    return run_id, workflow_ids
