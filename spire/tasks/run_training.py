from spire.framework.workflows.workflow import WorkflowStages
from spire.utils.databricks import monitor_runs
from spire.framework.runners import (
    TrainingRunner,
)
from spire.utils import get_logger
from .run_stage import run_stage
from spire.utils.execution import execute_cli_runner

logger = get_logger(__name__)


def run_training_legacy(
    run_date, training_config, workflows=None, max_per_cluster=300, **kwargs
):
    run_name = training_config["job_submit_json"]["run_name"]
    runner_script = training_config["job_submit_json"]["notebook_path"]
    training_runner = TrainingRunner(
        cluster_config=training_config["new_cluster"],
        run_name=run_name,
        runner_script=runner_script,
    )
    if kwargs["cli"]:
        kwargs["workflows"] = workflows
        run_ids, workflow_ids = execute_cli_runner(run_date, training_runner, **kwargs)
        return run_ids, workflow_ids
    run_id, launched_workflows = training_runner.run(
        run_date, workflows, max_per_cluster=max_per_cluster
    )
    monitor_runs(run_id)
    return run_id, launched_workflows


def run_training(run_date):
    return run_stage(WorkflowStages.TRAINING, run_date)
