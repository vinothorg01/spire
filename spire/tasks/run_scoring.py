from spire.framework.workflows.workflow import WorkflowStages
from spire.utils.databricks import monitor_runs

from spire.framework.runners import (
    ScoringRunner,
)
from spire.utils import get_logger
from .run_stage import run_stage
from spire.utils.execution import execute_cli_runner

logger = get_logger(__name__)


def run_scoring_legacy(run_date, config, **kwargs):
    run_name = config["job_submit_json"]["run_name"]
    runner_script = config["job_submit_json"]["notebook_path"]
    runner = ScoringRunner(
        cluster_config=config["new_cluster"],
        run_name=run_name,
        runner_script=runner_script,
    )
    runner.logger.info("scoring run_date= {}".format(run_date))
    if kwargs["cli"]:
        run_ids, workflow_ids = execute_cli_runner(run_date, runner, **kwargs)
        return run_ids, workflow_ids
    else:
        run_date = run_date.replace(hour=0)
        runner.logger.info("scoring modified run_date= {}".format(run_date))
        run_ids, workflow_ids = runner.run(run_date)
    monitor_runs(run_ids)
    return run_ids, workflow_ids


def run_scoring(run_date):
    return run_stage(WorkflowStages.SCORING, run_date)
