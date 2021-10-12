from spire.framework.workflows.workflow import WorkflowStages
from spire.utils.databricks import monitor_runs
from spire.utils.execution import execute_cli_runner
from spire.framework.runners import (
    AssemblyRunner,
)
from spire.utils import get_logger
from .run_stage import run_stage

logger = get_logger(__name__)


def run_assembly_legacy(
    run_date, assembly_config, return_successful_workflows_only=False, **kwargs
):

    # initialize runners
    run_name = assembly_config["job_submit_json"]["run_name"]
    runner_script = assembly_config["job_submit_json"]["notebook_path"]
    assembly_runner = AssemblyRunner(
        cluster_config=assembly_config["new_cluster"],
        run_name=run_name,
        runner_script=runner_script,
    )

    # launch assembly
    if kwargs["cli"]:
        run_id, workflow_ids = execute_cli_runner(run_date, assembly_runner, **kwargs)
        return run_id, workflow_ids
    else:
        run_ids, launched_wfs = assembly_runner.run(run_date)
        monitor_runs(run_ids)

    if return_successful_workflows_only:
        # remove any workflows that failed
        launched_wfs = assembly_runner.filter_failed_workflows(
            "assembly", run_date, launched_wfs
        )

    return run_ids, launched_wfs


def run_assembly(run_date):
    run_stage(WorkflowStages.ASSEMBLY, run_date)
