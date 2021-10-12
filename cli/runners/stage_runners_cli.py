import click
from deprecation import deprecated
from sqlalchemy.orm.session import Session
from cli.utils import runners_utils as utils
from spire.framework.workflows import Workflow, WorkflowStages
from spire.utils.databricks import get_run_url
from spire.tasks.run_stage import run_stage
from spire.utils.general import get_docker_image
from spire.integrations.postgres import connector


@click.group(help="Commands for the Spire execute stage-runners")
def stage_runners():
    pass


@stage_runners.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Execute workflow",
)
@click.help_option(
    help="""Run stage tasks by providing workflow ids (wf-ids) or the
    file path for a csv of workflow ids. For wf-ids and specific-dates, enter them
    comma-separated. Note that filepath will supercede wf-ids if both are provided, and
    run-date will supercede specific-dates"""
)
@click.option("--task", required=True, help="Task name e.g. assembly/training/scoring")
@click.option(
    "--wf-ids", default=None, help="one or more (comma-separated) workflow ids"
)
@click.option(
    "--filepath",
    help="filepath to a csv of workflow ids with workflow_id as column name which can"
    "be used in place of wf-ids",
)
@click.option(
    "--dryrun/--no-dryrun",
    default=False,
    help="run the command without launching the task",
)
@click.option(
    "--run-date",
    default=None,
    help="""%YYYY-%mm-%dd for the run date for the task. Combine with --days-to-backfill
    to run over a range of dates""",
)
@click.option(
    "--days-to-backfill",
    default=1,
    help="Number of days from run-date for the date range to launch the task",
)
@click.option(
    "--specific-dates",
    default=None,
    help="""comma-separated list of dates in %YYYY-%mm-%dd format to launch the task
    for a specific set of dates rather than a date range""",
)
def run(
    task: str,
    filepath: str,
    dryrun,
    run_date: str,
    days_to_backfill: int,
    specific_dates: str,
    wf_ids: str = None,
):
    click.echo(f"Dry run is {dryrun}")
    dates = utils.get_dates(
        run_date=run_date,
        days_to_backfill=days_to_backfill,
        specific_dates=specific_dates,
    )
    stage = WorkflowStages(task)
    workflow_ids = utils.get_list_workflow_ids(wf_ids=wf_ids, filepath=filepath)
    job_statuses = run_stage(
        wf_ids=workflow_ids, stage=stage, run_dates=dates, dry_run=dryrun
    )
    for job_status in job_statuses:
        click.echo(job_status)


@stage_runners.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Execute workflow",
)
@click.help_option(
    help="Run a assembly/train/score tasks"
    "by providing a workflow id or csv file"
    "use dryrun to run the job in dry run mode"
    "otherwise use no-dryrun to run the job"
    "run-date is an execution date to run a job"
    "this can be combined with days-to-backfill command"
    "to run as backfill job for more than 1 day"
    "otherwise job runs for only one provided run date"
    "or use specific-dates when we have multiple specific dates to backfill"
    "provide multiple dates as comma separated"
    "backfill process triggers to those specfic dates"
)
@click.option("--id", default=None, help="provide workflow id")
@click.option("--task", required=True, help="Task name assembly/training/scoring")
@click.option(
    "--filepath",
    default=None,
    help="Provide segment csv filepath" "with workflow_id as column name",
)
@click.option(
    "--dryrun/--no-dryrun",
    default=False,
    help="enable runner to run in dry run mode",
    hidden=True,
)
@click.option(
    "--run-date",
    default=None,
    help="Provide a run date which is execution date to run a job"
    "this can be combined with days-to-backfill command"
    "to run as backfill job for given no of days",
)
@click.option(
    "--days-to-backfill", default=1, help="Provide a no of days to run the backfill job"
)
@click.option(
    "--specific-dates",
    default=None,
    help="when we have multiple specific dates to backfill"
    "provide multiple dates as comma separated"
    "backfill process triggers to those specfic dates",
)
@click.option("--spire-image", default=None, help="Spire docker image name")
@click.option("--spire-version", default=None, help="Spire docker image version")
@click.option("--session", default=None, hidden=True)
@deprecated(details="Please use the run command instead")
def run_legacy(
    id: str,
    task: str,
    filepath: str,
    dryrun: bool,
    run_date: str,
    days_to_backfill: int,
    specific_dates: str,
    spire_image: str,
    spire_version: str,
    session: Session,
):
    try:
        click.echo(f"Dry run is {dryrun}")
        session = session or connector.make_session()
        date_range = utils.get_dates(
            run_date=run_date,
            days_to_backfill=days_to_backfill,
            specific_dates=specific_dates,
        )
        workflow_ids = []
        docker_url = None
        if type(session).__name__ == "MagicMock":
            workflow_ids.append(id)
        elif filepath:
            workflow_ids.extend(utils.get_list_workflows(session, filepath))
        else:
            workflow_ids.append(session.query(Workflow).filter(Workflow.id == id).one())
        if spire_image and spire_version:
            docker_url = get_docker_image(spire_image, spire_version)
        for single_date in date_range:
            run_id, wfs_ids = utils.run_helper(
                single_date, workflow_ids, task, docker_url, dry_run=dryrun
            )
            run_data = run_id if dryrun else get_run_url(run_id[0])
            click.echo(run_data)
    except click.ClickException as e:
        click.echo(e)
