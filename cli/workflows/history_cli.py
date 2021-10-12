import click
import datetime
import yaml
from cli.utils import nested_dict_uuid_to_str
import spire
from spire.config import config


@click.group(
    help="Commands for querying reports and "
    "other higher-level information about "
    "the Spire models"
)
def history():
    pass


@history.command(help="Get reports for a workflow")
@click.help_option(help="takes id as input")
@click.option("--id", help="workflow id", required=True)
def get_workflow_report(id):
    report = spire.history.get_report(wf_id=id)
    click.echo(yaml.safe_dump(nested_dict_uuid_to_str(report)))


@history.command(help="Get stage response for a workflow")
@click.help_option(
    help="takes id and stage as mandatory options, "
    "and takes ignore-failures, arg_date, and "
    "as-of as optional options"
)
@click.option("--id", help="workflow id", required=True)
@click.option("--stage", help="assembly, training, scoring", required=True)
@click.option(
    "--ignore-failures",
    help="adding this flag will filter failed stages " "from the query",
    is_flag=True,
)
@click.option(
    "--arg-date", help="Date to query stage response. " "Cannot be used with as-of"
)
@click.option(
    "--as-of",
    help="Starting date to query stage response. " "Cannot be used with arg-date",
)
def get_stage_response(id, stage, **kwargs):
    ignore_failures = kwargs["ignore_failures"]
    arg_date = kwargs["arg_date"]
    as_of = kwargs["as_of"]
    response = spire.history.get_stage_response(
        wf_id=id,
        stage=stage,
        ignore_failures=ignore_failures,
        arg_date=arg_date,
        as_of=as_of,
    )
    click.echo(yaml.safe_dump(nested_dict_uuid_to_str(response)))


@history.command(help="Get failed workflows by stage and date")
@click.help_option(help="Takes stage and date, as options. " "Date defaults to today.")
@click.option("--stage", help="assembly, training, scoring")
@click.option("--date", default=datetime.date.today(), help="Optional. Date to query")
def get_failures(stage, date):
    click.echo(spire.history.get_failures(stage=stage, date=date))


@history.command(help="Get reports for all workflows")
@click.help_option(
    help="Defaults are staging=training, " "arg-date=None " "and as-of=None"
)
@click.option("--stage", help="assembly, training, scoring, or " "postprocessing")
@click.option("--arg-date", help="Date to query reports. Cannot be " "used with as-of")
@click.option(
    "--as-of", help="Starting date to query reports. " "Cannot be used with arg-date"
)
def get_reports(stage, arg_date=None, as_of=None):
    reports = spire.history.get_reports(stage=stage, arg_date=arg_date, as_of=as_of)
    [click.echo(yaml.safe_dump(nested_dict_uuid_to_str(report))) for report in reports]
    click.echo(f"These reports are from the {config.DEPLOYMENT_ENV} environment")


@history.command(
    help="Get most recent successful run date for" "the workflow at a given stage"
)
@click.option("--id", help="workflow id")
@click.option("--name", help="workflow id")
@click.option("--stage", help="assembly, training, scoring, or " "postprocessing")
def get_last_successsful_run_date(id, name, stage):
    result = spire.history.last_successful_run_date(wf_id=id, wf_name=name, stage=stage)
    click.echo(result["last_successful_run_date"].strftime("%Y-%m-%d"))


@history.command(
    help="Get most recent successful run date for" "the workflow at a given stage"
)
@click.option("--id", help="workflow id")
@click.option("--name", help="workflow id")
@click.option("--stage", help="assembly, training, scoring, or " "postprocessing")
@click.option(
    "--as-of",
    default=datetime.datetime.today(),
    help="Starting date to query stage response.",
)
@click.option("--seconds", default=0, help="seconds")
def get_recent_failure_reports(id, name, **kwargs):
    seconds = kwargs["seconds"]
    stage = kwargs["stage"]
    as_of = kwargs["as_of"]
    if as_of:
        as_of = datetime.datetime.strptime(as_of, "%Y-%m-%d").date()
    reports = spire.history.get_recent_failures(
        wf_id=id, wf_name=name, stage=stage, as_of=as_of, seconds=seconds
    )
    click.echo("reports")
    click.echo(reports)
    [click.echo(yaml.safe_dump(nested_dict_uuid_to_str(report))) for report in reports]
    click.echo(f"These reports are from the {config.DEPLOYMENT_ENV} environment")
