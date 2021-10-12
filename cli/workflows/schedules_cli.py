import click
import yaml
import csv
import spire
import datetime


@click.group(help="Commands for the Spire model schedules")
def schedules():
    pass


@schedules.command(help="Get schedule for a workflow")
@click.help_option(
    help="Get schedule for a workflow. Takes id as "
    "mandatory option and stage as optional option"
)
@click.option("--id", help="workflow id")
@click.option("--name", help="workflow name")
@click.option("--stage", help="assembly, training, scoring")
def get(id, name=None, stage=None):
    result = spire.schedule.get(wf_id=id, wf_name=name)
    click.echo(yaml.dump({stage: result["args"][stage]} if stage else result))


@schedules.command(
    help="Update schedule for workflows in " "batch from a csv spreadsheet"
)
@click.help_option(
    help="To add/update/batch_update schedule use"
    "batch_update --filepath command"
    "To get segments csv spreadsheet use "
    "query get-by-name-like --help command"
    "query get-by-name-description --help command"
    "fill schedule column with valid schedule JSON"
    "in the exported segment csv, automatically"
    "batch_update command would take of parsing"
    "and updation of schedule"
)
@click.option("--filepath", help="Location of csv spreadsheet", required=True)
def batch_update(filepath):
    reader = csv.DictReader(open(filepath, "r"))
    click.echo("Process started, It may take while...")
    for row in reader:
        wf_id = row["id"]
        schedule_start_date = row.get("schedule_start_date", None)
        vendor = row.get("vendor", None)
        if schedule_start_date and vendor:
            schedule_date = datetime.datetime.strptime(schedule_start_date, "%Y-%m-%d")
            result = spire.schedule.update(
                wf_id=wf_id, schedule_date=schedule_date, vendor=vendor
            )
            click.echo(yaml.dump(result))
