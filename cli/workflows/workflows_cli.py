import click
import os
import csv
import datetime
import yaml
from cli.utils import create_csv, map_value_to_bool
from cli.env_config import make_directory
import spire
from spire.tasks.run_stage import run_stage
from spire.framework.workflows.workflow_stages import WorkflowStages


@click.group(help="Commands for the Spire workflow models")
def workflows():
    pass


@workflows.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Create workflow",
)
@click.help_option(
    help="Create a new workflow. Takes name, description, "
    "trait-id, vendor, source, groups, default-schedule / "
    "no-default-schedule, and enabled / no-enabled as "
    "options. Only name and description are mandatory."
    "vendor, source, and groups are used to attach a "
    "dataset to the workflow. Options for a typical "
    "datasets may also be included. Trait-id attaches a "
    "trait to the workflow. The schedule and enabled "
    "options are flags which default to true and false "
    "respectively. Other attributes need to be added or "
    "updated separately."
)
@click.option("--name", help="workflow name", required=True)
@click.option("--description", help="workflow description", required=True)
@click.option("--trait-id", help="workflow trait id")
@click.option("--vendor", help="data vendor (for dataset creation)")
@click.option("--source", help="data source (for dataset creation)")
@click.option("--groups", help="data groups (for dataset creation)")
@click.option(
    "--enabled/--no-enabled", default=False, help="enable workflow in scheduler"
)
def create(ctx=None, **kwargs):
    if click.confirm(
        "Do you want to create this workflow?\n"
        "NOTE: This workflow must be enabled and have a "
        "dataset and trait attached to it before it can "
        "be processed automatically in the "
        "spire pipeline"
    ):
        kwargs["schedule"] = {}
        kwargs["groups"] = kwargs["groups"].split(",") if kwargs["groups"] else []
        result = spire.workflow.create(**kwargs)
        if result:
            click.echo(f"Workflow {result['id']} was created")
    else:
        click.echo("Workflow create aborted!")


@workflows.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Get a specific workflow",
)
@click.option("--id", help="workflow id")
@click.option("--name", help="workflow name")
def get(id=None, name=None):
    result = spire.workflow.get(wf_id=id, wf_name=name)
    result["id"] = str(result["id"])
    click.echo(yaml.safe_dump(result))


@workflows.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Update attributes of a workflow",
)
@click.option("--id", help="workflow id")
@click.option("--name", help="workflow name")
@click.option("--description", help="workflow description")
@click.option(
    "--enabled/--no-enabled", default=True, help="enable workflow in scheduler"
)
def update(id, **kwargs):
    if click.confirm("Do you want to save these changes to " "workflow?"):
        data = {key: val for key, val in kwargs.items() if val is not None}
        result = spire.workflow.update(
            wf_id=id, wf_name=kwargs["name"], definition=data
        )
        result["id"] = str(result["id"])
        click.echo("updated successfully")
        click.echo(yaml.safe_dump(result))
    else:
        click.echo("Aborting update-workflow")


@workflows.command(help="Export update workflows csv spreadsheet")
@click.option(
    "--filepath",
    help="Location of csv spreadsheet to export, if not provided"
    "defaults to ~/spire-cli/ (user home directory)",
)
def export_batch_update_csv(filepath=None):
    try:
        if filepath:
            config_dir = filepath
        else:
            config_dir = os.path.join(os.path.expanduser("~"), "spire-cli")
            make_directory(config_dir)
        fieldnames = [
            "id",
            "name",
            "description",
            "enabled",
            "vendor",
            "source",
            "groups",
            "dataset",
            "trait_id",
            "schedule",
            "tags",
        ]
        result = create_csv(config_dir, fieldnames, "workflow_update.csv")
        if result:
            click.echo(
                f"workflow_update.csv file exported to {config_dir} successfully"
            )
        else:
            click.echo("Please check the export path")
    except Exception as e:
        click.echo(e)


@workflows.command(help="Update workflows in batch from csv")
@click.help_option(
    help="Download the segment csv by query cli"
    "use query get-by-name-like --help command"
    "use query get-by-name-description --help command"
    "if you want fill data manually then get csv by"
    "export-batch-update-csv sample structure csv"
    "Parse segments csv spreadsheet to automatically "
    "update workflows in batch"
)
@click.option("--filepath", help="Location of csv spreadsheet to export")
def batch_update(filepath):
    reader = csv.DictReader(open(filepath, "r"))
    for row in reader:
        wf_id = row["id"]
        update_workflow_attributes(wf_id, row)
        update_trait(wf_id, row)
        update_dataset(wf_id, row)
        update_schedule(wf_id, row)
        click.echo(f"Workflow {wf_id} updated successfully")


def update_workflow_attributes(workflow_id, data):
    enabled = map_value_to_bool(data["enabled"])
    workflow_update = {
        "enabled": enabled,
        "description": data["description"],
    }
    workflow_update_result = spire.workflow.update(
        wf_id=workflow_id, definition=workflow_update
    )
    workflow_update_result["id"] = str(workflow_update_result["id"])
    click.echo(yaml.safe_dump(workflow_update_result))


def update_trait(workflow_id, data):
    trait_id = data.get("trait_id", None)
    if trait_id:
        trait_update_result = spire.trait.update(wf_id=workflow_id, trait_id=trait_id)
        click.echo(yaml.safe_dump(trait_update_result))


def update_dataset(workflow_id, data):
    vendor = data.get("vendor", None)
    if vendor:
        dataset_update_result = spire.dataset.update(
            wf_id=workflow_id,
            vendor=vendor,
            source=data["source"],
            groups=data["groups"],
        )
        click.echo(yaml.safe_dump(dataset_update_result))


def update_schedule(workflow_id, data):
    vendor = data.get("vendor", None)
    schedule_start_date = data.get("schedule_start_date", None)
    if vendor and schedule_start_date:
        schedule_start_date = datetime.datetime.strptime(
            schedule_start_date, "%Y-%m-%d"
        )
        schedule_update_result = spire.schedule.update(
            wf_id=workflow_id, schedule_date=schedule_start_date, vendor=vendor
        )
        click.echo(yaml.safe_dump(schedule_update_result))


@workflows.command(help="Export create workflows csv spreadsheet")
@click.option(
    "--filepath",
    help="Location of csv spreadsheet to export, if not provided"
    "defaults to ~/spire-cli/ (user home directory)",
)
def export_batch_create_csv(filepath=None):
    if filepath:
        config_dir = filepath
    else:
        config_dir = os.path.join(os.path.expanduser("~"), "spire-cli")
        make_directory(config_dir)
    fieldnames = [
        "name",
        "description",
        "enabled",
        "vendor",
        "source",
        "groups",
        "trait_id",
    ]
    result = create_csv(config_dir, fieldnames, "workflow_create.csv")
    if result:
        click.echo(f"workflow_create.csv file exported to {config_dir} successfully")
    else:
        click.echo("Please check the export path")


@workflows.command(help="Create workflows in batch from csv spreadsheet")
@click.help_option(
    help="Download the segment csv by export_batch_create_csv command "
    "Parse segments csv spreadsheet to automatically create new workflows"
    "in batch. Assumes columns for name, description, groups, trait_id"
    "vendor, source, default_schedule, enabled in that order, with header."
    " Does not support creation of datasets which require parameters other than"
    "of datasets which require parameters other than vendor and source. "
    " For datasets with multiple groups, comma-separate them within "
    "groups, comma-separate them within the same tab-"
    "the same cell of the csv."
)
@click.option("--filepath", help="Location of csv spreadsheet", required=True)
def batch_create(filepath):
    reader = csv.DictReader(open(filepath, "r"))
    for row in reader:
        row["enabled"] = map_value_to_bool(row["enabled"])
        row["schedule"] = {}
        row["groups"] = row["groups"].split(",") if row["groups"] else []
        result = spire.workflow.create(**row)
        result["id"] = str(result["id"])
        if result:
            click.echo(f"Workflow {result['id']} was created")


@workflows.command(help="Run a specific workflow stage given the workflow id")
@click.option("--workflow_id")
@click.option("--stage")
@click.option("--run_date", default=lambda: str(datetime.date.today()))
def run_stage_by_id(workflow_id, stage, run_date):
    stage = WorkflowStages(stage)
    run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
    run_stage(stage, run_date, wf_ids=[workflow_id])
