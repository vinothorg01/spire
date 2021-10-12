import click
import yaml
import csv

import spire


@click.group(help="Commands for the Spire " "dataset definitions")
def datasets():
    pass


@datasets.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Updates the dataset definition of a workflow",
)
@click.help_option(
    help="Takes id, vendor, and source as mandatory "
    "options, and any number of additional options "
    "as needed for the dataset definition"
)
@click.option("--id", required=True)
@click.option("--vendor")
@click.option("--source")
@click.option("--groups")
def update(id, ctx=None, **kwargs):
    result = spire.dataset.update(
        wf_id=id,
        vendor=kwargs["vendor"],
        source=kwargs["source"],
        groups=kwargs["groups"],
    )
    click.echo(yaml.safe_dump(result))
    click.echo("Dataset updated successfully")


@datasets.command(help="Get dataset for a workflow")
@click.help_option(help="Get dataset for a workflow")
@click.option("--id", help="workflow id", required=True)
def get(id):
    try:
        result = spire.dataset.get(wf_id=id)
        click.echo(yaml.safe_dump(result))
    except Exception as e:
        click.echo(e)
        click.echo("Plese provide valid workfow id")


@datasets.command(
    help="Update dataset definitions for workflows in " "batch from a csv spreadsheet"
)
@click.help_option(
    help="Parse segments csv spreadsheet to automatically "
    "update dataset definitions for workflows in batch. "
    "Assumes columns for (workflow) id, vendor, groups, "
    "and source, in that order. This command currently "
    "cannot batch update datasets with more complex "
    "definitions."
)
@click.option("--filepath", help="Location of csv spreadsheet", required=True)
def batch_update(filepath):
    reader = csv.DictReader(open(filepath, "r"))
    try:
        for row in reader:
            result = spire.dataset.update(
                wf_id=row["id"],
                vendor=row["vendor"],
                source=row["source"],
                groups=row["groups"],
            )
            click.echo(yaml.safe_dump(result))
            click.echo(f"Workflow {row['id']} Dataset updated successfully")
    except click.ClickException as e:
        click.echo(e)
