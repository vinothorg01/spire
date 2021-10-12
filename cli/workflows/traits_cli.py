import click
import csv
import yaml

import spire


@click.group(help="Commands for the Spire model traits")
def traits():
    pass


@traits.command(help="Get trait for a workflow")
@click.help_option(help="Get trait for a workflow. Takes id " "as mandatory option")
@click.option("--id", help="workflow id")
def get(id):
    result = spire.trait.get(wf_id=id)
    click.echo(yaml.safe_dump(result))


@traits.command(help="Update trait for a workflow")
@click.help_option(help="Update trait for a workflow. Takes id " "as mandatory option")
@click.option("--id", help="workflow id")
@click.option("--trait-id", help="new trait id")
def update(id, trait_id):
    result = spire.trait.update(wf_id=id, trait_id=trait_id)
    click.echo(yaml.safe_dump(result))


@traits.command(help="Update traits for a list of workflows")
@click.help_option(
    help="To get segments csv spreadsheet use "
    "query get-by-name-like --help command"
    "query get-by-name-description --help command"
    "fill trait_id column with trait_id"
    "in the exported segment csv, automatically"
    "batch_update command would take of parsing"
    "and updation of traits"
)
@click.option("--filepath", help="Location of csv spreadsheet", required=True)
def batch_update(filepath):
    reader = csv.DictReader(open(filepath, "r"))
    for row in reader:
        trait_id = row.get("trait_id", None)
        wf_id = row.get("id", None)
        if trait_id:
            result = spire.trait.update(wf_id=wf_id, trait_id=trait_id)
            click.echo(yaml.safe_dump(result))
            click.echo(f"Updated workflow ID {wf_id}")
        else:
            click.echo(f"No trait id defined for workflow ID {wf_id} ...")
