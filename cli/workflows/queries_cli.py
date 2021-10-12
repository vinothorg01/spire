import click
import os
import yaml
from cli.utils import (
    workflow_to_dict,
    create_csv,
)
from cli.env_config import make_directory
from cli.utils.workflow_utils import write_csv
from spire.framework.workflows import Workflow
from spire.integrations.postgres import connector

QUERY_EXPORT_FIELDS = [
    "id",
    "name",
    "description",
    "enabled",
    "vendor",
    "source",
    "groups",
    "dataset",
    "trait_id",
    "schedule_start_date",
    "schedule",
    "tags",
]


@click.group(
    help="Commands for querying reports and "
    "other higher-level information about "
    "the Spire models"
)
def queries():
    pass


@queries.command(help="Count of all enabled workflow ids in environment")
@click.option("--session", default=False, hidden=True)
def count_enabled(session=None):
    try:
        session = session or connector.make_session()
        workflows = Workflow.load_all_enabled(session)
        click.echo(len(workflows))
    except Exception as e:
        click.echo(e)
    finally:
        session.close()


@queries.command(help="Get workflow information by trait")
@click.help_option(help="takes trait-id as option")
@click.option("--trait-id", help="trait id", required=True)
@click.option("--session", default=False, hidden=True)
def get_by_trait(trait_id, session=None):
    session = session or connector.make_session()
    try:
        workflow = Workflow.get_by_trait(session, trait_id)
        click.echo(yaml.safe_dump(workflow_to_dict(workflow)))
    except Exception as e:
        click.echo(e)
        click.echo("Please provide valid trait id")
    finally:
        session.close()


@queries.command(help="Get workflow information by name")
@click.help_option(help="takes name as option")
@click.option("--name", help="workflow name", required=True)
@click.option("--session", default=False, hidden=True)
def get_by_name(name, session=None):
    session = session or connector.make_session()
    try:
        workflow = Workflow.get_by_name(session, name)
        click.echo(yaml.safe_dump(workflow_to_dict(workflow)))
    except Exception as e:
        click.echo(e)
        click.echo("Please provide valid workflow name")
    if type(session).__name__ == "MagicMock":
        session.workflow = workflow
        return session


@queries.command(
    help="Get list of information of workflows " "containing the name-like pattern"
)
@click.help_option(help="takes name-like as option")
@click.option("--name-like", help="name pattern of workflows", required=True)
@click.option("--export/--no-export", default=False, help="name pattern of workflows")
@click.option(
    "--filepath",
    help="Location of csv spreadsheet to export, if not provided"
    "defaults to ~/spire-cli/ (user home directory)",
)
@click.option("--session", default=False, hidden=True)
def get_by_name_like(name_like, export, filepath=None, session=None):
    session = session or connector.make_session()
    try:
        workflow = Workflow.get_by_name_like(session, name_like)
        if export:
            export_csv(workflow, filepath, "workflow_by_name.csv", QUERY_EXPORT_FIELDS)
        else:
            [click.echo(yaml.safe_dump(workflow_to_dict(data))) for data in workflow]
    except Exception as e:
        click.echo(e)
        click.echo("Please provide valid workflow name")
    finally:
        session.close()


def export_csv(workflow, filepath, filename, fieldnames):
    try:
        if not filepath:
            filepath = os.path.join(os.path.expanduser("~"), "spire-cli")
            make_directory(filepath)
        result = create_csv(filepath, fieldnames, filename)
        if result:
            write_csv(workflow, filepath + "/" + filename)
    except Exception as e:
        click.echo(e)


@queries.command(help="Get workflow information by description")
@click.help_option(help="takes description as option ,please provide it with in quotes")
@click.option("--description", help="workflow description", required=True)
@click.option("--session", default=False, hidden=True)
def get_by_description(description, session=None):
    session = session or connector.make_session()
    try:
        workflow = Workflow.get_by_description(session, description.lower())
        click.echo(yaml.safe_dump(workflow_to_dict(workflow)))
    except Exception as e:
        click.echo(e)
        click.echo("Please provide valid workflow description")
    finally:
        session.close()


@queries.command(
    help="Get list of information of worklows "
    "containing the description-like pattern"
)
@click.help_option(help="takes description-like as option")
@click.option(
    "--description-like", help="description pattern of workflows", required=True
)
@click.option("--export/--no-export", default=False, help="name pattern of workflows")
@click.option(
    "--filepath",
    help="Location of csv spreadsheet to export, if not provided"
    "defaults to ~/spire-cli/ (user home directory)",
)
@click.option("--session", default=False, hidden=True)
def get_by_description_like(description_like, export, filepath=None, session=None):
    session = session or connector.make_session()
    try:
        workflow = Workflow.get_by_description_like(session, description_like.lower())
        if export:
            export_csv(
                workflow, filepath, "workflow_by_description.csv", QUERY_EXPORT_FIELDS
            )
        else:
            [click.echo(yaml.safe_dump(workflow_to_dict(data))) for data in workflow]
    except Exception as e:
        click.echo(e)
        click.echo("Please provide valid workflow description")
    finally:
        session.close()
