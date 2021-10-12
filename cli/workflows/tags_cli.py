import click
import csv
from cli.utils import get_workflow
from cli.utils.workflow_utils import update_tags, is_tag_exist, create_tag
from spire.integrations.postgres import connector


@click.group(help="Commands for the Spire model tags")
def tags():
    pass


@tags.command(help="Get tags for a workflow")
@click.help_option(help="Get tags for a workflow. Takes " "id as mandatory option")
@click.option("--id", help="workflow id")
@click.option("--session", default=False, hidden=True)
def get(id, session=None):
    try:
        session = session or connector.make_session()
        workflow = get_workflow(session, id, name=None)
        for tag in workflow.tags:
            click.echo(tag.label)
    except Exception as e:
        click.echo(e)
        click.echo("Please provide valid workflow")


@tags.command(help="Add tag to a workflow")
@click.help_option(
    help="Add tag to a workflow. " "Takes id and tag label " "as mandatory options"
)
@click.option("--id", help="workflow id")
@click.option("--tag-label", help="new tag")
@click.option("--session", default=False, hidden=True)
def add(id, tag_label, session=None):
    try:
        session = session or connector.make_session()
        workflow = get_workflow(session, id, name=None)
        tag_exist, tag = is_tag_exist(session, tag_label)
        tag = tag if tag_exist else create_tag(session, tag_label)
        click.echo(tag.label)
        if click.confirm(
            f"Do you want to save these changes to " f"workflow {workflow.name}?"
        ):
            session.add(tag)
            workflow.add_tag(tag)
            session.commit()
            click.echo(f"Tag {tag.label} added successfully")
        else:
            print("Aborting add-tag")
    except Exception as e:
        session.rollback()
        click.echo(e)
    finally:
        session.close()


@tags.command(help="Remove tag from a workflow")
@click.help_option(
    help="Remove tag from a workflow. " "Takes id and tag label " "as mandatory options"
)
@click.option("--id", help="workflow id")
@click.option("--tag-label", help="tag to remove")
@click.option("--session", default=False, hidden=True)
def remove(id, tag_label, session=None):
    try:
        session = session or connector.make_session()
        workflow = get_workflow(session, id, name=None)
        tag_exist, tag = is_tag_exist(session, tag_label)
        if tag_exist:
            click.echo(f"removing tag {tag.label} from " f"workflow {workflow.name}")
            if click.confirm(
                f"Do you want to save these changes to " f"workflow {workflow.name}?"
            ):
                workflow.remove_tag(tag)
                session.commit()
                click.echo(f"Tag {tag.label} removed successfully")
            else:
                print("Aborting remove-tag")
    except Exception as e:
        session.rollback()
        click.echo(e)
    finally:
        session.close()
        return


@tags.command(help="Update tags for workflows in " "batch from a csv spreadsheet")
@click.help_option(
    help="batch_update --filepath command"
    "To get segments csv spreadsheet use "
    "query get-by-name-like --help command"
    "query get-by-name-description --help command"
    "fill tags column with tags"
    "in the exported segment csv, automatically"
    "batch_update command would take of parsing"
    "and updation of tags"
)
@click.option("--filepath", help="Location of csv spreadsheet", required=True)
@click.option("--session", default=False, hidden=True)
def batch_update(filepath, session=None):
    reader = csv.DictReader(open(filepath, "r"))
    session = session or connector.make_session()
    try:
        for row in reader:
            session, workflow = update_tags(session, row)
        session.commit()
    except click.ClickException as e:
        session.rollback()
        click.echo(e)
    finally:
        session.close()
