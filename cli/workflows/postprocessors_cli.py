import click
import yaml
import os

from cli.utils import nested_dict_uuid_to_str, list2dict

from spire.integrations.postgres import connector
from spire.framework.workflows import Workflow
from spire.framework.workflows.postprocessor.thresholder import (
    Postprocessor,
    Thresholder,
)


@click.group(help="Commands for the Spire model postprocessors")
def postprocessors():
    pass


@postprocessors.command(help="Get postprocessors for a workflow")
@click.help_option(
    help="Get postprocessors for a workflow. Takes " "workflow id as mandatory option"
)
@click.option("--id", help="workflow id")
def get_postprocessors(id):
    session = connector.make_session()
    workflow = session.query(Workflow).filter(Workflow.id == id).one()
    click.echo(
        yaml.safe_dump(
            [nested_dict_uuid_to_str(p.to_dict()) for p in workflow.postprocessors]
        )
    )


@postprocessors.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    help="Append postprocessor to a workflow",
)
@click.help_option(
    help="Append postprocessor to a workflow. "
    "Takes workflow id and postprocessor type "
    "as mandatory options, followed by all other "
    "options for a given postprocessor type"
)
@click.option("--id", help="workflow id")
@click.option(
    "--type",
    help="Thresholder: Takes strategy, thresholds, "
    "and buckets as arguments. Thresholds and "
    "buckets should each be a single string of "
    "comma-separated values",
)
@click.pass_context
def append_postprocessor(ctx, id, type):
    p_args = list2dict(ctx.args)
    p_args = dict([(k.strip("--"), v) for k, v in p_args.items()])
    try:
        session = connector.make_session()
        workflow = session.query(Workflow).filter(Workflow.id == id).one()
        if type == "Thresholder":
            p_proc = Thresholder(
                p_args["strategy"], p_args["threshold"], p_args["buckets"]
            )
        click.echo(yaml.safe_dump(nested_dict_uuid_to_str(p_proc.to_dict())))
        if click.confirm(
            f"Do you want to save these changes to " f"workflow {workflow.name}?"
        ):
            workflow.append_postprocessor(p_proc)
            session.commit()
        else:
            print("Aborting append-postprocessor")
    except Exception as e:
        session.rollback()
        click.echo(e)
    finally:
        session.close()


@postprocessors.command(help="Remove postprocessor from a workflow")
@click.help_option(
    help="Remove postprocessor from a workflow. "
    "Takes workflow id and postprocessor id "
    "as mandatory options"
)
@click.option("--id", help="workflow id")
@click.option("--postprocessor-id", help="postprocessor to remove")
def remove_postprocessor(id, postprocessor_id):
    session = connector.make_session()
    workflow = session.query(Workflow).filter(Workflow.id == id).one()
    try:
        p_proc = (
            session.query(Postprocessor)
            .filter(Postprocessor.id == postprocessor_id)
            .one()
        )
    except Exception as e:
        click.echo(e)
        return
    click.echo(yaml.safe_dump(nested_dict_uuid_to_str(p_proc.to_dict())))
    click.echo(f"removing postprocessor {p_proc.id} from " f"workflow {workflow.name}")
    if click.confirm(
        f"Do you want to save these changes to " f"workflow {workflow.name}?"
    ):
        try:
            workflow.remove_postprocessor(p_proc)
            session.commit()
        except Exception as e:
            session.rollback()
            click.echo(e)
        finally:
            session.close()
            return
    else:
        print("Aborting remove-postprocessor")
