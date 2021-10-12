import click
import yaml

from cli.utils import nested_dict_uuid_to_str
import spire


@click.group(help="Commands for the Spire model clusterstatuses")
def clusterstatuses():
    pass


@clusterstatuses.command(help="Get clusterstatuses for a workflow")
@click.help_option(
    help="Get clusterstatuses for a workflow. Takes id as "
    "mandatory option and stage as optional option"
)
@click.option("--id", help="workflow id")
@click.option("--stage", help="assembly, training, scoring")
def get(id, stage=None):
    result = spire.cluster_status.get(wf_id=id)
    click.echo(
        yaml.safe_dump(
            nested_dict_uuid_to_str(result[f"{stage}_status"])
            if stage
            else nested_dict_uuid_to_str(result)
        )
    )
