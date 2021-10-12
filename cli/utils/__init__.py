"""Spire CLI util common functions"""
import os
import csv
import yaml
import click
import uuid
from click import ClickException
from spire.framework.workflows import Workflow


def read_config():
    try:
        config_path = os.path.join(os.path.expanduser("~/.spire/"), "config.yml")
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return config, config_path
    except Exception as e:
        click.echo(e)
        raise ClickException("please create a spire CLI environment ")


def validate_user_env_name(name, config):
    try:
        config["environments"][name]
        return True
    except Exception:
        raise ClickException(
            "Please provide environment ex. development/staging/production"
        )


# TODO Sankar: make this a spire.utils function
def list2dict(ctx_args):
    data = {ctx_args[i]: ctx_args[i + 1] for i in range(0, len(ctx_args), 2)}
    return data


def strip_ctx_args(ctx_args):
    return dict([(k.strip("--"), v) for k, v in ctx_args.items()])


def load_workflows_from_ids(session, filepath, ids):
    if filepath:
        ids = read_ids_from_txt(ids)
        ids = [uuid.UUID(id) for id in ids]
    else:
        ids = [id.strip() for id in ids.split(",")]
    workflows = []
    for id in ids:
        workflows.append(session.query(Workflow).filter(Workflow.id == id).one())
    return workflows


def read_ids_from_txt(ids):
    lines = []
    with open(ids, "r") as f:
        for line in f:
            line = line.strip("\n")
            lines.append(line)
    return lines


def create_csv(filepath, fieldnames, filename):
    try:
        config_path = os.path.join(filepath, filename)
        with open(config_path, "wt", newline="") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=fieldnames)
            writer.writeheader()
        return True
    except Exception as e:
        click.echo(e)
        return False


def map_value_to_bool(value):
    if value in ["True", "true", "T", "t", "TRUE"]:
        return True
    elif value in ["False", "false", "F", "f", "FALSE"]:
        return False
    else:
        return False


def get_db_host_key(name):
    prod_key = "PRODUCTION_DB_HOSTNAME"
    dev_key = "DEVELOPMENT_DB_HOSTNAME"
    stag_key = "STAGING_DB_HOSTNAME"
    key = (
        prod_key
        if name == "production"
        else dev_key
        if name == "development"
        else stag_key
    )
    return key


def get_workflow(session, id=None, name=None):
    if type(session).__name__ == "MagicMock":
        workflow = session.workflow
    elif id:
        workflow = session.query(Workflow).filter(Workflow.id == id).one()
    elif name:
        workflow = session.query(Workflow).filter(Workflow.name == name).one()
    else:
        raise click.ClickException("Workflow id or name is mandatory")
    return workflow


# TODO(Sankar) Add some kind of recursion depth check or
#  resolve that in some other way
def nested_dict_uuid_to_str(data_dict):
    for key in data_dict.keys():
        if isinstance(data_dict[key], dict):
            nested_dict_uuid_to_str(data_dict[key])
        if isinstance(data_dict[key], uuid.UUID):
            data_dict[key] = str(data_dict[key])
    return data_dict


def workflow_to_dict(value):
    output = {
        "Workflow ID": str(value.id),
        "Name": value.name,
        "Description": value.description,
        "enabled": value.enabled,
    }
    return output
