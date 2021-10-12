"""Spire environment CLI"""

import os
import yaml
import click
from cli.utils import read_config, validate_user_env_name
from spire.config import config
from click import ClickException
from spire.integrations import connector


@click.group(help="Commands for setting the Spire " "environment")
def environment():
    pass


@environment.command(help="activate environment")
@click.option(
    "--name",
    default="development",
    help="activate environment e.g.:\n" "development\n" "staging\n" "production\n",
)
@click.option("--session", default=False, hidden=True)
def activate(name, **kwargs):
    session = kwargs["session"]
    base_config, config_path = read_config()
    name = name.strip().lower()
    validate_user_env_name(name, base_config)
    env_config = base_config["environments"][name]
    for key, val in env_config.items():
        if val:
            os.environ[key] = val
            click.echo(f"{key}: {val}")
        else:
            raise ClickException(f"{key} value is missing")
    if not type(session).__name__ == "MagicMock":
        config.update()
        connector.refresh_session_factory()
    base_config["active_env"] = name
    with open(config_path, "w") as f:
        yaml.dump(base_config, f)
    click.echo(f"Activated {name} environment\n")


@environment.command(help="get active environment")
@click.option(
    "--name",
    help="get specific environment e.g.:\n" "development\n" "staging\n" "production\n",
)
def get(name=None):
    config, config_path = read_config()
    if name:
        name = name.strip().lower()
        validate_user_env_name(name, config)
    else:
        name = config["active_env"]
    click.echo(f"Getting {name} config \n")
    config = config["environments"][name]
    for key, val in config.items():
        click.echo(f"{key}: {val}")


@environment.command(help="list all environment")
def list():
    config, config_path = read_config()
    click.echo(yaml.safe_dump(config))


@environment.command(help="update environment information")
@click.option(
    "--name",
    required=True,
    help="update specific environment e.g.:\n"
    "development\n"
    "staging\n"
    "production\n",
)
@click.option("--db_hostname")
@click.option("--vault_address")
@click.option("--vault_token")
@click.option("--vault_read_path")
@click.option("--deployment_env")
@click.option("--spire_environ")
@click.option("--databricks_host")
@click.option("--databricks_token")
@click.option("--ghcr_user")
@click.option("--ghcr_password")
def update(name, **kwargs):
    config, config_path = read_config()
    name = name.strip().lower()
    validate_user_env_name(name, config)
    for key, val in kwargs.items():
        if kwargs[key]:
            config["environments"][name][key.upper()] = val
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    click.echo("Updated successfully")
