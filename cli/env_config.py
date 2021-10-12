import os
import yaml
import click
from pathlib import Path
import hvac
from click import ClickException

os.environ["CLI_INSTANCE"] = "True"

ENV_DICT = {
    "development": {
        "DB_HOSTNAME": "",
        "VAULT_ADDRESS": "",
        "VAULT_TOKEN": "",
        "VAULT_READ_PATH": "",
        "DEPLOYMENT_ENV": "",
        "SPIRE_ENVIRON": "",
        "DATABRICKS_HOST": "",
        "DATABRICKS_TOKEN": "",
        "GHCR_USER": "",
        "GHCR_PASSWORD": "",
        "MLFLOW_TRACKING_URI": "databricks",
    },
    "staging": {
        "DB_HOSTNAME": "",
        "VAULT_ADDRESS": "",
        "VAULT_TOKEN": "",
        "VAULT_READ_PATH": "",
        "DEPLOYMENT_ENV": "",
        "SPIRE_ENVIRON": "",
        "DATABRICKS_HOST": "",
        "DATABRICKS_TOKEN": "",
        "GHCR_USER": "",
        "GHCR_PASSWORD": "",
        "MLFLOW_TRACKING_URI": "databricks",
    },
    "production": {
        "DB_HOSTNAME": "",
        "VAULT_ADDRESS": "",
        "VAULT_TOKEN": "",
        "VAULT_READ_PATH": "",
        "DEPLOYMENT_ENV": "",
        "SPIRE_ENVIRON": "",
        "DATABRICKS_HOST": "",
        "DATABRICKS_TOKEN": "",
        "GHCR_USER": "",
        "GHCR_PASSWORD": "",
        "MLFLOW_TRACKING_URI": "databricks",
    },
}


def make_directory(dir_path):
    if not os.path.exists(dir_path):
        try:
            os.mkdir(dir_path)
            return True
        except click.ClickException as e:
            click.echo(e)


def write_config(config, config_path):
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)
        click.echo("config.yml file created in ~/.spire/")
    return True


def prompt_user_env_info(config):
    environment = (
        click.prompt(
            "Please specify environment ex. development/staging/production", type=str
        )
        .strip()
        .lower()
    )
    if environment not in ["development", "staging", "production"]:
        raise ClickException(
            "Please provide specified environment ex. development/staging/production"
        )
    for key in config["environments"][environment].keys():
        config["environments"][environment][key] = click.prompt(
            f"Please speicify {key}", type=str
        )
    return config, environment


def init_cli_config(inner_func):
    config_dir = os.path.join(os.path.expanduser("~"), ".spire")
    make_directory(config_dir)
    config_path = os.path.join(os.path.expanduser("~/.spire/"), "config.yml")
    if not os.path.exists(config_path):
        try:
            Path(config_path).touch()
            config = {"active_env": "", "environments": ENV_DICT}
            write_config(config, config_path)
        except click.ClickException as e:
            Path(config_path).unlink()
            click.echo(e)
    return inner_func


@init_cli_config
def update_env_config():
    try:
        config_path = os.path.join(os.path.expanduser("~/.spire/"), "config.yml")
        with open(config_path) as f:
            config = yaml.safe_load(f)
        environment = config["active_env"]
        if not environment:
            config, environment = prompt_user_env_info(config)
            config["active_env"] = environment
            with open(config_path, "w") as f:
                yaml.dump(config, f)
        update_environ(config, environment)
        update_wrong_env(config_path, config)
    except Exception as e:
        print("inside update config env")
        print(e)


def update_environ(config, environment):
    config = config["environments"][environment]
    for key, val in config.items():
        if val:
            os.environ[key] = val
            click.echo(f"{key}: {val}")
        else:
            raise ClickException(f"{key} value is missing")


def update_wrong_env(config_path, config):
    try:
        client = hvac.Client(
            url=os.environ["VAULT_ADDRESS"], token=os.environ["VAULT_TOKEN"]
        )
        client.renew_token()
        client.read(os.environ["VAULT_READ_PATH"])["data"]
    except Exception as e:
        click.echo(e)
        click.echo("Error in env config, please redefine config")
        config, environment = prompt_user_env_info(config)
        config["active_env"] = environment
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        update_environ(config, environment)
        update_wrong_env(config_path, config)


update_env_config()
