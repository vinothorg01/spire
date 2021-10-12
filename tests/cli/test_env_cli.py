from click.testing import CliRunner
from cli.utils.env_cli import environment
from cli import cli
from unittest.mock import MagicMock
from pathlib import Path
import os

# Unit test


def test_init_config():
    runner = CliRunner()
    environment = "development"
    DATABRICKS_HOST = "DATABRICKS_HOST"
    DATABRICKS_TOKEN = "DATABRICKS_TOKEN"
    DB_HOSTNAME = "DB_HOSTNAME"
    DEPLOYMENT_ENV = "DEPLOYMENT_ENV"
    SPIRE_ENVIRON = "SPIRE_ENVIRON"
    VAULT_ADDRESS = "VAULT_ADDRESS"
    VAULT_READ_PATH = "VAULT_READ_PATH"
    VAULT_TOKEN = "VAULT_TOKEN"
    GHCR_USER = "GHCR_USER"
    GHCR_PASSWORD = "1234"
    result = runner.invoke(
        cli,
        input="\n".join(
            [
                environment,
                DATABRICKS_HOST,
                DATABRICKS_TOKEN,
                DB_HOSTNAME,
                DEPLOYMENT_ENV,
                SPIRE_ENVIRON,
                VAULT_ADDRESS,
                VAULT_READ_PATH,
                VAULT_TOKEN,
                GHCR_USER,
                GHCR_PASSWORD,
            ]
        ),
    )
    assert "Starting Spire CLI interactive shell.." in result.output
    assert "Type `--help`, or `help` to see the commands list" in result.output
    assert "spire >" in result.output
    assert result.exit_code == 0


def test_env_cli_update_all():
    # setup
    runner = CliRunner()
    update_args = [
        "update",
        "--name",
        "development",
        "--databricks_host",
        "databricks_host",
        "--databricks_token",
        "databricks_token",
        "--spire_environ",
        "spire_environ",
        "--deployment_env",
        "deployment_env",
        "--vault_read_path",
        "vault_read_path",
        "--vault_token",
        "vault_token",
        "--vault_address",
        "vault_address",
        "--db_hostname",
        "hostname",
        "--ghcr_user",
        "ghcp_user",
        "--ghcr_password",
        "ghcp_password",
    ]
    get_args = ["get", "--name", "development"]
    # execution
    result = runner.invoke(environment, update_args)
    print("result")
    print(result.output)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)

    # assert
    assert "DATABRICKS_HOST: databricks_host" in result.output
    assert "DATABRICKS_TOKEN: databricks_token" in result.output
    assert "SPIRE_ENVIRON: spire_environ" in result.output
    assert "DEPLOYMENT_ENV: deployment_env" in result.output
    assert "VAULT_READ_PATH: vault_read_path" in result.output
    assert "VAULT_TOKEN: vault_token" in result.output
    assert "VAULT_ADDRESS: vault_address" in result.output
    assert result.exit_code == 0


def test_env_cli_get_default():
    # setup
    runner = CliRunner()
    args = ["get"]
    # execution
    result = runner.invoke(environment, args)
    # assert
    print(result.output)
    assert result.exit_code == 1


def test_env_cli_get_development():
    # setup
    runner = CliRunner()
    args = ["get", "--name", "development"]
    # execution
    result = runner.invoke(environment, args)

    # assert
    assert result.exit_code == 0


def test_env_cli_get_production():
    # setup
    runner = CliRunner()
    args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, args)

    # assert
    assert result.exit_code == 0


def test_env_cli_get_staging():
    # setup
    runner = CliRunner()
    args = ["get", "--name", "staging"]
    # execution
    result = runner.invoke(environment, args)

    # assert
    assert result.exit_code == 0


def test_env_cli_get_wrong_name():
    # setup
    runner = CliRunner()
    args = ["get", "--name", "dev"]
    # execution
    result = runner.invoke(environment, args)

    # assert
    assert (
        result.output
        == "Error: Please provide environment ex. development/staging/production\n"
    )


def test_env_cli_get_wrong_option():
    # setup
    runner = CliRunner()
    args = ["get", "--n", "production"]
    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 2


def test_env_cli_wrong_get_command():
    # setup
    runner = CliRunner()
    args = ["ge", "--name", "production"]

    # execution
    result = runner.invoke(environment, args)

    # assert
    assert result.exit_code == 2


def test_env_cli_list():
    # setup
    runner = CliRunner()
    args = ["list"]

    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 0


def test_env_cli_wrong_list_command():
    # setup
    runner = CliRunner()
    args = ["li"]

    # execution
    result = runner.invoke(environment, args)

    # assert
    assert result.exit_code == 2


def test_env_cli_activate_default():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["activate", "--session", session]

    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 0


def test_env_cli_activate():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["activate", "--name", "development", "--session", session]

    # execution
    result = runner.invoke(environment, args)
    # assert
    print(result.output)
    assert result.exit_code == 0


def test_env_cli_activate_empty_data():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["activate", "--name", "production", "--session", session]

    # execution
    result = runner.invoke(environment, args)
    # assert
    print(result.output)
    assert result.exit_code == 1


def test_env_cli_activate_wrong_env():
    # setup
    runner = CliRunner()
    args = ["activate", "--name", "prod"]

    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 1


def test_env_cli_activate_wrong_option():
    # setup
    runner = CliRunner()
    args = ["activate", "--n", "production"]

    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 2


def test_env_cli_wrong_activate_command():
    # setup
    runner = CliRunner()
    args = ["acti", "--name", "production"]

    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 2


def test_env_cli_update():
    # setup
    runner = CliRunner()
    args = [
        "update",
        "--name",
        "production",
    ]
    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 0


def test_env_cli_update_DB_host():
    # setup
    runner = CliRunner()
    update_args = ["update", "--name", "production", "--db_hostname", "hostname"]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "DB_HOSTNAME: hostname" in result.output
    assert result.exit_code == 0


def test_env_cli_update_vault_address():
    # setup
    runner = CliRunner()
    update_args = ["update", "--name", "production", "--vault_address", "vault_address"]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "VAULT_ADDRESS: vault_address" in result.output
    assert result.exit_code == 0


def test_env_cli_update_vault_token():
    # setup
    runner = CliRunner()
    update_args = ["update", "--name", "production", "--vault_token", "vault_token"]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "VAULT_TOKEN: vault_token" in result.output
    assert result.exit_code == 0


def test_env_cli_update_vault_read_path():
    # setup
    runner = CliRunner()
    update_args = [
        "update",
        "--name",
        "production",
        "--vault_read_path",
        "vault_read_path",
    ]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "VAULT_READ_PATH: vault_read_path" in result.output
    assert result.exit_code == 0


def test_env_cli_update_deployment_env():
    # setup
    runner = CliRunner()
    update_args = [
        "update",
        "--name",
        "production",
        "--deployment_env",
        "deployment_env",
    ]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "DEPLOYMENT_ENV: deployment_env" in result.output
    assert result.exit_code == 0


def test_env_cli_update_spire_environ():
    # setup
    runner = CliRunner()
    update_args = ["update", "--name", "production", "--spire_environ", "spire_environ"]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "SPIRE_ENVIRON: spire_environ" in result.output
    assert result.exit_code == 0


def test_env_cli_update_databricks_host():
    # setup
    runner = CliRunner()
    update_args = [
        "update",
        "--name",
        "production",
        "--databricks_host",
        "databricks_host",
    ]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "DATABRICKS_HOST: databricks_host" in result.output
    assert result.exit_code == 0


def test_env_cli_update_databricks_token():
    # setup
    runner = CliRunner()
    update_args = [
        "update",
        "--name",
        "production",
        "--databricks_token",
        "databricks_token",
    ]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "DATABRICKS_TOKEN: databricks_token" in result.output
    assert result.exit_code == 0


def test_env_cli_update_few():
    # setup
    runner = CliRunner()
    update_args = [
        "update",
        "--name",
        "production",
        "--databricks_host",
        "databricks_host_new",
        "--databricks_token",
        "databricks_token_new",
    ]
    get_args = ["get", "--name", "production"]
    # execution
    result = runner.invoke(environment, update_args)
    # assert
    assert result.exit_code == 0
    # execution
    result = runner.invoke(environment, get_args)
    # assert
    assert "DATABRICKS_HOST: databricks_host_new" in result.output
    assert "DATABRICKS_TOKEN: databricks_token_new" in result.output
    assert result.exit_code == 0


def test_env_cli_update_wrong_env():
    # setup
    runner = CliRunner()
    args = ["update", "--name", "prd", "--databricks_token", "databricks_token"]
    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 1


def test_env_cli_update_wrong_option():
    # setup
    runner = CliRunner()
    args = ["update", "--nam", "production", "--databricks_toke", "databricks_token"]
    # execution
    result = runner.invoke(environment, args)
    # assert
    assert result.exit_code == 2


def test_env_cli_wrong_update_command():
    # setup
    runner = CliRunner()
    args = ["upd", "--name", "production", "--databricks_token", "databricks_token"]
    # execution
    result = runner.invoke(environment, args)
    # assert
    Path(os.path.join(os.path.expanduser("~/.spire/"), "config.yml")).unlink()
    assert result.exit_code == 2


os.environ["CLI_INSTANCE"] = ""
