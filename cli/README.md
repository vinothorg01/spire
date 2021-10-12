# Spire Command Line Interface (Spirecli)

A command line interface to query and modify Spire models.

## Getting Started

* Install the Spire CLI as a package: `pip install -e ./cli`.

* In order to use most of the functionality of the Spire CLI, you will need to add additional config keys to the config in `~/.spire/config.yml`  which is automatically created when you run the Spire CLI for the first time or can be created manually.  

* On the first usage of CLI ,you are expected to setup at least one environment so CLI prompts for all the needed information.  
 
### Prerequisites

The Spire CLI automatically installs required python packages, and the Spire package itself installs any required Spire packages. It is recommended that Spire and Spirecli be installed and used in a conda or virtual environment.

## Setting the Environment

Certain variables are necessary to access Spire data. Already at first usage of CLI config keys added to either one of the environments in `~/.spire/config.yml` as below 
 

```
development:  
    DATABRICKS_HOST: https://condenast.cloud.databricks.com
    DATABRICKS_TOKEN: <personal databricks token>
    DB_HOSTNAME: <XXX>.rds.amazonaws.com
    DEPLOYMENT_ENV: development
    GHCR_PASSWORD: <github token, NOT user password>
    GHCR_USER: <github account username>
    MLFLOW_TRACKING_URI: databricks
    SPIRE_ENVIRON: cn-spire-dev
    VAULT_ADDRESS: https://prod.vault.conde.io:<XXX>
    VAULT_READ_PATH: <path>
    VAULT_TOKEN: <token>
staging:
    etc.
production:
    etc.
``` 

* You can list the existing environment config by using `environment list`  in which active env shows the current active environment

* You can get a specific config by name using `environment get --name [env]` or `environment get` to get current active environment

* You can activate a specific environment by name using `environment activate --name [env]` or `environment activate` to default development environment

* You can update config keys using the `environment update --name [env] --db_hostname [value] --db_hostname [value] --vault_token [value] --vault_read_path [value] --deployment_env [value] --spire_environ [value] --spire_environ [value] --databricks_host [value] --databricks_token [value]`  or `environment update --name [env] --db_hostname [value]` to update specific config key

By updating additional configuration keys and running that command for that environment, one can easily swap between different Spire environments.  

## How to Use

The pattern for the Spire CLI commands is:  

`spire [GROUP] [COMMAND] --[OPTION1] opt1 --[OPTION2] opt2`  

For example, to create a new Spire Model:  

`spire workflow create --name cli_test --description 'cli > test'`  

Every group and every command has help documentation that can be accessed using `--help`, for example:  

`spire workflow --help`  

The Spire CLI can also be run as a shell, by running just `spire` in the command line, which you will then see as the command line prompt. From the shell, only the group and subsequent arguments need be entered on the command line. For example:  

`query --name spire_cli_test`  

Type `exit` in the command line to exit the Spire CLI shell. 

This is the preferred way to use the Spire CLI, as it will instantiate the Spire runtime just once, as opposed to on each command.  

## Built With

* [Click](https://click.palletsprojects.com/en/7.x/) - The python package used as the framework of the CLI.  

* [click-shell](https://click-shell.readthedocs.io/en/latest/) - The python package used to make a shell out of Spirecli.  

## Authors

* **Max Cantor** - *Initial work* - [maxcan7](https://github.com/maxcan7)
* **Durga Sankar** - *Enhancements* - [DurgaSankar-CN](https://github.com/DurgaSankar-CN)

## Support
* Slack: [#datasci-spire](https://condenast.slack.com/archives/datasci-spire)


## License

Copyright (c) 2020 Condé Nast. All rights reserved.
