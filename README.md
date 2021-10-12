<div align="center">

![Spire Logo](https://github.com/condenast/spire/blob/main/assets/spire-logo-transparent.png)

</div>

---

**Spire** is Condé Nast's smart data platform that connects first-party user data to
other first-, second-, and third-party data to understand and influence our audiences at the "pre-search"
stage of our experience funnel. For more information, please see the Data Science
[Wiki](https://cnissues.atlassian.net/wiki/spaces/DSDS/pages/294191114/Spire) on the Spire project.

This repository contains the framework and implementation behind that process.

|      System      |                                                  Build                                                   |                                                   Tests                                                    |
| :--------------: | :------------------------------------------------------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------: |
| OS X / Linux CPU | ![Continuous Deployment](https://github.com/CondeNast/spire/workflows/Continuous%20Deployment/badge.svg) | ![Continuous Integration](https://github.com/CondeNast/spire/workflows/Continuous%20Integration/badge.svg) |

## Getting Started

### Python

Spire's Python-based interface is available on Databricks (clusters with the "spire-dev" prefix; e.g., [spire-dev-1](https://condenast.cloud.databricks.com/#/setting/clusters/0128-145813-debs305)) or through a local build (see the "Developer" section below).

```python
import spire
print(spire.__version__)
```

For more information on available methods and actions, please refer to the [Python API Documentation](https://github.com/CondeNast/spire/wiki/Python-API).

### CLI

Spire also offers a companion Command-line Interface (CLI). For more information, please refer to the [CLI's Getting Started section](https://github.com/CondeNast/spire/tree/main/cli#getting-started).

## Developer

[![Maintainability](https://api.codeclimate.com/v1/badges/8a197f6bbda408b992f7/maintainability)](https://codeclimate.com/repos/5df9452a216c9d475b000a63/maintainability)
 [![Test Coverage](https://api.codeclimate.com/v1/badges/8a197f6bbda408b992f7/test_coverage)](https://codeclimate.com/repos/5df9452a216c9d475b000a63/test_coverage)

### Introduction

Spire as a software product is primarily a Python-based process that leverages [Apache Spark](https://spark.apache.org/) and [Apache Airflow](https://airflow.apache.org/).

If you're interested in contributing to the development of Spire, please refer to the following sections.

### Requirements

- Python 3.8.x
- Java JDK 8 or 11
- Spark 3.1.2
- Scala 2.12.x (Optional)
- Anaconda3/Miniconda3 (Recommended) or Virtualenv/Tox
- Docker and Docker Compose (comes with Docker on Mac)

### Build

#### Build w/ [Conda](https://docs.conda.io/en/latest/) (Recommended)

```shell
$ conda create --name spire-dev python=3.8
$ conda activate spire-dev
$ git submodule update --init --recursive
$ (cd include/kalos && pip install -r requirements.txt && pip install -e .)
$ (cd include/datasci-common/python && pip install -r requirements.txt && pip install -e .)
$ pip install -e .
$ pip install -e ./cli/
```

The above creates a conda environment, updates the submodules relative to the Spire commit sha, installs the submodules and their requirements, and installs Spire.  
In the case of Spire, installing the package automatically installs the requirements, but this may or may not be the case with all submodules which is why we're manually installing their requirements.  
The last line installs the CLI, but this is optional.  

#### Spire Config

Spire's "runtime" (i.e. when importing a Spire module) instantiates a config (see `/spire/config.py`), which is necessary to use most of Spire's functionality. While many of these variables have default values, in most cases you'll need to have these variables exported into your bash environment prior to running Spire.  

This can be done by adding them to your bash profile, but given that the variables are in many cases dependent on the environment (e.g. development, staging, production), it may be better to export them from the terminal as needed.  

The `~/.spire/config.yml` for the CLI (see `/cli/README.md`) can be a good point of reference for which variables you'll need to export.  

### Lint

We [lint](https://en.wikipedia.org/wiki/Lint_(software)) Spire's codebase through [Flake8](https://flake8.pycqa.org/en/latest/).

```shell
$ pip install flake8
$ flake8 spire
```

_(Note, the [Build](https://github.com/orgs/CondeNast/packages/container/package/spire#build-conda) step must be completed first.)_

### Code Formatting

We use [Black](https://black.readthedocs.io/en/stable/) to format Spire's codebase.

We recommend that you configure your IDE to run Black automatically.

Alternately, you can run it explicitly from the command line.

```shell
$ pip install black
$ black spire
```

Note that unlike flake8 which provides suggestions, running black will automatically apply formatting. Many parts of the Spire codebase were written before we started using black, and so running black on the whole codebase may inadvertently introduce unintended and undesired changes into a PR.  

### Typing

We have begun to implement type hinting into Spire, although many parts of the codebase do not have type hinting and adding type hinting may be a non-trivial task.  
Where applying type hinting, mypy is our standard, i.e.

```shell
$ pip install mypy
$ mypy spire
```

### Test

#### Test Setup

We recommend you run tests through [Docker Compose](https://docs.docker.com/compose/). Using containers ensures that your environment is as similar to the production environment as possible.  
More than that, it is also necessary for the integration and functional tests, or at least barring manual workarounds.  

#### Running Tests w/ [Docker](https://docs.docker.com/get-started/overview/)

There are two ways to run tests w/ Docker.

##### One-off Test Run w/ Docker Compose

```shell
$ docker-compose run spire pytest tests
```

##### Interactive Development w/ Docker Compose

If you are developing Spire and would like to run the test suite frequently, consider Docker Compose's [up command](https://docs.docker.com/compose/reference/up/).

```shell
$ docker-compose up -d
```

_(Note, this command will create the environment and keep it running.)_

You can then run your tests with docker-compose's [exec command](https://docs.docker.com/compose/reference/exec/).

```shell
$ docker-compose exec spire pytest tests
```

You can also pass arguments through the end of the command.

For example, to run a single test file:

```shell
$ docker-compose exec spire pytest <test_file_name.py>
```

Other useful Docker commands:

```shell
$ docker-compose down # shutdown the environment
$ docker ps # see what's running
$ docker system prune -a # pruning the inevitable egregious buildup of docker containers, images, builders, etc.
$ docker run -it <image sha> bash # ssh into the container running the image
```

#### Running Tests w/ Terminal
Before running pytest, you should have `spire` installed in an editable mode i.e. `pip install -e .`, then

```
$ pip install pytest
$ pytest tests
```

If you do not have a database set up locally, you can skip the integration tests or use the Postgres Docker container.

To skip the integration test
```shell
$ pytest tests --ignore=tests/integrations
```

To use the Postgres Docker container
1. Start the Postgres container by doing `docker-compose up -d postgres`
2. set the env `DB_HOSTNAME` to `localhost` i.e. `$ export DB_HOSTNAME=localhost`
3. Run test as usual `$ pytest tests`


#### Test Coverage

We generate test coverage with [Coverage.py](https://coverage.readthedocs.io/). This coverage is automatically sent to [Code Climate](https://codeclimate.com/repos/5df9452a216c9d475b000a63) during [continuous integration (CI)](https://github.com/CondeNast/spire/blob/main/.github/workflows/ci.yml).

```shell
$ pip install coverage
$ coverage run -m pytest tests
```

#### Debugging .github/workflows

These are the github actions worflows for CI/CD. The workflows can be run locally using the `act` CLI (https://github.com/nektos/act). For additional information, see `docs.ci-cd.md`.    
  
### Database Management

#### Database Versioning

We use [Alembic](https://alembic.sqlalchemy.org/en/latest/) to manage our database schema. If you have made any modifications to 
the ORM mapping, make sure to generate new migrations.

```shell
$ pip install alembic
$ alembic revision --autogenerate -m <message>
```

Then, update the database with the appropriate revision.

```shell
$ alembic upgrade head
```

#### Accessing the Database within Docker

As mentioned previously, running the tests with Docker allows one to run the integrations and functional tests using a Docker Volume to run a test database. Different tests set up the session scope differently, but for each scope, the test database will be torn down and rebuilt at startup, ensuring consistency.  

## Development Workflow & Release Process

```text
# create a feature branch
From main or a release branch, create a new branch off of a JIRA ticket e.g. `git checkout -b SPIRE-123` or barring that, following a semantic convention `feature/x`, `bugfix/y`, `enhancement/z`.

# merge into main
feature/x ---(PR and CI)---> main branch ---> CD x.y.z-dev (rolling unstable release)

# ready for production
main (x.y.z-dev) ---(checkout)---> release/vx.y.(z+1) ---> create tag (vx.y.(z+1))
  ---> publish release (Release vx.y.(z+1)) ---> CD (x.y.(z+1))stable produced
```
_(Note, the +1 bump may be added to the x, y, or z (major, minor, patch) version in keeping with [SemVer](https://semver.org/) requirements.)_

## Support

- **Slack:** [\#data-spire-dev](https://condenast.slack.com/archives/data-spire-dev)

## License

Copyright (c) 2020 Condé Nast. All rights reserved.
