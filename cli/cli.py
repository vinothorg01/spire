"""Spire CLI base setup"""

import click
from click_shell import shell

from spire import __version__ as v

from cli.utils.env_cli import environment
from cli.workflows import (
    workflows,
    datasets,
    queries,
    schedules,
    clusterstatuses,
    traits,
    tags,
    history,
)
from cli.runners import stage_runners

CONTEXT_SETTINGS = dict(help_option_names=["--help"])


@shell(
    prompt="spire > ",
    intro="\nStarting Spire CLI interactive shell..\n"
    r"""
             ________  ________  ___  ________  _______
            |\   ____\|\   __  \|\  \|\   __  \|\  ___ \
            \ \  \___|\ \  \|\  \ \  \ \  \|\  \ \   __/|
             \ \_____  \ \   ____\ \  \ \   _  _\ \  \_|/__
              \|____|\  \ \  \___|\ \  \ \  \\  \\ \  \_|\ \
                ____\_\  \ \__\    \ \__\ \__\\ _\\ \_______\
               |\_________\|__|     \|__|\|__|\|__|\|_______|
               \|_________|
             """
    "\nType `--help`, or `help` "
    "to see the commands list\n",
    context_settings=CONTEXT_SETTINGS,
)
@click.version_option(version=v, prog_name="Spire")
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    ctx.obj["HELP"] = ctx.get_help()


@cli.command(name="--version", hidden=True)
def get_version():
    click.echo(v)


@cli.command(name="--help", hidden=True)
@click.pass_context
def _get_help(ctx):
    click.echo(ctx.obj["HELP"])


cli.add_command(environment, name="environment")
cli.add_command(workflows, name="workflow")
cli.add_command(datasets, name="dataset")
cli.add_command(queries, name="query")
cli.add_command(history, name="history")
cli.add_command(schedules, name="schedule")
cli.add_command(clusterstatuses, name="clusterstatus")
cli.add_command(traits, name="trait")
cli.add_command(tags, name="tag")
cli.add_command(stage_runners, name="stage-runner")
