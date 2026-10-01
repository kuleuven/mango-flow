import shutil
from importlib import resources

import click
from mango_lib.cli.base import mango

import mango_flow


@click.group("flow")
def flow_group() -> None:
    """ManGO Flow cli commands"""


@flow_group.command("init")
@click.option("--script", "-s", is_flag=True, help="Intialize a script to start the various celery parts")
def init(script: bool) -> None:
    """Initialize the ManGO Flow environment."""
    if script:
        src = resources.files(mango_flow) / "bin/run_celery.sh"
        shutil.copyfile(str(src), "run_celery.sh")
        click.secho("Created run_celery.sh script to start the celery workers and beat scheduler.", fg="green", bold=True)


def register() -> None:
    """Attach this plugin's commands to the base `mango` CLI group.

    register() is called automatically by the plugin loader when this plugin is discovered."""
    mango.add_command(flow_group)