"""all commands here"""
import click

from .serv import add_seed
from .serv import calculate
from .serv import export
from .serv import reset
from .serv import update_params


@click.group()
def cli():
    """all clicks here"""


cli.add_command(add_seed)
cli.add_command(calculate)
cli.add_command(export)
cli.add_command(reset)
cli.add_command(update_params)
