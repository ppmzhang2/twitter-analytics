"""all commands"""
import click

from .saver import Saver


@click.command()
@click.option(
    "--seed",
    '-s',
    type=click.STRING,
    multiple=True,
)
def add_seed(seed):
    """add seed"""
    return Saver().seeds(*seed)


@click.command()
def reset():
    """reset DB"""
    return Saver().reset()


@click.command()
def calculate():
    """calculate"""
    return Saver().automaton()


@click.command()
def export():
    """export"""
    return Saver().export()
