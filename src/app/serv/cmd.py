"""all commands"""
import click

from .saver import Saver


@click.command()
@click.option(
    "--consumer-key",
    type=click.STRING,
    help='consumer key',
    required=True,
)
@click.option(
    "--consumer-secret",
    type=click.STRING,
    help='consumer secret',
    required=True,
)
@click.option(
    "--access-token",
    type=click.STRING,
    help='access token',
    required=True,
)
@click.option(
    "--access-token-secret",
    type=click.STRING,
    help='access token secret',
    required=True,
)
@click.option(
    "--project-path",
    type=click.STRING,
    help='access token secret',
    required=True,
)
def update_params(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret,
    project_path,
):
    """add seed"""
    return Saver.update_params(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
        project_path,
    )


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
    return Saver().search()


@click.command()
@click.option(
    "--csv-path",
    type=click.STRING,
    help='path of output csv file',
    required=True,
)
def export(csv_path):
    """export"""
    return Saver().export(csv_path)
