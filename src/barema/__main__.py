from random import seed

import click

from barema.core.generate_report import start_process
from barema.core.review_data import review_data
from barema.core.setup import db_up, process_cvs, seed


@click.group()
def cli():
    pass


@cli.command()
def review():
    click.echo("Iniciando visualização de dados...")
    review_data()


@cli.command()
def setup():
    click.echo("Iniciando a montagem do banco de dados...")
    db_up()
    seed()
    process_cvs()


@cli.command()
def generate():
    click.echo("Gerando o relatório...")
    start_process()


if __name__ == "__main__":
    cli()
