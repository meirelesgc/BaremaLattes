import click

from barema.core.report_generation import report_generation_process
from barema.core.report_production import report_production_csv
from barema.core.review_data import review_data
from barema.core.setup import db_up, populate_db, seeding


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
    seeding()
    populate_db()


@cli.command()
def report():
    click.echo("Gerando o relatório...")
    report_generation_process()
    report_production_csv()


if __name__ == "__main__":
    cli()
