import typer
from rich.console import Console

from baremalattes.download import run_download_process
from baremalattes.report import run_report_process

app = typer.Typer(help='Barema Lattes - Ferramenta de análise de currículos.')
console = Console()


@app.command()
def download():
    console.print('[bold blue]Baixando currículos...[/bold blue]')
    run_download_process()


@app.command()
def report():
    console.print('[bold green]Gerando relatório...[/bold green]')
    run_report_process()


@app.command()
def exit_app():
    """
    Encerra a execução do programa.
    """
    console.print('[yellow]Saindo do programa...[/yellow]')
    raise typer.Exit()


if __name__ == '__main__':
    app()
