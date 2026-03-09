import subprocess
import sys

import click
import polars as pl

from barema.services.download_lattes import add_lattes_id, download_lattes_xml
from barema.services.pre_process_projects import (
    download_attachments,
    extract_project_metadata,
)


def db_up():
    try:
        print("Iniciando banco de dados")
        subprocess.run(["docker", "compose", "up", "barema_db", "-d"], check=True)

        print("Executando migrações")
        subprocess.run(["poetry", "run", "alembic", "upgrade", "head"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Falha na execução. Código de saída: {e.returncode}")
        sys.exit(1)
    except FileNotFoundError:
        print("Comando não encontrado no sistema.")
        sys.exit(1)


def seed():
    if click.confirm("Deseja semear o banco novamente?", default=False):
        try:
            print("Iniciando seeding")
            hop_command = [
                "docker",
                "compose",
                "run",
                "--rm",
                "-e",
                "HOP_FILE_PATH=$${PROJECT_HOME}/workflows/Seeding.hwf",
                "barema_hop",
            ]
            subprocess.run(hop_command, check=True)
            print("Fim da execução")
        except subprocess.CalledProcessError as e:
            print(f"Falha na execução. Código de saída: {e.returncode}")
            sys.exit(1)
        except FileNotFoundError:
            print("Comando não encontrado no sistema.")
            sys.exit(1)


def process_cvs():
    try:
        print("Preparando pesquisadores")
        folder_path = r"data/raw/projects"
        researchers = extract_project_metadata(folder_path)
        researchers = pl.DataFrame(
            researchers,
            schema={
                "Arquivo": pl.Utf8,
                "Nome": pl.Utf8,
                "CPF": pl.Utf8,
                "Link": pl.Utf8,
            },
        )
        download_attachments(researchers, folder_path)
        researchers = add_lattes_id(researchers)
        download_lattes_xml(researchers)

        hop_command = [
            "docker",
            "compose",
            "run",
            "--rm",
            "barema_hop",
        ]
        subprocess.run(hop_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Falha na execução. Código de saída: {e.returncode}")
        sys.exit(1)
    except FileNotFoundError:
        print("Comando não encontrado no sistema.")
        sys.exit(1)
