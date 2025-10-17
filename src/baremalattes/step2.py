from pprint import pprint

import pandas as pd

from baremalattes.core.database import Connection
from baremalattes.core.settings import Settings

conn = Connection(Settings().DATABASE_URL)
conn_admin = Connection(Settings().ADMIN_DATABASE_URL)

CSV_FILE_PATH = "storage/csv/dmdc.csv"
INSTITUTION_ID = "ed4089c6-4a75-407a-95b5-eaad408566a0"
SQL_INSERT_RESEARCHER = """
    INSERT INTO researcher (name, lattes_id, institution_id)
    VALUES (%(name)s, %(lattes_id)s, %(institution_id)s)
    """


def extract_lattes_id(lattes_url: str) -> str | None:
    if not isinstance(lattes_url, str):
        return None

    try:
        lattes_id = lattes_url.strip().split("/")[-1]
        return lattes_id if lattes_id.isnumeric() else None
    except (IndexError, AttributeError):
        return None


def process_researchers_data(df: pd.DataFrame, db_connection):
    invalid_lattes_entries = []
    insertion_failures = []

    for row in df.itertuples(index=False, name="Researcher"):
        name = str(row.NOME).strip()
        lattes_url = str(row.LATTES).strip()

        lattes_id = extract_lattes_id(lattes_url)

        if not lattes_id:
            invalid_lattes_entries.append(
                {
                    "name": name,
                    "lattes_url": lattes_url,
                }
            )
            continue

        try:
            params = {
                "name": name,
                "lattes_id": lattes_id,
                "institution_id": INSTITUTION_ID,
            }
            db_connection.exec(SQL_INSERT_RESEARCHER, params)
        except Exception:
            insertion_failures.append({"name": name, "lattes_url": lattes_url})

    return invalid_lattes_entries, insertion_failures


def main():
    try:
        researchers_df = pd.read_csv(CSV_FILE_PATH)
    except FileNotFoundError:
        print(f"Erro: O arquivo '{CSV_FILE_PATH}' não foi encontrado.")
        return

    cleaned_df = researchers_df.dropna(subset=["NOME", "LATTES"])

    invalid_lattes, db_errors = process_researchers_data(cleaned_df, conn_admin)

    print("--- Relatório de Processamento ---")

    print("\n[!] Pesquisadores com URL de Lattes inválida:")
    pprint(invalid_lattes)

    print("\n[!] Falhas ao inserir no banco de dados (ex: duplicados):")
    pprint(db_errors)

    print("\n--- Fim do Relatório ---")


if __name__ == "__main__":
    main()
