import os
import zipfile
from typing import List

import httpx
from zeep import Client

from baremalattes.core.database import Connection
from baremalattes.core.settings import Settings

settings = Settings()

conn_admin = Connection(settings.ADMIN_DATABASE_URL)


XML_PATH = "storage/xml"
CURRENT_XML_PATH = "storage/xml/current"
ZIP_XML_PATH = "storage/xml/current"
PROXY = True

WSDL_URL = "http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl"
soap_client = Client(WSDL_URL) if not PROXY else None


def setup_directories(dirs):
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def clear_xml_directory(directory):
    for file in os.listdir(directory):
        if file.endswith(".xml"):
            os.remove(os.path.join(directory, file))


def get_researchers_from_db():
    SQL_SCRIPT = "SELECT researcher_id, name, lattes_id FROM public.researcher;"
    return conn_admin.select(SQL_SCRIPT)


def download_and_unzip_curriculum(researcher):
    lattes_id = researcher.get("lattes_id", "").zfill(16)

    try:
        if PROXY:
            proxy_url = f"https://simcc.uesc.br/v3/api/getCurriculoCompactado?lattes_id={lattes_id}"
            response = httpx.get(proxy_url, verify=False, timeout=30.0).content
        else:
            response = soap_client.service.getCurriculoCompactado(lattes_id)
    except Exception as e:
        error_msg = f"Falha no download: {e.__class__.__name__}"
        return False, error_msg

    zip_path = os.path.join(ZIP_XML_PATH, f"{lattes_id}.zip")
    try:
        with open(zip_path, "wb") as zip_file:
            zip_file.write(response)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(XML_PATH)
            zip_ref.extractall(CURRENT_XML_PATH)

        return True, "Currículo baixado e descompactado."
    except zipfile.BadZipFile as e:
        error_msg = f"Arquivo corrompido ou não é .zip: {e}"
        return False, error_msg
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)


def print_summary_report(total: int, successes: List, failures: List):
    print("\n" + "=" * 50)
    print("--- RELATÓRIO FINAL DA EXECUÇÃO ---")
    print("=" * 50)
    print(f"Total de pesquisadores processados: {total}")
    print(f"✅ Sucessos: {len(successes)}")
    print(f"❌ Falhas: {len(failures)}")

    if failures:
        print("\n--- Detalhes das Falhas ---")
        for failure in failures:
            print(
                f"  - Pesquisador: {failure['name']} (Lattes: {failure['lattes_id']})"
            )
            print(f"    Motivo: {failure['error']}\n")

    print("=" * 50)
    print("Processo finalizado.")


def main():
    setup_directories([CURRENT_XML_PATH, ZIP_XML_PATH, XML_PATH])
    clear_xml_directory(XML_PATH)

    researchers = get_researchers_from_db()
    total_researchers = len(researchers)

    successful_downloads = []
    failed_downloads = []

    if not researchers:
        print("Nenhum pesquisador encontrado para processar.")
        return

    print(f"\nIniciando download de {total_researchers} currículos...")
    for index, researcher in enumerate(researchers):
        name = researcher.get("name")
        lattes_id = researcher.get("lattes_id")

        print(
            f"\n[{index + 1}/{total_researchers}] Processando: {name} (Lattes: {lattes_id})"
        )

        success, message = download_and_unzip_curriculum(researcher)

        if success:
            successful_downloads.append(researcher)
            print("-> Status: SUCESSO")
        else:
            failed_downloads.append(
                {
                    "name": name,
                    "lattes_id": lattes_id,
                    "error": message,
                }
            )
            print(f"-> Status: FALHA - {message}")

    print_summary_report(total_researchers, successful_downloads, failed_downloads)


if __name__ == "__main__":
    main()
