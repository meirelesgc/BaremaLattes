import ssl
import zipfile
from pathlib import Path
from typing import List

import httpx
import polars as pl
import requests
import urllib3
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from zeep import Client
    from zeep.transports import Transport
except ImportError:
    Client = None
    Transport = None

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_DATA_PATH = BASE_DIR / "data" / "raw" / "lattes"
RESEARCHERS_FILE = BASE_DIR / "data" / "raw" / "researchers.csv"
PROXY_URL = "https://simcc.uesc.br/v3/api/getCurriculoCompactado"
WSDL_URL = "http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl"


def get_soap_client():
    if Client is None:
        raise RuntimeError("Biblioteca zeep nao instalada.")

    ssl._create_default_https_context = ssl._create_unverified_context
    session = requests.Session()
    session.verify = False
    transport = Transport(timeout=30, operation_timeout=30, session=session)
    return Client(WSDL_URL, transport=transport)


def download_and_extract(
    lattes_id: str, use_proxy: bool, http_client: httpx.Client, soap_client
) -> None:
    if use_proxy:
        response = http_client.get(PROXY_URL, params={"lattes_id": lattes_id})
        response.raise_for_status()
        content = response.content
    else:
        content = soap_client.service.getCurriculoCompactado(lattes_id)

    if not content:
        raise ValueError("Conteudo retornado sem dados.")

    zip_path = RAW_DATA_PATH / f"{lattes_id}.zip"
    with open(zip_path, "wb") as f:
        f.write(content)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(RAW_DATA_PATH)

    zip_path.unlink(missing_ok=True)


def setup_researchers_file() -> bool:
    print(f"\n[!] Arquivo nao encontrado em: {RESEARCHERS_FILE}")
    choice = input("Deseja inserir um pesquisador manualmente agora? (s/n): ").lower()

    if choice == "s":
        name = input("Digite o nome: ")
        lattes_id = input("Digite o ID Lattes: ")

        df = pl.DataFrame({"name": [name], "lattes_id": [lattes_id]})
        RESEARCHERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(RESEARCHERS_FILE)
        return True

    return False


def get_researcher_list() -> List[str]:
    if not RESEARCHERS_FILE.exists() or RESEARCHERS_FILE.stat().st_size == 0:
        if not setup_researchers_file():
            return []

    df = pl.read_csv(RESEARCHERS_FILE)
    if df.is_empty():
        return []

    return df["lattes_id"].cast(pl.Utf8).to_list()


def run_download_process():
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

    try:
        ids = get_researcher_list()
        if not ids:
            return

        use_proxy = True
        http_client = httpx.Client(timeout=30.0, verify=False)
        soap_client = None if use_proxy else get_soap_client()

        for lattes_id in tqdm(ids, desc="Downloads"):
            try:
                download_and_extract(lattes_id, use_proxy, http_client, soap_client)
            except Exception:
                pass

        print("Operacao concluida.")

    except Exception as e:
        print(f"Erro: {e}")
