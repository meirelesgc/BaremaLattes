import logging
import ssl
import zipfile
from pathlib import Path
from typing import List

import httpx
import polars as pl
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from zeep import Client
    from zeep.transports import Transport
except ImportError:
    Client = None
    Transport = None

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_PATH = BASE_DIR / 'data' / 'raw' / 'lattes'
RESEARCHERS_FILE = BASE_DIR / 'data' / 'raw' / 'researchers.csv'
LOG_DIR = BASE_DIR / 'logs'
LOG_FILE = LOG_DIR / 'download.log'


def setup_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('baremalattes.download')
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
        fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


logger = setup_logger()


class LattesDownloader:
    def __init__(self, use_proxy: bool = False):
        self.use_proxy = use_proxy
        self.proxy_url = 'https://simcc.uesc.br/v3/api/getCurriculoCompactado'
        self.wsdl_url = (
            'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl'
        )

        self.http = httpx.Client(timeout=30.0, verify=False)

        self.soap_client = None
        if not self.use_proxy:
            if Client is None:
                raise RuntimeError('Biblioteca zeep nao instalada.')

            ssl._create_default_https_context = ssl._create_unverified_context

            session = requests.Session()
            session.verify = False
            transport = Transport(
                timeout=30, operation_timeout=30, session=session
            )

            self.soap_client = Client(self.wsdl_url, transport=transport)

    def download_and_extract(self, lattes_id: str) -> None:
        logger.info(f'Iniciando download: {lattes_id}')

        if self.use_proxy:
            response = self.http.get(
                self.proxy_url, params={'lattes_id': lattes_id}
            )
            response.raise_for_status()
            content = response.content
        else:
            content = self.soap_client.service.getCurriculoCompactado(lattes_id)

        if not content:
            raise ValueError('Conteudo vazio retornado.')

        zip_path = RAW_DATA_PATH / f'{lattes_id}.zip'
        with open(zip_path, 'wb') as f:
            f.write(content)

        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(RAW_DATA_PATH)

        zip_path.unlink(missing_ok=True)
        logger.info(f'Download e extracao concluidos: {lattes_id}')


def setup_researchers_file() -> bool:
    print(f'\n[!] Arquivo ausente ou vazio em: {RESEARCHERS_FILE}')
    choice = input(
        'Deseja inserir um pesquisador manualmente agora? (s/n): '
    ).lower()

    if choice == 's':
        name = input('Digite o nome: ')
        lattes_id = input('Digite o ID Lattes: ')

        df = pl.DataFrame({'name': [name], 'lattes_id': [lattes_id]})
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

    return df['lattes_id'].cast(pl.Utf8).to_list()


def run_download_process():
    logger.info('--- INICIANDO PROCESSO DE DOWNLOAD ---')
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

    try:
        ids = get_researcher_list()
        if not ids:
            return

        downloader = LattesDownloader(use_proxy=True)

        for lattes_id in ids:
            try:
                downloader.download_and_extract(lattes_id)
            except Exception as e:
                logger.error(f'Erro no lattes_id {lattes_id}: {e}')

        logger.info('--- DOWNLOAD FINALIZADO ---')
        print('Operacao concluida com sucesso.')

    except Exception as e:
        logger.error(f'Erro fatal durante a execucao: {e}')
        print(f'Erro critico: {e}')
