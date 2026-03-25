import json
import os

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from tqdm import tqdm

from barema.core.settings import Settings
from barema.prompts import PROMPT_BAREMA_NOVO

SETTINGS = Settings()

CACHE_DIR = "data/raw/cache"
CSV_PATH = os.path.join(CACHE_DIR, "sumula_cache.csv")
XLSX_PATH = os.path.join(CACHE_DIR, "sumula_cache.xlsx")

CACHE_SCHEMA = {
    "lattes_id": pl.Utf8,
    "sumula": pl.Utf8,
    "transferencia_tecnologia_quantidade": pl.Int64,
    "transferencia_tecnologia_observacao": pl.Utf8,
    "extensao_inovadora_quantidade": pl.Int64,
    "extensao_inovadora_observacao": pl.Utf8,
    "trajetoria_proponente": pl.Int64,
}


def load_cache() -> pl.DataFrame:
    if os.path.exists(CSV_PATH):
        return pl.read_csv(CSV_PATH)

    return pl.DataFrame(schema=CACHE_SCHEMA)


def save_cache(df: pl.DataFrame):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.write_csv(CSV_PATH)
    df.write_excel(XLSX_PATH)


def evaluation(lattes_id: str) -> dict:
    file_path = f"data/raw/projects/{lattes_id}.pdf"

    default_response = {
        "lattes_id": lattes_id,
        "sumula": "Não encontrado",
        "transferencia_tecnologia_quantidade": 0,
        "transferencia_tecnologia_observacao": "Relatório não encontrado",
        "extensao_inovadora_quantidade": 0,
        "extensao_inovadora_observacao": "Relatório não encontrado",
        "trajetoria_proponente": 0,
    }

    if not os.path.exists(file_path):
        return default_response

    loader = PyMuPDFLoader(file_path, mode="single")
    documents = loader.load()
    documento_completo = "\n\n".join([doc.page_content for doc in documents])

    llm = ChatOpenAI(
        api_key=SETTINGS.OPENAI_API_KEY,
        model="gpt-4o-mini",
        temperature=0,
        model_kwargs={"response_format": {"type": "json_object"}},
    )

    mensagem = HumanMessage(
        content=f"{PROMPT_BAREMA_NOVO}\n\nDocumento: {documento_completo}"
    )

    resposta = llm.invoke([mensagem])

    try:
        dados = json.loads(resposta.content)
        dados["lattes_id"] = lattes_id
        return dados
    except Exception:
        return default_response


def analyze_sumula(researchers: pl.DataFrame) -> pl.DataFrame:
    cache = load_cache()

    cached_ids = set(cache["lattes_id"].to_list())

    all_ids = researchers["lattes_id"].to_list()
    new_ids = [l_id for l_id in all_ids if l_id not in cached_ids]

    results = []

    for l_id in tqdm(new_ids, desc="Processando currículos"):
        result = evaluation(l_id)
        results.append(result)

    if results:
        df_new = pl.DataFrame(results, schema=CACHE_SCHEMA)
        cache = pl.concat([cache, df_new], how="vertical")
        cache = cache.unique(subset=["lattes_id"], keep="last")
        save_cache(cache)

    colunas_remover = [
        "sumula",
        "transferencia_tecnologia_quantidade",
        "transferencia_tecnologia_observacao",
        "extensao_inovadora_quantidade",
        "extensao_inovadora_observacao",
        "trajetoria_proponente",
    ]

    colunas_existentes = [col for col in colunas_remover if col in researchers.columns]
    if colunas_existentes:
        researchers = researchers.drop(colunas_existentes)

    df_resultado = researchers.join(cache, on="lattes_id", how="left")

    return df_resultado
