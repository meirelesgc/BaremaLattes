import json
import os

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from barema.core.settings import Settings
from barema.prompts import PROMPT_BAREMA_NOVO

SETTINGS = Settings()

CACHE_DIR = "data/raw/cache"
CSV_PATH = os.path.join(CACHE_DIR, "sumula_cache.csv")
XLSX_PATH = os.path.join(CACHE_DIR, "sumula_cache.xlsx")

CACHE_SCHEMA = {
    "lattes_id": pl.Utf8,
    "sumula": pl.Utf8,
    "transferencia_tecnologia_nota": pl.Int64,
    "transferencia_tecnologia_observacao": pl.Utf8,
    "extensao_inovadora_nota": pl.Int64,
    "extensao_inovadora_observacao": pl.Utf8,
    "trajetoria_proponente": pl.Int64,
    "trajetoria_proponente_observacao": pl.Utf8,
}

llm = ChatOpenAI(
    api_key=SETTINGS.OPENAI_API_KEY,
    model="gpt-5-nano",
    temperature=0,
    model_kwargs={"response_format": {"type": "json_object"}},
)


def load_cache() -> pl.DataFrame:
    if os.path.exists(CSV_PATH):
        return pl.read_csv(CSV_PATH, schema=CACHE_SCHEMA)

    return pl.DataFrame(schema=CACHE_SCHEMA)


def save_cache(df: pl.DataFrame):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.write_csv(CSV_PATH)
    df.write_excel(XLSX_PATH)


def load_document_content(lattes_id: str) -> str:
    file_path = f"data/raw/projects/{lattes_id}.pdf"
    if not os.path.exists(file_path):
        return ""
    loader = PyMuPDFLoader(file_path, mode="single")
    documents = loader.load()
    return "\n\n".join([doc.page_content for doc in documents])


def analyze_sumula(researchers: pl.DataFrame) -> pl.DataFrame:
    cache = load_cache()

    cached_ids = set(cache["lattes_id"].to_list())
    all_ids = researchers["lattes_id"].to_list()
    new_ids = [l_id for l_id in all_ids if l_id not in cached_ids]

    results = []
    inputs_gerais = []
    lattes_validos = []

    default_response_template = {
        "sumula": "Não encontrado",
        "transferencia_tecnologia_nota": 0,
        "transferencia_tecnologia_observacao": "Não encontrado",
        "extensao_inovadora_nota": 0,
        "extensao_inovadora_observacao": "Não encontrado",
        "trajetoria_proponente": 0,
        "trajetoria_proponente_observacao": "Não encontrado",
    }

    for l_id in new_ids:
        doc_content = load_document_content(l_id)
        if doc_content:
            mensagem = HumanMessage(
                content=f"{PROMPT_BAREMA_NOVO}\n\nDocumento: {doc_content}"
            )
            inputs_gerais.append([mensagem])
            lattes_validos.append(l_id)
        else:
            default_data = default_response_template.copy()
            default_data["lattes_id"] = l_id
            results.append(default_data)

    if inputs_gerais:
        respostas = llm.batch(inputs_gerais)
        for l_id, resposta in zip(lattes_validos, respostas):
            try:
                dados = json.loads(resposta.content)
                dados["lattes_id"] = l_id
                results.append(dados)
            except Exception:
                error_data = default_response_template.copy()
                error_data["lattes_id"] = l_id
                results.append(error_data)

    if results:
        df_new = pl.DataFrame(results, schema=CACHE_SCHEMA)
        cache = pl.concat([cache, df_new], how="vertical")
        cache = cache.unique(subset=["lattes_id"], keep="last")
        save_cache(cache)

    colunas_remover = [
        "sumula",
        "transferencia_tecnologia_nota",
        "transferencia_tecnologia_observacao",
        "extensao_inovadora_nota",
        "extensao_inovadora_observacao",
        "trajetoria_proponente_nota",
        "trajetoria_proponente_observacao",
    ]

    colunas_existentes = [col for col in colunas_remover if col in researchers.columns]
    if colunas_existentes:
        researchers = researchers.drop(colunas_existentes)

    df_resultado = researchers.join(cache, on="lattes_id", how="left")

    return df_resultado
