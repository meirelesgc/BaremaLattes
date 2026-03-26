import os

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from tqdm import tqdm

from barema.core.settings import Settings
from barema.prompts import PROMPTS_AVALIACAO

SETTINGS = Settings()

CACHE_DIR = "data/raw/cache"
CSV_PATH = os.path.join(CACHE_DIR, "project_analysis_cache.csv")
XLSX_PATH = os.path.join(CACHE_DIR, "project_analysis_cache.xlsx")

llm = ChatOpenAI(api_key=SETTINGS.OPENAI_API_KEY, model="gpt-5-nano", temperature=0)

EXPECTED_KEYS = [
    "publico_produto",
    "objetivos_metas_relevancia",
    "metodologia_gestao",
    "colaboracoes_financiamento",
    "potencial_inovacao",
    "atendimento_necessidades",
    "maturidade_resultados",
    "organizacao_parcerias",
    "aderencia_area",
    "parecer_final",
]


class EvaluationResult(BaseModel):
    publico_produto: str
    objetivos_metas_relevancia: str
    metodologia_gestao: str
    colaboracoes_financiamento: str
    potencial_inovacao: str
    atendimento_necessidades: str
    maturidade_resultados: str
    organizacao_parcerias: str
    aderencia_area: str
    parecer_final: str


parser = PydanticOutputParser(pydantic_object=EvaluationResult)

prompt_template = """
Responda aos critérios utilizando o documento fornecido.

Critérios:
{criterios}

Responda SOMENTE com o JSON no formato solicitado.
{format_instructions}

Documento:
{text}
"""

prompt = PromptTemplate(
    template=prompt_template,
    input_variables=["criterios", "text"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

chain = prompt | llm | parser


def _load_text_from_pdf(file_path: str) -> str:
    loader = PyMuPDFLoader(file_path, mode="single")
    documents = loader.load()
    return "\n\n".join([doc.page_content for doc in documents])


def evaluation(lattes_id: str) -> dict:
    file_path = f"data/raw/projects/{lattes_id}.pdf"

    if not os.path.exists(file_path):
        resultado = {"lattes_id": lattes_id}
        for key in EXPECTED_KEYS:
            resultado[key] = "Relatório não encontrado"
        return resultado

    text = _load_text_from_pdf(file_path)

    criterios_texto = "\n".join(
        [f"- {key}: {PROMPTS_AVALIACAO.get(key, 'Descreva')}" for key in EXPECTED_KEYS]
    )

    parsed = chain.invoke({"criterios": criterios_texto, "text": text})

    resultado_final = {"lattes_id": lattes_id}
    for key in EXPECTED_KEYS:
        resultado_final[key] = getattr(parsed, key)

    return resultado_final


def load_cache() -> pl.DataFrame:
    if os.path.exists(CSV_PATH):
        return pl.read_csv(CSV_PATH, schema_overrides={"lattes_id": pl.Utf8})

    schema = {"lattes_id": pl.Utf8}
    for key in EXPECTED_KEYS:
        schema[key] = pl.Utf8

    return pl.DataFrame(schema=schema)


def save_cache(df: pl.DataFrame):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.write_csv(CSV_PATH)
    df.write_excel(XLSX_PATH)


def evaluate_projects(df_researchers: pl.DataFrame) -> pl.DataFrame:
    cache = load_cache()
    cached_ids = set(cache["lattes_id"].to_list())

    df_researchers = df_researchers.with_columns(pl.col("lattes_id").cast(pl.Utf8))
    all_ids = df_researchers["lattes_id"].to_list()

    new_ids = [lid for lid in all_ids if lid not in cached_ids]
    results = []

    for lattes_id in tqdm(new_ids, desc="Analisando projetos"):
        result = evaluation(lattes_id)
        results.append(result)

    if results:
        schema = {"lattes_id": pl.Utf8}
        for key in EXPECTED_KEYS:
            schema[key] = pl.Utf8

        df_new = pl.DataFrame(results, schema=schema)
        cache = pl.concat([cache, df_new], how="vertical")
        cache = cache.unique(subset=["lattes_id"], keep="last")
        save_cache(cache)

    df_final = df_researchers.join(cache, on="lattes_id", how="left")
    return df_final
