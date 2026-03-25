import os
from typing import Optional

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from tqdm import tqdm

from barema.core.settings import Settings

SETTINGS = Settings()

CACHE_DIR = "data/raw/cache"
CSV_PATH = os.path.join(CACHE_DIR, "transfer_tech_cache.csv")
XLSX_PATH = os.path.join(CACHE_DIR, "transfer_tech_cache.xlsx")

llm = ChatOpenAI(api_key=SETTINGS.OPENAI_API_KEY, model="gpt-4o-mini", temperature=0)


class ExtractionResult(BaseModel):
    licenciamento_qtd: int
    licenciamento: Optional[str]
    servicos_qtd: int
    servicos: Optional[str]
    empresas_qtd: int
    empresas: Optional[str]
    demanda_qtd: int
    demanda: Optional[str]


class AttachmentExtractionResult(BaseModel):
    carta_apoio: bool
    comentarios_anexos: Optional[str]


parser = PydanticOutputParser(pydantic_object=ExtractionResult)
parser_attachment = PydanticOutputParser(pydantic_object=AttachmentExtractionResult)

prompt_template = """
Você receberá o texto extraído das seções 11 e 12 do currículo.

Para cada critério abaixo:

1) Licenciamento
2) Serviços
3) Empresas/Outros
4) Demanda

Faça:

- Identifique evidências claras no texto.
- Conte quantas evidências distintas existem.
- Produza um pequeno texto em Markdown explicando o que foi identificado.
- Inclua pelo menos uma citação literal curta retirada exatamente do texto.
- A citação deve estar em bloco Markdown usando > 

Se não houver evidência:
- Retorne quantidade 0
- Retorne null para o markdown

A quantidade deve refletir o número real de evidências distintas encontradas no texto.

Responda SOMENTE com o JSON no formato do parser.
{format_instructions}

Texto:
{text}
"""

prompt_template_attachment = """
Você receberá o texto extraído dos anexos do formulário do projeto ou da súmula.

Faça:
1. Verifique se existe menção ou transcrição de uma carta de apoio da instituição de ensino ou de pesquisa a qual o proponente pertence. Retorne um valor booleano.
2. Gere um texto resumindo os comentários gerais e o conteúdo dos anexos do projeto.

Responda SOMENTE com o JSON no formato do parser.
{format_instructions}

Texto:
{text}
"""

prompt = PromptTemplate(
    template=prompt_template,
    input_variables=["text"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

prompt_attachment = PromptTemplate(
    template=prompt_template_attachment,
    input_variables=["text"],
    partial_variables={
        "format_instructions": parser_attachment.get_format_instructions()
    },
)

chain = prompt | llm | parser
chain_attachment = prompt_attachment | llm | parser_attachment


def _load_text_from_pdf(file_path: str) -> str:
    loader = PyMuPDFLoader(file_path, mode="single")
    documents = loader.load()
    texts = [d.page_content for d in documents]
    return "\n".join(texts)


def extract_data(lattes_id: str) -> dict:
    file_path = f"data/raw/projects/{lattes_id}.pdf"
    attachment_path = f"data/raw/projects/attachment/{lattes_id}.pdf"

    result = {
        "lattes_id": lattes_id,
        "licenciamento_qtd": 0,
        "licenciamento": None,
        "servicos_qtd": 0,
        "servicos": None,
        "empresas_qtd": 0,
        "empresas": None,
        "demanda_qtd": 0,
        "demanda": None,
        "carta_apoio": False,
        "comentarios_anexos": None,
    }

    if os.path.exists(file_path):
        text = _load_text_from_pdf(file_path)
        parsed = chain.invoke({"text": text})
        result.update(
            {
                "licenciamento_qtd": parsed.licenciamento_qtd,
                "licenciamento": parsed.licenciamento,
                "servicos_qtd": parsed.servicos_qtd,
                "servicos": parsed.servicos,
                "empresas_qtd": parsed.empresas_qtd,
                "empresas": parsed.empresas,
                "demanda_qtd": parsed.demanda_qtd,
                "demanda": parsed.demanda,
            }
        )

    if os.path.exists(attachment_path):
        text_attachment = _load_text_from_pdf(attachment_path)
        parsed_attachment = chain_attachment.invoke({"text": text_attachment})
        result.update(
            {
                "carta_apoio": parsed_attachment.carta_apoio,
                "comentarios_anexos": parsed_attachment.comentarios_anexos,
            }
        )

    return result


def load_cache() -> pl.DataFrame:
    if os.path.exists(CSV_PATH):
        return pl.read_csv(CSV_PATH, schema_overrides={"lattes_id": pl.Utf8})

    return pl.DataFrame(
        schema={
            "lattes_id": pl.Utf8,
            "licenciamento_qtd": pl.Int64,
            "licenciamento": pl.Utf8,
            "servicos_qtd": pl.Int64,
            "servicos": pl.Utf8,
            "empresas_qtd": pl.Int64,
            "empresas": pl.Utf8,
            "demanda_qtd": pl.Int64,
            "demanda": pl.Utf8,
            "carta_apoio": pl.Boolean,
            "comentarios_anexos": pl.Utf8,
        }
    )


def save_cache(df: pl.DataFrame):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.write_csv(CSV_PATH)
    df.write_excel(XLSX_PATH)


def get_transfer_of_technology(df_researchers: pl.DataFrame) -> pl.DataFrame:
    cache = load_cache()
    cached_ids = set(cache["lattes_id"].to_list())

    df_researchers = df_researchers.with_columns(pl.col("lattes_id").cast(pl.Utf8))
    all_ids = df_researchers["lattes_id"].to_list()

    new_ids = [lid for lid in all_ids if lid not in cached_ids]
    results = []

    for lattes_id in tqdm(new_ids, desc="Extraindo Transferência de Tecnologia"):
        result = extract_data(lattes_id)
        results.append(result)

    if results:
        df_new = pl.DataFrame(
            results,
            schema={
                "lattes_id": pl.Utf8,
                "licenciamento_qtd": pl.Int64,
                "licenciamento": pl.Utf8,
                "servicos_qtd": pl.Int64,
                "servicos": pl.Utf8,
                "empresas_qtd": pl.Int64,
                "empresas": pl.Utf8,
                "demanda_qtd": pl.Int64,
                "demanda": pl.Utf8,
                "carta_apoio": pl.Boolean,
                "comentarios_anexos": pl.Utf8,
            },
        )
        cache = pl.concat([cache, df_new], how="vertical")
        cache = cache.unique(subset=["lattes_id"], keep="last")
        save_cache(cache)

    df_final = df_researchers.join(cache, on="lattes_id", how="left")
    return df_final
