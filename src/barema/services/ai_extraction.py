import os
import re
from typing import Optional

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from barema.core.settings import Settings

SETTINGS = Settings()
llm = ChatOpenAI(api_key=SETTINGS.OPENAI_API_KEY, model="gpt-4o-mini", temperature=0)


class ExtractionResult(BaseModel):
    licenciamento_qtd: int
    licenciamento_markdown: Optional[str]
    servicos_qtd: int
    servicos_markdown: Optional[str]
    empresas_qtd: int
    empresas_markdown: Optional[str]
    demanda_qtd: int
    demanda_markdown: Optional[str]


parser = PydanticOutputParser(pydantic_object=ExtractionResult)

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

prompt = PromptTemplate(
    template=prompt_template,
    input_variables=["text"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

chain = prompt | llm | parser


def _load_text_from_pdf(file_path: str) -> str:
    loader = PyMuPDFLoader(file_path, mode="single")
    documents = loader.load()
    texts = [d.page_content for d in documents]
    return "\n".join(texts)


def _extract_section_between(text: str, start_pattern: str, end_pattern: str) -> str:
    flags = re.IGNORECASE | re.DOTALL
    start = re.search(re.escape(start_pattern), text, flags)
    if not start:
        return ""
    start_idx = start.start()
    if end_pattern:
        end = re.search(re.escape(end_pattern), text[start_idx:], flags)
        if end:
            return text[start_idx : start_idx + end.start()]
    return text[start_idx:]


def extract_data(lattes_id: str):
    file_path = f"data/raw/projects/{lattes_id}.pdf"

    if not os.path.exists(file_path):
        return {
            "licenciamento_qtd": 0,
            "licenciamento_markdown": None,
            "servicos_qtd": 0,
            "servicos_markdown": None,
            "empresas_qtd": 0,
            "empresas_markdown": None,
            "demanda_qtd": 0,
            "demanda_markdown": None,
        }

    text = _load_text_from_pdf(file_path)
    parsed = chain.invoke({"text": text})
    return {
        "licenciamento_qtd": parsed.licenciamento_qtd,
        "licenciamento_markdown": parsed.licenciamento_markdown,
        "servicos_qtd": parsed.servicos_qtd,
        "servicos_markdown": parsed.servicos_markdown,
        "empresas_qtd": parsed.empresas_qtd,
        "empresas_markdown": parsed.empresas_markdown,
        "demanda_qtd": parsed.demanda_qtd,
        "demanda_markdown": parsed.demanda_markdown,
    }


def run_ai_extraction(researchers: pl.DataFrame):
    result = researchers.with_columns(
        pl.col("lattes_id")
        .map_elements(
            extract_data,
            return_dtype=pl.Struct(
                {
                    "licenciamento_qtd": pl.Int64,
                    "licenciamento_markdown": pl.Utf8,
                    "servicos_qtd": pl.Int64,
                    "servicos_markdown": pl.Utf8,
                    "empresas_qtd": pl.Int64,
                    "empresas_markdown": pl.Utf8,
                    "demanda_qtd": pl.Int64,
                    "demanda_markdown": pl.Utf8,
                }
            ),
        )
        .alias("extraction")
    ).unnest("extraction")

    return result
