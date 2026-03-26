import json
import os

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from barema.core.settings import Settings

SETTINGS = Settings()

CACHE_DIR = "data/raw/cache"
CSV_PATH = os.path.join(CACHE_DIR, "transfer_tech_cache.csv")
XLSX_PATH = os.path.join(CACHE_DIR, "transfer_tech_cache.xlsx")

llm = ChatOpenAI(
    api_key=SETTINGS.OPENAI_API_KEY,
    model="gpt-5-mini",
    temperature=0,
    model_kwargs={"response_format": {"type": "json_object"}},
)

CRITERIOS = {
    "licenciamento": "Licenciamento",
    "servicos": "Serviços",
    "empresas": "Empresas/Outros",
    "demanda": "Demanda",
}


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def gerar_prompt(chaves, texto):
    criterios_texto = "\n".join(
        [f"{i + 1}) {CRITERIOS[k]}" for i, k in enumerate(chaves)]
    )
    chaves_json = ", ".join(
        [f'"{k}_qtd" (int), "{k}" (string ou null)' for k in chaves]
    )

    return f"""Você está recebendo o texto de um projeto elaborado por um pesquisador.

Para cada critério abaixo:

{criterios_texto}

Faça:

- Identifique evidências claras no texto.
- Conte quantas evidências distintas existem.
- Enumere separado por ponto e virgula (;) de forma coesa e descritiva.

Se não houver evidência:
- Retorne quantidade 0
- Retorne null para o markdown

A quantidade deve refletir o número real de evidências distintas encontradas no texto.

Responda SOMENTE com um objeto JSON válido contendo exatamente as seguintes chaves:
{chaves_json}.

Texto:
{texto}
"""


prompt_template_attachment = """
Você receberá o texto extraído dos anexos do formulário do projeto ou da súmula.

Faça:
1. Verifique se existe menção ou transcrição de uma carta de apoio da instituição de ensino ou de pesquisa a qual o proponente pertence. Retorne um valor booleano.
2. Gere um texto resumindo os comentários gerais e o conteúdo dos anexos do projeto.

Responda SOMENTE com um objeto JSON válido contendo exatamente as seguintes chaves:
"carta_apoio" (bool), "comentarios_anexos" (string ou null).

Texto:
{text}
"""


def _load_text_from_pdf(file_path: str) -> str:
    loader = PyMuPDFLoader(file_path, mode="single")
    documents = loader.load()
    texts = [d.page_content for d in documents]
    return "\n".join(texts)


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

    if not new_ids:
        return df_researchers.join(cache, on="lattes_id", how="left")

    inputs_gerais = []
    lattes_validos = []
    textos_cache = {}

    for lattes_id in new_ids:
        file_path = f"data/raw/projects/{lattes_id}.pdf"
        if os.path.exists(file_path):
            text = _load_text_from_pdf(file_path)
            textos_cache[lattes_id] = text
            prompt = gerar_prompt(list(CRITERIOS.keys()), text)
            inputs_gerais.append([HumanMessage(content=prompt)])
            lattes_validos.append(lattes_id)

    resultados_iniciais = {}
    if inputs_gerais:
        respostas_gerais = llm.batch(inputs_gerais)
        for lattes_id, resposta in zip(lattes_validos, respostas_gerais):
            try:
                dados = json.loads(resposta.content)
                for chave in CRITERIOS.keys():
                    dados[f"{chave}_qtd"] = to_int(dados.get(f"{chave}_qtd"))
                resultados_iniciais[lattes_id] = dados
            except Exception:
                resultados_iniciais[lattes_id] = {}

    inputs_fallback = []
    fallback_map = []

    for lattes_id in lattes_validos:
        dados = resultados_iniciais.get(lattes_id, {})
        text = textos_cache[lattes_id]
        for chave in CRITERIOS.keys():
            if to_int(dados.get(f"{chave}_qtd")) == 0:
                prompt = gerar_prompt([chave], text)
                inputs_fallback.append([HumanMessage(content=prompt)])
                fallback_map.append((lattes_id, chave))

    if inputs_fallback:
        respostas_fallback = llm.batch(inputs_fallback)
        for (lattes_id, chave), resposta in zip(fallback_map, respostas_fallback):
            try:
                dados_fb = json.loads(resposta.content)
                qtd_fb = to_int(dados_fb.get(f"{chave}_qtd"))
                if qtd_fb > 0:
                    resultados_iniciais[lattes_id][f"{chave}_qtd"] = qtd_fb
                    resultados_iniciais[lattes_id][chave] = dados_fb.get(chave)
            except Exception:
                pass

    inputs_anexos = []
    lattes_anexos = []

    for lattes_id in new_ids:
        attachment_path = f"data/raw/projects/attachment/{lattes_id}.pdf"
        if os.path.exists(attachment_path):
            text = _load_text_from_pdf(attachment_path)
            prompt = prompt_template_attachment.format(text=text)
            inputs_anexos.append([HumanMessage(content=prompt)])
            lattes_anexos.append(lattes_id)

    resultados_anexos = {}
    if inputs_anexos:
        respostas_anexos = llm.batch(inputs_anexos)
        for lattes_id, resposta in zip(lattes_anexos, respostas_anexos):
            try:
                resultados_anexos[lattes_id] = json.loads(resposta.content)
            except Exception:
                resultados_anexos[lattes_id] = {}

    results = []
    for lattes_id in new_ids:
        dados_gerais = resultados_iniciais.get(lattes_id, {})
        dados_anexos = resultados_anexos.get(lattes_id, {})

        result = {
            "lattes_id": lattes_id,
            "licenciamento_qtd": to_int(dados_gerais.get("licenciamento_qtd")),
            "licenciamento": dados_gerais.get("licenciamento"),
            "servicos_qtd": to_int(dados_gerais.get("servicos_qtd")),
            "servicos": dados_gerais.get("servicos"),
            "empresas_qtd": to_int(dados_gerais.get("empresas_qtd")),
            "empresas": dados_gerais.get("empresas"),
            "demanda_qtd": to_int(dados_gerais.get("demanda_qtd")),
            "demanda": dados_gerais.get("demanda"),
            "carta_apoio": dados_anexos.get("carta_apoio", False),
            "comentarios_anexos": dados_anexos.get("comentarios_anexos"),
        }
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
