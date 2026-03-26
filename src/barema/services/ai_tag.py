import os

import polars as pl
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from tqdm import tqdm

from barema.core.settings import Settings

SETTINGS = Settings()

CACHE_DIR = "data/raw/cache"
CSV_PATH = os.path.join(CACHE_DIR, "agencies_cache.csv")
XLSX_PATH = os.path.join(CACHE_DIR, "agencies_cache.xlsx")


def evaluate_agency(agency_name: str) -> bool:
    llm = ChatOpenAI(api_key=SETTINGS.OPENAI_API_KEY, model="gpt-5-nano", temperature=0)

    prompt = (
        "Estou classificando projetos acadêmicos segundo critérios de avaliação (barema). "
        "Preciso identificar se o financiador caracteriza um projeto com empresa ou organização externa ao setor acadêmico/público. "
        f"Agência: {agency_name}. "
        "Classifique a instituição como:\n"
        "- True: empresa privada, ONG, fundação privada ou entidade não governamental\n"
        "- False: agência pública, universidade, instituto público, ministério\n"
        "Exemplos False: CNPq, CAPES, FAPESP, FINEP, MEC\n"
        "Exemplos True: Petrobras, Microsoft\n"
        "Responda apenas 'True' ou 'False'."
    )

    resposta = llm.invoke([HumanMessage(content=prompt)])
    return resposta.content.strip().lower() == "true"


def load_cache():
    if os.path.exists(CSV_PATH):
        return pl.read_csv(CSV_PATH)

    return pl.DataFrame(
        schema={"agency_name": pl.Utf8, "company_or_organization": pl.Boolean}
    )


def save_cache(df: pl.DataFrame):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.write_csv(CSV_PATH)
    df.write_excel(XLSX_PATH)


def analyze_funding_agencies(df_agencies: pl.DataFrame) -> pl.DataFrame:
    cache = load_cache()

    cached_names = set(cache["agency_name"].to_list())

    new_agencies = [
        a for a in df_agencies["agency_name"].to_list() if a not in cached_names
    ]

    results = []

    for agency in tqdm(new_agencies, desc="Avaliando novas"):
        result = evaluate_agency(agency)
        results.append({"agency_name": agency, "company_or_organization": result})

    if results:
        df_new = pl.DataFrame(
            results,
            schema={"agency_name": pl.Utf8, "company_or_organization": pl.Boolean},
        )
        cache = pl.concat([cache, df_new], how="vertical")
        cache = cache.unique(subset=["agency_name"], keep="last")
        save_cache(cache)

    if "company_or_organization" in df_agencies.columns:
        df_agencies = df_agencies.drop("company_or_organization")

    df_final = df_agencies.join(cache, on="agency_name", how="left")

    return df_final


if __name__ == "__main__":

    def project_funding_agencies():
        data = [
            {"agency_name": "FAPESP", "company_or_organization": None},
            {"agency_name": "Petrobras", "company_or_organization": None},
            {"agency_name": "CNPq", "company_or_organization": None},
            {"agency_name": "Microsoft", "company_or_organization": None},
        ]
        schema = {"agency_name": pl.Utf8, "company_or_organization": pl.Boolean}
        return pl.DataFrame(data, schema=schema)

    df_agencies = project_funding_agencies()
    df_analyzed = analyze_funding_agencies(df_agencies)

    print(df_analyzed)
