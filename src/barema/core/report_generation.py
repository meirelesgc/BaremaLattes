import os
from datetime import datetime

import polars as pl
from tqdm import tqdm

from barema.services.ai_evaluation import evaluation
from barema.services.ai_extraction import extract_data
from barema.services.queries import (
    get_articles,
    get_books,
    get_cultivar_patents,
    get_foment_level,
    get_guidance_postdoc,
    get_msc_completed,
    get_msc_ongoing,
    get_no_reg_software,
    get_other_technical_production,
    get_phd_completed,
    get_phd_ongoing,
    get_phd_time,
    get_projects_with_companies,
    get_research_projects,
    get_researchers,
    get_scientific_projects,
    get_software,
)
from barema.services.report_utils import (
    add_evaluation_window,
    add_phd_level,
    merge_data,
    process_and_merge_production,
)

current_year = datetime.now().year


FILES = [
    "data/csv/researcher_profile.csv",
    "data/csv/technological_production_and_innovation.csv",
    "data/csv/transfer_of_technology.csv",
    "data/csv/participation_in_project.csv",
    "data/csv/human_resources.csv",
    "data/csv/project_analysis.csv",
]

CONFIG = [
    {
        "original": "last_update",
        "new": "Ultima atualização",
        "default": "Erro",
    },
    {
        "original": "nome",
        "new": "Nome",
        "default": "Não informado",
    },
    {
        "original": "lattes_id",
        "new": "Identificador Lattes",
        "default": "Não informado",
    },
    {"original": "_", "new": "Avaliador", "default": "Não informado"},
    {"original": "_", "new": "Segundo olhar", "default": "Não informado"},
    {"original": "_", "new": "Classificação", "default": "Não informado"},
    {"original": "_", "new": "Projeto", "default": "Não informado"},
    {"original": "_", "new": "Proponente", "default": "Não informado"},
    {"original": "_", "new": "Súmula realizações", "default": "Não extraido"},
    {
        "original": "_",
        "new": "Transferência tecnologia impacto",
        "default": "Não extraido",
    },
    {"original": "_", "new": "Observação inicial", "default": "Não informado"},
    {"original": "_", "new": "Extensão inovadora", "default": "Não informado"},
    {"original": "_", "new": "Coeficiente súmula", "default": "Não informado"},
    {"original": "_", "new": "Ad hocs", "default": "Não informado"},
    {"original": "tempo_doutorado", "new": "Tempo doutorado", "default": 0},
    {"original": "nivel_bolsa", "new": "Nível bolsista", "default": 0},
    {"original": "h_index", "new": "Fator h", "default": 0},
    {"original": "_", "new": "Maternidade adoção", "default": "Não extraido"},
    {"original": "total_articles", "new": "Artigos periódicos", "default": 0},
    {"original": "total_books", "new": "Livros capítulos", "default": 0},
    {"original": "total_software", "new": "Software com registro", "default": 0},
    {"original": "total_no_reg_software", "new": "Software sem registro", "default": 0},
    {"original": "total_cultivar_patents", "new": "Patentes cultivares", "default": 0},
    {
        "original": "total_other_technical_production",
        "new": "Outros produtos tecnológicos",
        "default": 0,
    },
    {"original": "_", "new": "Soma produção tecnológica", "default": 0},
    {"original": "_", "new": "Nota produção tecnológica", "default": 0},
    {"original": "_", "new": "Nota coeficiente súmula produção", "default": 0},
    {
        "original": "licenciamento_qtd",
        "new": "Licenciamento transferência",
        "default": 0,
    },
    {"original": "servicos_qtd", "new": "Serviços tecnológicos", "default": 0},
    {"original": "empresas_qtd", "new": "Empresas terceiro setor", "default": 0},
    {"original": "demanda_qtd", "new": "Demanda", "default": 0},
    {"original": "_", "new": "Carta", "default": 0},
    {"original": "_", "new": "Soma transferência tecnologia", "default": 0},
    {"original": "_", "new": "Nota transferência tecnologia", "default": 0},
    {
        "original": "total_scientific_projects",
        "new": "Coord projetos científicos",
        "default": 0,
    },
    {
        "original": "total_projects_with_companies",
        "new": "Coord projetos empresas",
        "default": 0,
    },
    {
        "original": "total_research_projects",
        "new": "Coord projetos pesquisa",
        "default": 0,
    },
    {"original": "_", "new": "Soma projetos", "default": 0},
    {"original": "_", "new": "Nota projetos", "default": 0},
    {"original": "_", "new": "Nota coeficiente súmula projetos", "default": 0},
    {"original": "total_guidance_postdoc", "new": "Pós doutorado", "default": 0},
    {"original": "total_phd_completed", "new": "Doutorado concluído", "default": 0},
    {"original": "total_phd_ongoing", "new": "Doutorado andamento", "default": 0},
    {"original": "total_msc_completed", "new": "Mestrado concluído", "default": 0},
    {"original": "total_msc_ongoing", "new": "Mestrado andamento", "default": 0},
    {"original": "_", "new": "Bolsas tecnológicas", "default": "Não extraido"},
    {"original": "_", "new": "Bolsas ic outras", "default": "Não extraido"},
    {
        "original": "_",
        "new": "Organização programas formação",
        "default": "Não extraido",
    },
    {"original": "_", "new": "Capacitação rh", "default": "Não extraido"},
    {"original": "_", "new": "Nota rh", "default": "Não extraido"},
    {"original": "_", "new": "Nota coeficiente súmula rh", "default": "Não extraido"},
    {
        "original": "publico_produto",
        "new": "Público alvo produto",
        "default": "Projeto não encontrado",
    },
    {
        "original": "objetivos_metas_relevancia",
        "new": "Objetivos metas relevância",
        "default": "Projeto não encontrado",
    },
    {
        "original": "metodologia_gestao",
        "new": "Metodologia gestão",
        "default": "Projeto não encontrado",
    },
    {
        "original": "colaboracoes_financiamento",
        "new": "Colaborações financiamento",
        "default": "Projeto não encontrado",
    },
    {
        "original": "potencial_inovacao_empreendedorismo",
        "new": "Potencial inovação",
        "default": "Projeto não encontrado",
    },
    {
        "original": "demandas_escalabilidade",
        "new": "Demandas escalabilidade",
        "default": "Projeto não encontrado",
    },
    {
        "original": "maturidade_resultados",
        "new": "Maturidade resultados",
        "default": "Projeto não encontrado",
    },
    {
        "original": "organizacao_parcerias_extensao",
        "new": "Organização parcerias extensão",
        "default": "Projeto não encontrado",
    },
    {
        "original": "perfil_tecnologico",
        "new": "Perfil tecnológico",
        "default": "Projeto não encontrado",
    },
    {"original": "_", "new": "Usou formulário", "default": 0},
    {"original": "_", "new": "Observação projeto", "default": 0},
    {"original": "_", "new": "Nota foco desenvolvimento", "default": 0},
    {"original": "_", "new": "Parecer final", "default": 0},
    {"original": "_", "new": "Nota final com súmula", "default": 0},
    {"original": "_", "new": "Nota final sem súmula", "default": 0},
    {"original": "_", "new": "Observação final", "default": 0},
]


def researcher_profile_csv(base_year=current_year):
    researchers = get_researchers()
    phd_time = get_phd_time()
    researchers = merge_data(researchers, phd_time)
    phd_level = add_phd_level(phd_time)
    researchers = merge_data(researchers, phd_level)
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers.write_csv("data/csv/researcher_profile.csv")
    researchers.write_excel("data/csv/researcher_profile.xlsx")


def technological_production_and_innovation_csv(base_year=current_year):
    researchers = get_researchers()
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)
    productions_to_process = [
        (get_articles, "total_articles"),
        (get_books, "total_books"),
        (get_software, "total_software"),
        (get_no_reg_software, "total_no_reg_software"),
        (get_cultivar_patents, "total_cultivar_patents"),
        (get_other_technical_production, "total_other_technical_production"),
    ]
    for get_func, col_name in productions_to_process:
        researchers = process_and_merge_production(
            researchers, get_func, col_name, base_year
        )
    researchers.write_csv("data/csv/technological_production_and_innovation.csv")
    researchers.write_excel("data/csv/technological_production_and_innovation.xlsx")


def transfer_of_technology_csv():
    def get_processed_ids(path):
        if not os.path.exists(path):
            return set()
        df = pl.read_csv(path, schema_overrides={"lattes_id": pl.String})
        return set(df["lattes_id"].to_list())

    researchers = get_researchers()
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)
    researchers = researchers.with_columns(pl.col("lattes_id").cast(pl.String))
    output_path = "data/csv/transfer_of_technology.csv"
    processed = get_processed_ids(output_path)
    rows = list(researchers.iter_rows(named=True))

    for row in tqdm(
        rows, desc="Extraindo Transferência de Tecnologia", total=len(rows)
    ):
        lattes_id = str(row["lattes_id"])
        if lattes_id in processed:
            continue
        resultado = extract_data(lattes_id)
        linha = {**row, **resultado}
        df = pl.DataFrame([linha]).with_columns(pl.col("lattes_id").cast(pl.String))
        if not os.path.exists(output_path):
            df.write_csv(output_path)
        else:
            with open(output_path, "a", encoding="utf-8") as f:
                df.write_csv(f, include_header=False)

    if os.path.exists(output_path):
        df_final = pl.read_csv(output_path, schema_overrides={"lattes_id": pl.String})
        df_final.write_excel("data/csv/transfer_of_technology.xlsx")


def participation_in_project_csv(base_year=current_year):
    researchers = get_researchers()
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)
    productions_to_process = [
        (get_scientific_projects, "total_scientific_projects"),
        (get_projects_with_companies, "total_projects_with_companies"),
        (get_research_projects, "total_research_projects"),
    ]
    for get_func, col_name in productions_to_process:
        researchers = process_and_merge_production(
            researchers, get_func, col_name, base_year
        )
    researchers.write_csv("data/csv/participation_in_project.csv")
    researchers.write_excel("data/csv/participation_in_project.xlsx")


def human_resources_csv():
    researchers = get_researchers()
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)
    productions_to_process = [
        (get_guidance_postdoc, "total_guidance_postdoc"),
        (get_phd_completed, "total_phd_completed"),
        (get_phd_ongoing, "total_phd_ongoing"),
        (get_msc_completed, "total_msc_completed"),
        (get_msc_ongoing, "total_msc_ongoing"),
    ]
    for get_func, col_name in productions_to_process:
        researchers = process_and_merge_production(researchers, get_func, col_name, 0)
    researchers.write_csv("data/csv/human_resources.csv")
    researchers.write_excel("data/csv/human_resources.xlsx")


def project_analysis_csv(base_year=current_year):
    def get_processed_ids(path):
        if not os.path.exists(path):
            return set()
        df = pl.read_csv(path, schema_overrides={"lattes_id": pl.String})
        return set(df["lattes_id"].to_list())

    researchers = get_researchers()
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)
    researchers = researchers.with_columns(pl.col("lattes_id").cast(pl.String))
    output_path = "data/csv/project_analysis.csv"
    processed = get_processed_ids(output_path)
    rows = list(researchers.iter_rows(named=True))

    for row in tqdm(rows, desc="Analisando projetos", total=len(rows)):
        lattes_id = str(row["lattes_id"])
        if lattes_id in processed:
            continue
        resultado = evaluation(lattes_id)
        linha = {**row, **resultado}
        df = pl.DataFrame([linha]).with_columns(pl.col("lattes_id").cast(pl.String))
        if not os.path.exists(output_path):
            df.write_csv(output_path)
        else:
            with open(output_path, "a", encoding="utf-8") as f:
                df.write_csv(f, include_header=False)


def translate_file(file_name, config):
    input_csv = f"data/csv/{file_name}.csv"
    output_csv = f"data/csv/output/{file_name}.csv"
    output_xlsx = f"data/csv/output/{file_name}.xlsx"

    if not os.path.exists(input_csv):
        return

    df = pl.read_csv(input_csv, infer_schema_length=None)

    rename_map = {}
    for item in config:
        original = item["original"]
        new = item["new"]
        default = item["default"]

        if original != "_" and original in df.columns:
            df = df.with_columns(pl.col(original).fill_null(default))
            rename_map[original] = new

    df = df.rename(rename_map)

    seen = set()
    dedup_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in seen:
            dedup_map[col] = f"{col}_dup"
        else:
            seen.add(col_lower)

    if dedup_map:
        df = df.rename(dedup_map)

    df.write_csv(output_csv)
    df.write_excel(output_xlsx)


def load_and_merge():
    dfs = []

    for file in FILES:
        if os.path.exists(file):
            df = pl.read_csv(file, schema_overrides={"lattes_id": pl.String})
            dfs.append(df)

    base = dfs[0]

    for df in dfs[1:]:
        new_cols = [c for c in df.columns if c not in base.columns or c == "lattes_id"]
        df = df.select(new_cols)
        base = base.join(df, on="lattes_id", how="left")

    return base


def apply_config(df):
    expressions = []

    for item in CONFIG:
        original = item["original"]
        new = item["new"]
        default = item["default"]

        if original != "_" and original in df.columns:
            expressions.append(pl.col(original).fill_null(default).alias(new))
        else:
            expressions.append(pl.lit(default).alias(new))

    return df.select(expressions)


def generate_final_report():
    os.makedirs("data/csv/output", exist_ok=True)

    researcher_profile_csv()
    technological_production_and_innovation_csv()
    transfer_of_technology_csv()
    participation_in_project_csv()
    human_resources_csv()
    project_analysis_csv()

    df = load_and_merge()
    df = apply_config(df)

    os.makedirs("data/final", exist_ok=True)

    df.write_csv("data/final/barema_final.csv")
    df.write_excel("data/final/barema_final.xlsx")


if __name__ == "__main__":
    generate_final_report()
