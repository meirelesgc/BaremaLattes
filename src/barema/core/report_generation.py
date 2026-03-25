import os
from datetime import datetime

import polars as pl

from barema.services.ai_evaluation import evaluate_projects
from barema.services.ai_extraction import get_transfer_of_technology
from barema.services.ai_sumula import analyze_sumula
from barema.services.ai_tag import analyze_funding_agencies
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
    get_project_funding_agencies,
    get_research_projects,
    get_researchers,
    get_software,
)
from barema.services.report_utils import (
    add_evaluation_window,
    add_phd_level,
    merge_data,
    process_and_merge_production,
)

current_year = datetime.now().year

# fmt: off
CONFIG = [
    {"old_name": "nome", "new_name": "Nome", "default_value": ""},
    {"old_name": "lattes_id", "new_name": "Identificador Lattes", "default_value": ""},
    {"old_name": "h_index", "new_name": "Índice h", "default_value": 0},
    {"old_name": "last_update", "new_name": "Atualizado em", "default_value": ""},
    {"old_name": "tempo_doutorado", "new_name": "Tempo de doutorado", "default_value": "0"},
    {"old_name": "level", "new_name": "Nivel", "default_value": ""},
    {"old_name": "nivel_bolsa", "new_name": "Bolsa de produtividade", "default_value": "Não contemplado"},
    {"old_name": "window_years", "new_name": "Janela de analise", "default_value": 0},
    {"old_name": "total_articles", "new_name": "Total de artigos", "default_value": 0},
    {"old_name": "total_books", "new_name": "Total de livros", "default_value": 0},
    {"old_name": "total_software", "new_name": "Total de software", "default_value": 0},
    {"old_name": "total_no_reg_software", "new_name": "Total de software sem registro", "default_value": 0},
    {"old_name": "total_cultivar_patents", "new_name": "Total de patentes cultivar", "default_value": 0},
    {"old_name": "total_other_technical_production", "new_name": "Total de outra producao tecnica", "default_value": 0},
    {"old_name": "total_guidance_postdoc", "new_name": "Total orientação pós-doutorado", "default_value": 0},
    {"old_name": "total_phd_completed", "new_name": "Total doutorado concluído", "default_value": 0},
    {"old_name": "total_phd_ongoing", "new_name": "Total doutorado em andamento", "default_value": 0},
    {"old_name": "total_msc_completed", "new_name": "Total mestrado concluído", "default_value": 0},
    {"old_name": "total_msc_ongoing", "new_name": "Total mestrado em andamento", "default_value": 0},
    {"old_name": "coord_cientifico_tecnologico", "new_name": "Coordenação de projeto científico tecnológico", "default_value": 0},
    {"old_name": "membro_cientifico_tecnologico", "new_name": "Membro de projeto científico tecnológico", "default_value": 0},
    {"old_name": "coord_empresa", "new_name": "Coordenação de projeto com empresa", "default_value": 0},
    {"old_name": "membro_empresa", "new_name": "Membro de projeto com empresa", "default_value": 0},
    {"old_name": "coord_pesquisa", "new_name": "Coordenação de projeto de pesquisa", "default_value": 0},
    {"old_name": "membro_pesquisa", "new_name": "Membro de projeto de pesquisa", "default_value": 0},
    {"old_name": "licenciamento_qtd", "new_name": "Licenciamento", "default_value": 0},
    {"old_name": "servicos_qtd", "new_name": "Servicos", "default_value": 0},
    {"old_name": "empresas_qtd", "new_name": "Empresas", "default_value": 0},
    {"old_name": "demanda_qtd", "new_name": "Demanda", "default_value": 0},
    {"old_name": "licenciamento", "new_name": "Licenciamento - Descricao", "default_value": "Não encontrado"},
    {"old_name": "servicos", "new_name": "Servicos - Descricao", "default_value": "Não encontrado"},
    {"old_name": "empresas", "new_name": "Empresas - Descricao", "default_value": "Não encontrado"},
    {"old_name": "demanda", "new_name": "Demanda - Descricao", "default_value": "Não encontrado"},
    {"old_name": "publico_produto", "new_name": "Publico Produto", "default_value": "Não encontrado"},
    {"old_name": "objetivos_metas_relevancia", "new_name": "Objetivos Metas Relevancia", "default_value": "Não encontrado"},
    {"old_name": "metodologia_gestao", "new_name": "Metodologia Gestao", "default_value": "Não encontrado"},
    {"old_name": "colaboracoes_financiamento", "new_name": "Colaboracoes Financiamento", "default_value": "Não encontrado"},
    {"old_name": "potencial_inovacao_empreendedorismo", "new_name": "Potencial Inovacao Empreendedorismo", "default_value": "Não encontrado"},
    {"old_name": "demandas_escalabilidade", "new_name": "Demandas Escalabilidade", "default_value": "Não encontrado"},
    {"old_name": "maturidade_resultados", "new_name": "Maturidade Resultados", "default_value": "Não encontrado"},
    {"old_name": "organizacao_parcerias_extensao", "new_name": "Organizacao Parcerias Extensao", "default_value": "Não encontrado"},
    {"old_name": "perfil_tecnologico", "new_name": "Perfil Tecnologico", "default_value": "Não encontrado"},
]
# fmt: on


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
    researchers = get_researchers()
    foment_level = get_foment_level()

    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)

    df_final = get_transfer_of_technology(researchers)

    output_csv = "data/csv/transfer_of_technology.csv"
    output_xlsx = "data/csv/transfer_of_technology.xlsx"

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df_final.write_csv(output_csv)
    df_final.write_excel(output_xlsx)


def sumula_csv():
    researchers = get_researchers()
    foment_level = get_foment_level()

    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)

    df_final = analyze_sumula(researchers)

    output_csv = "data/csv/sumula.csv"
    output_xlsx = "data/csv/sumula.xlsx"

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df_final.write_csv(output_csv)
    df_final.write_excel(output_xlsx)


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
    researchers = get_researchers()
    foment_level = get_foment_level()

    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)

    df_final = evaluate_projects(researchers)

    output_csv = "data/csv/project_analysis.csv"
    output_xlsx = "data/csv/project_analysis.xlsx"

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df_final.write_csv(output_csv)
    df_final.write_excel(output_xlsx)


def _get_projects_base():
    agencies = get_project_funding_agencies()
    df_analyzed = analyze_funding_agencies(agencies)
    projects = get_research_projects()

    return (
        projects.explode("agency_names")
        .join(df_analyzed, left_on="agency_names", right_on="agency_name", how="left")
        .group_by(["project_id", "researcher_id", "year", "is_coordinator", "nature"])
        .agg(
            [
                pl.col("agency_names"),
                pl.col("company_or_organization").any().alias("has_company_funding"),
            ]
        )
    )


def get_coord_cientifico_tecnologico():
    df = _get_projects_base()
    df = df.filter(
        (pl.col("nature") != "PESQUISA")
        & (~pl.col("has_company_funding"))
        & (pl.col("is_coordinator"))
    )
    df = df.group_by(["researcher_id", "year"]).agg(pl.count().alias("qtd"))
    return df.with_columns(
        [
            pl.col("researcher_id").cast(pl.Utf8),
            pl.col("year").cast(pl.Int32),
            pl.col("qtd").cast(pl.Int64),
        ]
    )


def get_membro_cientifico_tecnologico():
    df = _get_projects_base()
    df = df.filter(
        (pl.col("nature") != "PESQUISA")
        & (~pl.col("has_company_funding"))
        & (~pl.col("is_coordinator"))
    )
    df = df.group_by(["researcher_id", "year"]).agg(pl.count().alias("qtd"))
    return df.with_columns(
        [
            pl.col("researcher_id").cast(pl.Utf8),
            pl.col("year").cast(pl.Int32),
            pl.col("qtd").cast(pl.Int64),
        ]
    )


def get_coord_empresa():
    df = _get_projects_base()
    df = df.filter((pl.col("has_company_funding")) & (pl.col("is_coordinator")))
    df = df.group_by(["researcher_id", "year"]).agg(pl.count().alias("qtd"))
    return df.with_columns(
        [
            pl.col("researcher_id").cast(pl.Utf8),
            pl.col("year").cast(pl.Int32),
            pl.col("qtd").cast(pl.Int64),
        ]
    )


def get_membro_empresa():
    df = _get_projects_base()
    df = df.filter((pl.col("has_company_funding")) & (~pl.col("is_coordinator")))
    df = df.group_by(["researcher_id", "year"]).agg(pl.count().alias("qtd"))
    return df.with_columns(
        [
            pl.col("researcher_id").cast(pl.Utf8),
            pl.col("year").cast(pl.Int32),
            pl.col("qtd").cast(pl.Int64),
        ]
    )


def get_coord_pesquisa():
    df = _get_projects_base()
    df = df.filter(
        (pl.col("nature") == "PESQUISA")
        & (~pl.col("has_company_funding"))
        & (pl.col("is_coordinator"))
    )
    df = df.group_by(["researcher_id", "year"]).agg(pl.count().alias("qtd"))
    return df.with_columns(
        [
            pl.col("researcher_id").cast(pl.Utf8),
            pl.col("year").cast(pl.Int32),
            pl.col("qtd").cast(pl.Int64),
        ]
    )


def get_membro_pesquisa():
    df = _get_projects_base()
    df = df.filter(
        (pl.col("nature") == "PESQUISA")
        & (~pl.col("has_company_funding"))
        & (~pl.col("is_coordinator"))
    )
    df = df.group_by(["researcher_id", "year"]).agg(pl.count().alias("qtd"))
    return df.with_columns(
        [
            pl.col("researcher_id").cast(pl.Utf8),
            pl.col("year").cast(pl.Int32),
            pl.col("qtd").cast(pl.Int64),
        ]
    )


def participation_in_project_csv():
    researchers = get_researchers()
    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)
    researchers = add_evaluation_window(researchers)

    productions_to_process = [
        (get_coord_cientifico_tecnologico, "coord_cientifico_tecnologico"),
        (get_membro_cientifico_tecnologico, "membro_cientifico_tecnologico"),
        (get_coord_empresa, "coord_empresa"),
        (get_membro_empresa, "membro_empresa"),
        (get_coord_pesquisa, "coord_pesquisa"),
        (get_membro_pesquisa, "membro_pesquisa"),
    ]

    for get_func, col_name in productions_to_process:
        researchers = process_and_merge_production(
            researchers, get_func, col_name, current_year
        )

    researchers.write_csv("data/csv/participation_in_project.csv")
    researchers.write_excel("data/csv/participation_in_project.xlsx")


def merge_all_reports():
    files_to_merge = [
        "data/csv/researcher_profile.csv",
        "data/csv/technological_production_and_innovation.csv",
        "data/csv/transfer_of_technology.csv",
        "data/csv/project_analysis.csv",
        "data/csv/human_resources.csv",
        "data/csv/participation_in_project.csv",
        "data/csv/sumula.csv",
    ]

    dfs = [pl.read_csv(f) for f in files_to_merge]
    df_final = dfs[0]

    for df in dfs[1:]:
        overlapping_cols = [
            col
            for col in df.columns
            if col in df_final.columns and col != "researcher_id"
        ]
        df_to_join = df.drop(overlapping_cols)
        df_final = df_final.join(df_to_join, on="researcher_id", how="left")

    for item in CONFIG:
        col_name = item["old_name"]
        default_val = item["default_value"]

        if col_name not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(default_val).alias(col_name))
        else:
            df_final = df_final.with_columns(pl.col(col_name).fill_null(default_val))

    selected_cols = [item["old_name"] for item in CONFIG]
    df_final = df_final.select(selected_cols)

    rename_mapping = {
        item["old_name"]: item["new_name"]
        for item in CONFIG
        if item["old_name"] != item["new_name"]
    }
    if rename_mapping:
        df_final = df_final.rename(rename_mapping)

    df_final.write_csv("data/csv/output/unified_report.csv")
    df_final.write_excel("data/csv/output/unified_report.xlsx")


def generate_final_report():
    os.makedirs("data/csv/output", exist_ok=True)

    researcher_profile_csv()
    technological_production_and_innovation_csv()
    transfer_of_technology_csv()
    project_analysis_csv()
    human_resources_csv()
    participation_in_project_csv()
    sumula_csv()

    merge_all_reports()


if __name__ == "__main__":
    generate_final_report()
