import polars as pl

from baremalattes.database.connection import get_session as get_session
from baremalattes.report.ai_evaluation import run_aieval_process
from baremalattes.report.metrics import (
    get_articles,
    get_assets_ip,
    get_books_chapters,
    get_foment_level,
    get_guidance_postdoc,
    get_msc_completed,
    get_msc_ongoing,
    get_patents,
    get_phd_completed,
    get_phd_ongoing,
    get_phd_time,
    get_researchers,
    get_software,
)


def add_evaluation_window(df_researchers):
    levels_10_years = ['1A', '1B', 'SR']

    return df_researchers.with_columns(
        pl
        .when(pl.col('nivel_bolsa').is_in(levels_10_years))
        .then(10)
        .otherwise(5)
        .alias('window_years')
    )


def filter_by_window(df_production, df_researchers, base_year=2026):
    df_joined = df_production.join(
        df_researchers.select(['researcher_id', 'window_years']),
        on='researcher_id',
        how='inner',
    )

    df_filtered = df_joined.filter(
        (base_year - pl.col('year')) <= pl.col('window_years')
    )

    return df_filtered.drop('window_years')


def merge_data(main_df, extra_df):
    return main_df.join(extra_df, on='researcher_id', how='left')


def add_phd_level(df_time):
    CLASS_C = 2
    CLASS_A_B = 6

    df_with_level = df_time.with_columns(
        pl
        .when(pl.col('tempo_doutorado') <= CLASS_C)
        .then(pl.lit('C'))
        .when(pl.col('tempo_doutorado') >= CLASS_A_B)
        .then(pl.lit('A'))
        .otherwise(pl.lit('B'))
        .alias('level')
    )

    return df_with_level


def process_and_merge_production(
    df_researchers, get_data_func, total_col_name, base_year=2026
):
    df_data = get_data_func()
    df_filtered = filter_by_window(df_data, df_researchers, base_year=base_year)

    df_grouped = df_filtered.group_by('researcher_id').agg(
        pl.col('qtd').sum().alias(total_col_name)
    )

    return merge_data(df_researchers, df_grouped)


def run_report_process(base_year=2026):
    researchers = get_researchers()

    phd_time = add_phd_level(get_phd_time())
    researchers = merge_data(researchers, phd_time)

    foment_level = get_foment_level()
    researchers = merge_data(researchers, foment_level)

    researchers = add_evaluation_window(researchers)

    productions_to_process = [
        (get_articles, 'total_valid_articles'),
        (get_books_chapters, 'total_valid_books_chapters'),
        (get_software, 'total_valid_software'),
        (get_patents, 'total_valid_patents'),
        (get_assets_ip, 'total_valid_assets_ip'),
        (get_guidance_postdoc, 'total_valid_guidance_postdoc'),
        (get_phd_completed, 'total_valid_phd_completed'),
        (get_phd_ongoing, 'total_valid_phd_ongoing'),
        (get_msc_completed, 'total_valid_msc_completed'),
        (get_msc_ongoing, 'total_valid_msc_ongoing'),
    ]

    for get_func, col_name in productions_to_process:
        researchers = process_and_merge_production(
            researchers, get_func, col_name, base_year
        )

    researchers = run_aieval_process(researchers)

    researchers.write_excel('relatorio.xlsx')
