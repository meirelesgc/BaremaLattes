import polars as pl
from sqlalchemy import text

from baremalattes.database.connection import get_session


def get_pesquisadores():
    session = get_session()
    query = """
    SELECT id::text AS researcher_id, name AS nome, lattes_id
    FROM researcher;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_tempo_doutorado():
    session = get_session()
    query = """
    SELECT researcher_id::text,
        (EXTRACT(YEAR FROM CURRENT_DATE) - education_end)::INT AS tempo_doutorado
    FROM education
    WHERE degree = 'DOCTORATE'
        AND education_end IS NOT NULL;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_nivel_bolsistas():
    session = get_session()
    query = """
    SELECT researcher_id::text, foment.category_level_code AS nivel_bolsa
    FROM foment;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_artigos_em_periodicos():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM bibliographic_production
    WHERE type = 'ARTICLE' AND year IS NOT NULL
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_livros_e_capitulos():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM bibliographic_production
    WHERE type IN ('BOOK', 'BOOK_CHAPTER') AND year IS NOT NULL
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_software():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM software
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_patentes():
    session = get_session()
    query = """
    SELECT researcher_id::text, development_year::int AS year, COUNT(*) as qtd
    FROM patent
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def get_desenhos_industriais_ou_marcas():
    session = get_session()
    query = """
    WITH combined_data AS (
        SELECT researcher_id::text, year::int
        FROM industrial_design
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM brand
    )
    SELECT researcher_id, year, COUNT(*) as qtd
    FROM combined_data
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    return pl.DataFrame(data)


def adicionar_janela_avaliacao(df_pesquisadores):
    niveis_10_anos = ['1A', '1B', 'SR']

    return df_pesquisadores.with_columns(
        pl
        .when(pl.col('nivel_bolsa').is_in(niveis_10_anos))
        .then(10)
        .otherwise(5)
        .alias('janela_anos')
    )


def filtrar_por_janela(df_producao, df_pesquisadores, ano_base=2026):
    df_joined = df_producao.join(
        df_pesquisadores.select(['researcher_id', 'janela_anos']),
        on='researcher_id',
        how='inner',
    )

    df_filtrado = df_joined.filter(
        (ano_base - pl.col('year')) <= pl.col('janela_anos')
    )

    return df_filtrado.drop('janela_anos')


def merge_data(main_df, extra_df):
    return main_df.join(extra_df, on='researcher_id', how='left')


def adicionar_nivel_doutorado(df_tempo):
    CLASS_C = 2
    CLASS_A_B = 6

    df_com_nivel = df_tempo.with_columns(
        pl
        .when(pl.col('tempo_doutorado') <= CLASS_C)
        .then(pl.lit(['C']))
        .when(pl.col('tempo_doutorado') >= CLASS_A_B)
        .then(pl.lit(['A', 'B']))
        .otherwise(pl.lit(['B']))
        .alias('nivel')
    )

    return df_com_nivel


def processar_e_mesclar_producao(
    df_pesquisadores, func_get_dados, nome_coluna_total, ano_base=2026
):
    df_dados = func_get_dados()
    df_filtrado = filtrar_por_janela(
        df_dados, df_pesquisadores, ano_base=ano_base
    )

    df_agrupado = df_filtrado.group_by('researcher_id').agg(
        pl.col('qtd').sum().alias(nome_coluna_total)
    )

    return merge_data(df_pesquisadores, df_agrupado)


def calcular_pontuacao_tecnologica(df_pesquisadores):
    colunas_tecnologicas = [
        'total_software_validos',
        'total_patentes_validas',
        'total_desenhos_industriais_ou_marcas_validas',
    ]

    df = df_pesquisadores.with_columns(pl.col(colunas_tecnologicas).fill_null(0))

    df = df.with_columns(
        pl.sum_horizontal(colunas_tecnologicas).alias(
            'total_producao_tec_inovacao'
        )
    )

    tem_registro_patente = (pl.col('total_patentes_validas') > 0) | (
        pl.col('total_desenhos_industriais_ou_marcas_validas') > 0
    )

    df = df.with_columns(
        pl
        .when(
            (pl.col('total_producao_tec_inovacao') > 30) & tem_registro_patente
        )
        .then(8)
        .when(pl.col('total_producao_tec_inovacao') >= 30)
        .then(5)
        .when(pl.col('total_producao_tec_inovacao') >= 10)
        .then(2)
        .otherwise(0)
        .alias('nota_base_producao_tec')
    )

    df = df.with_columns(
        (pl.col('nota_base_producao_tec') * 3).alias(
            'pontuacao_final_tec_peso_3'
        )
    )

    return df


def run_report_process(ano_base=2026):
    pesquisadores = get_pesquisadores()

    tempo_doutorado = adicionar_nivel_doutorado(get_tempo_doutorado())
    pesquisadores = merge_data(pesquisadores, tempo_doutorado)

    nivel_bolsistas = get_nivel_bolsistas()
    pesquisadores = merge_data(pesquisadores, nivel_bolsistas)

    pesquisadores = adicionar_janela_avaliacao(pesquisadores)

    producoes_para_processar = [
        (get_artigos_em_periodicos, 'total_artigos_validos'),
        (get_livros_e_capitulos, 'total_livros_validos'),
        (get_software, 'total_software_validos'),
        (get_patentes, 'total_patentes_validas'),
        (
            get_desenhos_industriais_ou_marcas,
            'total_desenhos_industriais_ou_marcas_validas',
        ),
    ]

    for func_get, nome_coluna in producoes_para_processar:
        pesquisadores = processar_e_mesclar_producao(
            pesquisadores, func_get, nome_coluna, ano_base
        )

    pesquisadores = calcular_pontuacao_tecnologica(pesquisadores)

    pesquisadores = pesquisadores.with_columns(
        pl.lit('Segundo bloco em desenvolvimento').alias('status_etapa_2')
    )

    pesquisadores.write_excel('relatorio.xlsx')
