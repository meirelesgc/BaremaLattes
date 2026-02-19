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


def run_report_process():
    pesquisadores = get_pesquisadores()

    _tempo_doutorado = get_tempo_doutorado()
    tempo_doutorado = adicionar_nivel_doutorado(_tempo_doutorado)
    pesquisadores = merge_data(pesquisadores, tempo_doutorado)

    _nivel_bolsistas = get_nivel_bolsistas()
    pesquisadores = merge_data(pesquisadores, _nivel_bolsistas)

    pesquisadores = adicionar_janela_avaliacao(pesquisadores)

    _artigos = get_artigos_em_periodicos()
    artigos_filtrados = filtrar_por_janela(
        _artigos, pesquisadores, ano_base=2026
    )
    artigos_finais = artigos_filtrados.group_by('researcher_id').agg(
        pl.col('qtd').sum().alias('total_artigos_validos')
    )
    pesquisadores = merge_data(pesquisadores, artigos_finais)

    _livros = get_livros_e_capitulos()
    livros_filtrados = filtrar_por_janela(_livros, pesquisadores, ano_base=2026)
    livros_finais = livros_filtrados.group_by('researcher_id').agg(
        pl.col('qtd').sum().alias('total_livros_validos')
    )
    pesquisadores = merge_data(pesquisadores, livros_finais)

    _software = get_software()
    software_filtrados = filtrar_por_janela(
        _software, pesquisadores, ano_base=2026
    )
    softwars_filnais = software_filtrados.group_by('researcher_id').agg(
        pl.col('qtd').sum().alias('total_software_validos')
    )
    pesquisadores = merge_data(pesquisadores, softwars_filnais)

    _patentes = get_patentes()
    patentes_filtradas = filtrar_por_janela(
        _patentes, pesquisadores, ano_base=2026
    )
    patentes_finais = patentes_filtradas.group_by('researcher_id').agg(
        pl.col('qtd').sum().alias('total_patentes_validas')
    )
    pesquisadores = merge_data(pesquisadores, patentes_finais)

    _desenhos_industriais_ou_marcas = get_desenhos_industriais_ou_marcas()
    desenhos_industriais_ou_marcas_filtradas = filtrar_por_janela(
        _desenhos_industriais_ou_marcas, pesquisadores, ano_base=2026
    )
    desenhos_industriais_ou_marcas_finais = (
        desenhos_industriais_ou_marcas_filtradas.group_by('researcher_id').agg(
            pl
            .col('qtd')
            .sum()
            .alias('total_desenhos_industriais_ou_marcas_validas')
        )
    )
    pesquisadores = merge_data(
        pesquisadores, desenhos_industriais_ou_marcas_finais
    )

    print(pesquisadores)
