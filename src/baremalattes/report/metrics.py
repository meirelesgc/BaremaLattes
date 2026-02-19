import polars as pl
from sqlalchemy import text

from baremalattes.database.connection import get_session


def get_researchers():
    session = get_session()
    query = """
    SELECT id::text AS researcher_id, name AS nome, lattes_id
    FROM researcher;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'nome': pl.Utf8, 'lattes_id': pl.Utf8}
    return pl.DataFrame(data, schema=schema)


def get_phd_time():
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
    schema = {'researcher_id': pl.Utf8, 'tempo_doutorado': pl.Int32}
    return pl.DataFrame(data, schema=schema)


def get_foment_level():
    session = get_session()
    query = """
    SELECT researcher_id::text, foment.category_level_code AS nivel_bolsa
    FROM foment;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'nivel_bolsa': pl.Utf8}
    return pl.DataFrame(data, schema=schema)


def get_articles():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM bibliographic_production
    WHERE type = 'ARTICLE' AND year IS NOT NULL
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_books_chapters():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM bibliographic_production
    WHERE type IN ('BOOK', 'BOOK_CHAPTER') AND year IS NOT NULL
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_software():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM software
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_patents():
    session = get_session()
    query = """
    SELECT researcher_id::text, development_year::int AS year, COUNT(*) as qtd
    FROM patent
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_assets_ip():
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
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_guidance_postdoc():
    session = get_session()
    query = """
    SELECT researcher_id::text, year, COUNT(*) as qtd
    FROM guidance
    WHERE nature = 'Supervisão De Pós-Doutorado'
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_phd_completed():
    session = get_session()
    query = """
    SELECT researcher_id::text, year, COUNT(*) as qtd
    FROM guidance
    WHERE nature = 'Tese De Doutorado'
        AND guidance.status = 'Concluída'
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_phd_ongoing():
    session = get_session()
    query = """
    SELECT researcher_id::text, year, COUNT(*) as qtd
    FROM guidance
    WHERE nature = 'Tese De Doutorado'
        AND guidance.status = 'Em andamento'
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_msc_completed():
    session = get_session()
    query = """
    SELECT researcher_id::text, year, COUNT(*) as qtd
    FROM guidance
    WHERE nature = 'Dissertação De Mestrado'
        AND guidance.status =  'Concluída'
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_msc_ongoing():
    session = get_session()
    query = """
    SELECT researcher_id::text, year, COUNT(*) as qtd
    FROM guidance
    WHERE nature = 'Dissertação De Mestrado'
        AND guidance.status = 'Em andamento'
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {'researcher_id': pl.Utf8, 'year': pl.Int32, 'qtd': pl.Int64}
    return pl.DataFrame(data, schema=schema)
