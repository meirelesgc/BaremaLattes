import polars as pl
from sqlalchemy import text

from barema.db.connection import get_session


def get_researchers():
    session = get_session()
    query = """
    SELECT id::text AS researcher_id, name AS nome, lattes_id,
        openalex_researcher.h_index
    FROM researcher
    LEFT JOIN openalex_researcher ON
        openalex_researcher.researcher_id = researcher.id
    WHERE lattes_id IN ('6716225567627323','6608165728129698','7401907691814937','1966167015825708');
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {
        "researcher_id": pl.Utf8,
        "nome": pl.Utf8,
        "lattes_id": pl.Utf8,
        "h_index": pl.Utf8,
    }
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
    schema = {"researcher_id": pl.Utf8, "tempo_doutorado": pl.Int32}
    return pl.DataFrame(data, schema=schema)


def get_foment_level():
    session = get_session()
    query = """
    SELECT researcher_id::text, foment.category_level_code AS nivel_bolsa
    FROM foment;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "nivel_bolsa": pl.Utf8}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_cultivar():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM registered_cultivar
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_short_course_taught():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM short_course_taught
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_radio_or_tv_program():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM radio_or_tv_program
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_social_media_website_blog():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM social_media_website_blog
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_other_technical_production():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM other_technical_production
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_software():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM software
    WHERE code IS NOT NULL
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_no_reg_software():
    session = get_session()
    query = """
    SELECT researcher_id::text, year::int, COUNT(*) as qtd
    FROM software
    WHERE code IS NULL
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_research_report():
    session = get_session()
    query = """
    SELECT researcher_id::text, year, COUNT(*) as qtd
    FROM research_report
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
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
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_scientific_projects():
    session = get_session()
    query = """
    SELECT rp.researcher_id::text, rp.start_year::INT AS year, COUNT(DISTINCT rp.id) as qtd
    FROM research_project rp
    WHERE EXISTS (
        SELECT 1 
        FROM research_project_components rpc 
        JOIN researcher r ON r.lattes_id = rpc.lattes_id
        WHERE rpc.project_id = rp.id 
        AND r.id = rp.researcher_id 
        AND rpc.coordinator IS TRUE
    )
    AND rp.nature = 'PESQUISA'
    AND EXISTS (
        SELECT 1 FROM research_project_foment rpf 
        WHERE rpf.project_id = rp.id AND rpf.nature = 'AUXILIO_FINANCEIRO'
    )
    GROUP BY rp.researcher_id, rp.start_year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_projects_with_companies():
    session = get_session()
    query = """
    SELECT rp.researcher_id::text, rp.start_year::INT AS year, COUNT(DISTINCT rp.id) as qtd
    FROM research_project rp
    WHERE EXISTS (
        SELECT 1 
        FROM research_project_components rpc 
        JOIN researcher r ON r.lattes_id = rpc.lattes_id
        WHERE rpc.project_id = rp.id 
        AND r.id = rp.researcher_id 
        AND rpc.coordinator IS TRUE
    )
    AND (rp.nature != 'PESQUISA' OR rp.nature IS NULL)
    AND EXISTS (
        SELECT 1 FROM research_project_foment rpf 
        WHERE rpf.project_id = rp.id AND rpf.nature = 'AUXILIO_FINANCEIRO'
    )
    GROUP BY rp.researcher_id, rp.start_year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_research_projects():
    session = get_session()
    query = """
    SELECT rp.researcher_id::text, rp.start_year::INT AS year, COUNT(DISTINCT rp.id) as qtd
    FROM research_project rp
    WHERE EXISTS (
        SELECT 1 
        FROM research_project_components rpc 
        JOIN researcher r ON r.lattes_id = rpc.lattes_id
        WHERE rpc.project_id = rp.id 
        AND r.id = rp.researcher_id 
        AND rpc.coordinator IS TRUE
    )
    AND rp.nature = 'PESQUISA'
    AND NOT EXISTS (
        SELECT 1 FROM research_project_foment rpf 
        WHERE rpf.project_id = rp.id AND rpf.nature = 'AUXILIO_FINANCEIRO'
    )
    GROUP BY rp.researcher_id, rp.start_year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)
