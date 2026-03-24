import polars as pl
from sqlalchemy import text

from barema.db.connection import get_session


def get_researchers():
    session = get_session()
    query = """
    SELECT id::text AS researcher_id, name AS nome, lattes_id,
        openalex_researcher.h_index, last_update::varchar AS last_update
    FROM researcher
    LEFT JOIN openalex_researcher ON
        openalex_researcher.researcher_id = researcher.id;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {
        "researcher_id": pl.Utf8,
        "nome": pl.Utf8,
        "lattes_id": pl.Utf8,
        "h_index": pl.Utf8,
        "last_update": pl.Utf8,
    }
    return pl.DataFrame(data, schema=schema)


def get_phd_time():
    session = get_session()
    query = """
    SELECT DISTINCT ON (researcher_id)
        researcher_id::text,
        (EXTRACT(YEAR FROM CURRENT_DATE) - education_end)::INT AS tempo_doutorado
    FROM education
    WHERE degree = 'DOCTORATE'
        AND education_end IS NOT NULL
    ORDER BY researcher_id, education_end ASC;
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


def get_cultivar_patents():
    session = get_session()
    query = """
    SELECT researcher_id, year, SUM(qtd) as qtd
    FROM (
        SELECT researcher_id::text, year::int AS year, COUNT(*) as qtd
        FROM registered_cultivar
        GROUP BY researcher_id, year

        UNION ALL

        SELECT researcher_id::text, development_year::int AS year, COUNT(*) as qtd
        FROM patent
        GROUP BY researcher_id, development_year
    ) t
    GROUP BY researcher_id, year;
    """
    result = session.execute(text(query))
    data = result.mappings().all()
    schema = {"researcher_id": pl.Utf8, "year": pl.Int32, "qtd": pl.Int64}
    return pl.DataFrame(data, schema=schema)


def get_books():
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


def get_other_technical_production():
    session = get_session()
    query = """
    WITH combined_data AS (
        SELECT researcher_id::text, year::int
        FROM industrial_design
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM brand
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM technological_product
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM process_or_technique
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM technical_work
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM public.technical_work_presentation
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM letter_map_or_similar
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM short_course_taught
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM didactic_material
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM publishing
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM mockup
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM event_organization
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM public.radio_or_tv_program
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM research_report
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM public.social_media_website_blog
        UNION ALL
        SELECT researcher_id::text, year::int
        FROM other_technical_production
    )
    SELECT researcher_id, year, COUNT(*) as qtd
    FROM combined_data
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


def fat_articles():
    session = get_session()
    query = """
    SELECT title, year::INT, qualis, periodical_magazine_id::TEXT,
        researcher_id::TEXT, nature
    FROM bibliographic_production
    INNER JOIN bibliographic_production_article 
        ON bibliographic_production_article.bibliographic_production_id = bibliographic_production.id
    """
    result = session.execute(text(query))
    data = result.mappings().all()

    schema = {
        "title": pl.Utf8,
        "year": pl.Int32,
        "qualis": pl.Utf8,
        "periodical_magazine_id": pl.Utf8,
        "researcher_id": pl.Utf8,
        "nature": pl.Utf8,
    }

    return pl.DataFrame(data, schema=schema)


def fat_books():
    session = get_session()
    query = """
    SELECT title, year::INT, nature, isbn, researcher_id::TEXT
    FROM bibliographic_production
        INNER JOIN bibliographic_production_book 
            ON bibliographic_production_book.bibliographic_production_id = bibliographic_production.id

    UNION ALL

    SELECT title, year::INT, nature, isbn, researcher_id::TEXT
    FROM bibliographic_production
        INNER JOIN bibliographic_production_book_chapter
            ON bibliographic_production_book_chapter.bibliographic_production_id = bibliographic_production.id
    """
    result = session.execute(text(query))
    data = result.mappings().all()

    schema = {
        "title": pl.Utf8,
        "year": pl.Int32,
        "nature": pl.Utf8,
        "isbn": pl.Utf8,
        "researcher_id": pl.Utf8,
    }

    return pl.DataFrame(data, schema=schema)


def fat_software():
    session = get_session()
    query = """
    SELECT title, goal, financing_institutionc, researcher_id::TEXT, year::INT,
        code
    FROM public.software;
    """
    result = session.execute(text(query))
    data = result.mappings().all()

    schema = {
        "title": pl.Utf8,
        "goal": pl.Utf8,
        "financing_institutionc": pl.Utf8,
        "researcher_id": pl.Utf8,
        "year": pl.Int32,
        "code": pl.Utf8,
    }

    return pl.DataFrame(data, schema=schema)


def fat_patent():
    session = get_session()
    query = """
    SELECT title, category, development_year::INT, details, researcher_id::TEXT, code,
        grant_date::DATE, deposit_date::DATE
    FROM public.patent;
    """
    result = session.execute(text(query))
    data = result.mappings().all()

    schema = {
        "title": pl.Utf8,
        "category": pl.Utf8,
        "development_year": pl.Int32,
        "details": pl.Utf8,
        "researcher_id": pl.Utf8,
        "code": pl.Utf8,
        "grant_date": pl.Date,
        "deposit_date": pl.Date,
    }

    return pl.DataFrame(data, schema=schema)


def fat_cultivar():
    session = get_session()
    query = """
    SELECT denomination, year::INT, country, code,
        researcher_id::TEXT
    FROM public.registered_cultivar;
    """
    result = session.execute(text(query))
    data = result.mappings().all()

    schema = {
        "denomination": pl.Utf8,
        "year": pl.Int32,
        "country": pl.Utf8,
        "code": pl.Utf8,
        "researcher_id": pl.Utf8,
    }

    return pl.DataFrame(data, schema=schema)
