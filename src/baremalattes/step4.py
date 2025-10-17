import pandas as pd

from baremalattes.core.database import Connection
from baremalattes.core.settings import Settings

settings = Settings()

conn = Connection(settings.DATABASE_URL)


def get_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM researcher;
        """
    researchers = conn.select(SCRIPT_SQL)
    return researchers


def calculate_master_degree_score(researchers: pd.DataFrame):
    """
    Calculates the score for a Master's degree.
    Rule: 1.5 points, with a maximum of 1.5.
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM education
        WHERE degree IN ('MASTERS-DEGREE', 'PROFESSIONAL-MASTERS-DEGREE')
            AND education.education_end IS NOT NULL
            AND (education.status = 'CONCLUIDO' OR education.status IS NULL)
        GROUP BY researcher_id;
        """
    education_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    education_df = pd.DataFrame(education_data, columns=columns)

    if education_df.empty:
        researchers["master_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        education_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["master_score"] = (researchers["count"] * 1.5).clip(upper=1.5)

    return researchers.drop(columns=["count"])


def calculate_specialization_score(researchers: pd.DataFrame):
    """
    Calculates the score for specialization courses.
    Rule: 0.25 each, maximum of 2 courses (0.5 points).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM education
        WHERE degree = 'SPECIALIZATION'
            AND education.education_end IS NOT NULL
            AND (education.status = 'CONCLUIDO' OR education.status IS NULL)
        GROUP BY researcher_id
        ORDER BY count DESC;
    """
    specialization_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    specialization_df = pd.DataFrame(specialization_data, columns=columns)

    if specialization_df.empty:
        researchers["specialization_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        specialization_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["specialization_score"] = (researchers["count"] * 0.25).clip(upper=0.5)
    return researchers.drop(columns=["count"])


def calculate_research_project_participation_score(researchers: pd.DataFrame):
    """
    Calculates the score for participation in research projects.
    Rule: 0.25 each, maximum of 2 projects (0.5 points).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM research_project
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    project_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    project_df = pd.DataFrame(project_data, columns=columns)

    if project_df.empty:
        researchers["project_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        project_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["project_score"] = (researchers["count"] * 0.25).clip(upper=0.5)

    return researchers.drop(columns=["count"])


def calculate_indexed_journal_articles_score(researchers: pd.DataFrame):
    """
    Calculates the score for articles in indexed journals.
    Rule: 0.50 each, maximum of 3 articles (1.5 points).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM bibliographic_production
        INNER JOIN bibliographic_production_article
            ON bibliographic_production_article.bibliographic_production_id = bibliographic_production.id
        WHERE bibliographic_production.year::INT >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            AND bibliographic_production_article.qualis IS NOT NULL
            AND bibliographic_production_article.qualis <> 'SQ'
            AND type = 'ARTICLE'
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    indexed_articles_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    indexed_articles_df = pd.DataFrame(indexed_articles_data, columns=columns)

    if indexed_articles_df.empty:
        researchers["indexed_articles_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        indexed_articles_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["indexed_articles_score"] = (researchers["count"] * 0.5).clip(upper=1.5)

    return researchers.drop(columns=["count"])


def calculate_non_indexed_journal_articles_score(researchers: pd.DataFrame):
    """
    Calculates the score for articles in non-indexed journals.
    Rule: 0.25 points each, maximum of 2 articles (0.5 points).
    Indexed articles above 3 are counted as non-indexed.
    """
    columns = ["researcher_id", "count"]

    SCRIPT_SQL_INDEXED = """
        SELECT researcher_id, COUNT(*) AS count
        FROM bibliographic_production
        INNER JOIN bibliographic_production_article
            ON bibliographic_production_article.bibliographic_production_id = bibliographic_production.id
        WHERE bibliographic_production.year::INT >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            AND bibliographic_production_article.qualis IS NOT NULL
            AND bibliographic_production_article.qualis <> 'SQ'
            AND type = 'ARTICLE'
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    indexed_articles_data = conn.select(SCRIPT_SQL_INDEXED)
    indexed_articles_df = pd.DataFrame(indexed_articles_data, columns=columns)

    indexed_articles_df["over_count"] = (indexed_articles_df["count"] - 3).clip(lower=0)
    indexed_overage_df = indexed_articles_df[["researcher_id", "over_count"]]

    SCRIPT_SQL_NON_INDEXED = """
        SELECT researcher_id, COUNT(*) AS count
        FROM bibliographic_production
        INNER JOIN bibliographic_production_article
            ON bibliographic_production_article.bibliographic_production_id = bibliographic_production.id
        WHERE bibliographic_production.year::INT >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            AND bibliographic_production_article.qualis = 'SQ'
            AND type = 'ARTICLE'
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    non_indexed_articles_data = conn.select(SCRIPT_SQL_NON_INDEXED)
    non_indexed_articles_df = pd.DataFrame(non_indexed_articles_data, columns=columns)

    if not non_indexed_articles_df.empty:
        combined_df = pd.merge(
            non_indexed_articles_df,
            indexed_overage_df,
            on="researcher_id",
            how="outer",
        )
    else:
        combined_df = indexed_overage_df.rename(columns={"over_count": "count"})
        combined_df["over_count"] = 0

    if combined_df.empty:
        researchers["non_indexed_articles_score"] = 0.0
        return researchers

    combined_df["count"] = combined_df["count"].fillna(0)
    combined_df["over_count"] = combined_df["over_count"].fillna(0)
    combined_df["count"] += combined_df["over_count"]

    researchers = pd.merge(
        researchers,
        combined_df[["researcher_id", "count"]],
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["non_indexed_articles_score"] = (researchers["count"] * 0.25).clip(
        upper=0.5
    )

    return researchers.drop(columns=["count"])


def calculate_published_books_score(researchers: pd.DataFrame):
    """
    Calculates the score for published books.
    Rule: 0.50 each, maximum of 3 books (1.5 points).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM bibliographic_production
        WHERE bibliographic_production.year::INT >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            AND type = 'BOOK'
        GROUP BY researcher_id;
        """
    book_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    book_df = pd.DataFrame(book_data, columns=columns)

    if book_df.empty:
        researchers["book_score"] = 0.0
        return researchers

    researchers = pd.merge(researchers, book_df, on="researcher_id", how="left")

    researchers["count"] = researchers["count"].fillna(0)
    researchers["book_score"] = (researchers["count"] * 0.5).clip(upper=1.5)

    return researchers.drop(columns=["count"])


def calculate_published_book_chapters_score(researchers: pd.DataFrame):
    """
    Calculates the score for published book chapters.
    Rule: 0.25 points per chapter, maximum of 4 chapters (1.0 point).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM bibliographic_production
        WHERE bibliographic_production.year::INT >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            AND type = 'BOOK_CHAPTER'
        GROUP BY researcher_id;
        """
    book_chapters_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    book_chapters_df = pd.DataFrame(book_chapters_data, columns=columns)

    if book_chapters_df.empty:
        researchers["published_book_chapters_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        book_chapters_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["book_chapters_score"] = (researchers["count"] * 0.25).clip(upper=1.0)

    return researchers.drop(columns=["count"])


def calculate_technical_production_score(researchers: pd.DataFrame):
    """
    Calculates the score for technical productions.
    Rule: 0.25 each, maximum of 4 productions (1.0 point).
    Includes data from multiple technical production tables.
    """
    TABLES = [
        ("advisory_activity", "start_year"),
        ("process_or_technique", "year"),
        ("mockup", "year"),
        ("publishing", "year"),
        ("letter_map_or_similar", "year"),
        ("short_course", "year"),
        ("research_report", "year"),
        ("technical_work_program", "year"),
        ("social_media_website_blog", "year"),
        ("other_technical_production", "year"),
        ("technological_product", "year"),
        ("patent", "development_year"),
        ("technical_work", "year"),
        # ("technical_work_presentation", "year"),
        ("brand", "year"),
    ]

    all_data = []

    for table, year_field in TABLES:
        SCRIPT_SQL = f"""
            SELECT researcher_id, COUNT(*) AS count
            FROM {table}
            WHERE {table}.{year_field}::INT >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
            GROUP BY researcher_id;
        """
        data = conn.select(SCRIPT_SQL)
        all_data.extend(data)

    if not all_data:
        researchers["technical_production_score"] = 0.0
        return researchers

    technical_df = pd.DataFrame(all_data, columns=["researcher_id", "count"])

    technical_df = technical_df.groupby("researcher_id", as_index=False)["count"].sum()

    researchers = pd.merge(
        researchers,
        technical_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["technical_production_score"] = (researchers["count"] * 0.25).clip(
        upper=1.0
    )

    return researchers.drop(columns=["count"])


def calculate_artistic_production_score(researchers: pd.DataFrame):
    """
    Calculates the score for artistic productions.
    Rule: 0.25 each, maximum of 4 productions (1.0 point).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM artistic_production
        WHERE artistic_production.year >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    artistic_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    artistic_df = pd.DataFrame(artistic_data, columns=columns)

    if artistic_df.empty:
        researchers["artistic_production_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        artistic_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["artistic_production_score"] = (researchers["count"] * 0.25).clip(
        upper=1.0
    )

    return researchers.drop(columns=["count"])


def calculate_event_organization_score(researchers: pd.DataFrame):
    """
    Calculates the score for organizing events.
    Rule: 0.10 each, maximum of 5 events (0.5 points).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM event_organization
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    event_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    event_df = pd.DataFrame(event_data, columns=columns)

    if event_df.empty:
        researchers["event_organization_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        event_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["event_organization_score"] = (researchers["count"] * 0.10).clip(
        upper=0.5
    )

    return researchers.drop(columns=["count"])


def calculate_guidance_score(researchers: pd.DataFrame):
    """
    Calculates the score for guidance.
    Rule: 0.10 each, maximum of 5 orientations (0.5 points).
    """
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS count
        FROM guidance
        GROUP BY researcher_id
        ORDER BY COUNT(*) DESC;
        """
    guidance_data = conn.select(SCRIPT_SQL)
    columns = ["researcher_id", "count"]
    guidance_df = pd.DataFrame(guidance_data, columns=columns)

    if guidance_df.empty:
        researchers["guidance_score"] = 0.0
        return researchers

    researchers = pd.merge(
        researchers,
        guidance_df,
        on="researcher_id",
        how="left",
    )

    researchers["count"] = researchers["count"].fillna(0)
    researchers["guidance_score"] = (researchers["count"] * 0.10).clip(upper=0.5)

    return researchers.drop(columns=["count"])


def extract_lattes_id(lattes_url: str) -> str | None:
    if not isinstance(lattes_url, str):
        return None

    try:
        lattes_id = lattes_url.strip().split("/")[-1]
        return lattes_id if lattes_id.isnumeric() else None
    except (IndexError, AttributeError):
        return None


def registration_number(researchers: pd.DataFrame):
    csv = pd.read_csv("storage/csv/dmdc.csv")
    csv["lattes_id"] = csv["LATTES"].apply(extract_lattes_id)
    csv["n_inscrição"] = csv["INSCRIÇÃO"].astype(str)
    csv = csv[["n_inscrição", "lattes_id", "INSCRIÇÃO"]]

    researchers = pd.merge(
        csv,
        researchers,
        on="lattes_id",
        how="left",
    )
    return researchers.drop(columns=["name", "INSCRIÇÃO"])


def main():
    researchers = get_researchers()

    columns = ["researcher_id", "name", "lattes_id"]
    researchers = pd.DataFrame(researchers, columns=columns)

    researchers = calculate_master_degree_score(researchers)

    researchers = calculate_specialization_score(researchers)

    researchers = calculate_research_project_participation_score(researchers)

    # Ultimos 5 anos
    researchers = calculate_indexed_journal_articles_score(researchers)

    # Ultimos 5 anos
    researchers = calculate_non_indexed_journal_articles_score(researchers)

    # Ultimos 5 anos
    researchers = calculate_published_books_score(researchers)

    # Ultimos 5 anos
    researchers = calculate_published_book_chapters_score(researchers)

    # Ultimos 5 anos
    researchers = calculate_technical_production_score(researchers)

    # Ultimos 5 anos
    researchers = calculate_artistic_production_score(researchers)

    researchers = calculate_event_organization_score(researchers)

    researchers = calculate_guidance_score(researchers)

    print("Concluido")

    researchers = registration_number(researchers)

    researchers.to_excel("score.xlsx")


if __name__ == "__main__":
    main()
