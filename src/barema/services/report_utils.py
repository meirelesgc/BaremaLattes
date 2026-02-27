import polars as pl


def filter_by_window(df_production, df_researchers, base_year=2026):
    df_joined = df_production.join(
        df_researchers.select(["researcher_id", "window_years"]),
        on="researcher_id",
        how="inner",
    )

    df_filtered = df_joined.filter(
        (base_year - pl.col("year")) <= pl.col("window_years")
    )

    return df_filtered.drop("window_years")


def merge_data(main_df, extra_df):
    return main_df.join(extra_df, on="researcher_id", how="left")


def process_and_merge_production(
    df_researchers, get_data_func, total_col_name, base_year=2026
):
    df_data = get_data_func()
    df_filtered = filter_by_window(df_data, df_researchers, base_year=base_year)

    df_grouped = df_filtered.group_by("researcher_id").agg(
        pl.col("qtd").sum().alias(total_col_name)
    )

    return merge_data(df_researchers, df_grouped)


def add_phd_level(df_time):
    CLASS_C = 2
    CLASS_A_B = 6

    df_with_level = df_time.with_columns(
        pl.when(pl.col("tempo_doutorado") <= CLASS_C)
        .then(pl.lit("C"))
        .when(pl.col("tempo_doutorado") >= CLASS_A_B)
        .then(pl.lit("A"))
        .otherwise(pl.lit("B"))
        .alias("level")
    )

    return df_with_level


def add_evaluation_window(df_researchers):
    levels_10_years = ["SR", "A", "1A", "1B", "B", "1C", "1D"]

    return df_researchers.with_columns(
        pl.when(pl.col("nivel_bolsa").is_in(levels_10_years))
        .then(10)
        .otherwise(5)
        .alias("window_years")
    )
