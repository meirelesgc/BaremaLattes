from datetime import datetime

from barema.services.ai_evaluation import run_ai_evaluation
from barema.services.ai_extraction import run_ai_extraction
from barema.services.download_lattes import run_download_process
from barema.services.pre_process_projects import project_metadata
from barema.services.queries import (
    get_articles,
    get_assets_ip,
    get_books_chapters,
    get_cultivar,
    get_foment_level,
    get_guidance_postdoc,
    get_msc_completed,
    get_msc_ongoing,
    get_no_reg_software,
    get_other_technical_production,
    get_patents,
    get_phd_completed,
    get_phd_ongoing,
    get_phd_time,
    get_projects_with_companies,
    get_radio_or_tv_program,
    get_research_projects,
    get_research_report,
    get_researchers,
    get_scientific_projects,
    get_short_course_taught,
    get_social_media_website_blog,
    get_software,
)
from barema.services.report_utils import (
    add_evaluation_window,
    add_phd_level,
    merge_data,
    process_and_merge_production,
)

current_year = datetime.now().year


def generate_report(base_year=current_year):
    folder_path = r"data/raw/projects"
    df = project_metadata(folder_path)
    print(df)
    if False:
        run_download_process()
        # os.system("docker compose run --rm barema_hop")
        researchers = get_researchers()
        phd_time = get_phd_time()
        researchers = merge_data(researchers, phd_time)
        phd_level = add_phd_level(phd_time)
        researchers = merge_data(researchers, phd_level)

        foment_level = get_foment_level()
        researchers = merge_data(researchers, foment_level)

        researchers = add_evaluation_window(researchers)

        productions_to_process = [
            (get_articles, "total_valid_articles"),
            (get_books_chapters, "total_valid_books_chapters"),
            (get_software, "total_valid_software"),
            (get_no_reg_software, "total_valid_no_reg_software"),
            (get_patents, "total_valid_patents"),
            (get_assets_ip, "total_valid_assets_ip"),
            (get_research_report, "total_research_report"),
            (get_guidance_postdoc, "total_valid_guidance_postdoc"),
            (get_phd_completed, "total_valid_phd_completed"),
            (get_phd_ongoing, "total_valid_phd_ongoing"),
            (get_msc_completed, "total_valid_msc_completed"),
            (get_msc_ongoing, "total_valid_msc_ongoing"),
            (get_cultivar, "total_valid_get_cultivar"),
            (get_short_course_taught, "total_valid_short_course_taught"),
            (get_radio_or_tv_program, "total_valid_radio_or_tv_program"),
            (get_social_media_website_blog, "total_valid_social_media_website_blog"),
            (get_other_technical_production, "total_valid_other_technical_production"),
            (get_scientific_projects, "total_valid_scientific_projects"),
            (get_projects_with_companies, "total_valid_projects_with_companies"),
            (get_research_projects, "total_valid_research_projects"),
        ]

        for get_func, col_name in productions_to_process:
            researchers = process_and_merge_production(
                researchers, get_func, col_name, base_year
            )

        researchers = run_ai_extraction(researchers)
        researchers = run_ai_evaluation(researchers)
        researchers.write_excel("relatorio.xlsx")
