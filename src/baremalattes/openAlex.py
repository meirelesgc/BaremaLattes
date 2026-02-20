import json
import time

import httpx
import polars as pl
from sqlalchemy import text

from baremalattes.database.connection import get_session


def scrapping_researcher_data():
    session = get_session()

    openalex_url = 'https://api.openalex.org/authors/orcid:'

    query = text("""
        SELECT r.orcid, r.id
        FROM researcher r
        LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
        WHERE r.orcid IS NOT NULL AND opr.researcher_id IS NULL;
    """)

    result = session.execute(query)
    dataframe = pl.DataFrame(result.mappings().all())

    with httpx.Client() as client:
        for data in dataframe.iter_rows(named=True):
            researcher_url = openalex_url + data['orcid']
            researcher_file = f'data/raw/openAlex/researcher/{data["id"]}.json'

            response = client.get(researcher_url)

            if response.status_code == 200:
                with open(researcher_file, 'w', encoding='utf-8') as buffer:
                    json_data = response.json()
                    json.dump(json_data, buffer)

                    try:
                        extract_researcher(session, data['id'], json_data)
                    except Exception as e:
                        print(f'[ERROR] - {e}')

                print(f'[201] - CREATED RESEARCHER [{data["id"]}]')
            else:
                with open(researcher_file, 'w', encoding='utf-8') as buffer:
                    buffer.write(str(response.status_code))
                print(f'[404] - NOT FOUND RESEARCHER [{data["id"]}]')

            time.sleep(6)


def extract_researcher(session, researcher_id, data):
    h_index = None
    if summary_stats := data.get('summary_stats', None):
        h_index = summary_stats.get('h_index')

    i10_index = None
    if summary_stats := data.get('summary_stats', None):
        i10_index = summary_stats.get('i10_index')

    orcid = None
    if ids := data.get('ids', ''):
        if orcid_url := ids.get('orcid', ''):
            orcid = orcid_url[-18:]

    scopus = None
    if ids := data.get('ids', None):
        scopus = ids.get('scopus')

    open_alex = data.get('id')
    works_count = data.get('works_count')
    cited_by_count = data.get('cited_by_count')

    query = text("""
        INSERT INTO public.openalex_researcher
        (researcher_id, h_index, relevance_score, works_count, cited_by_count, i10_index, scopus, orcid, openalex)
        VALUES (:researcher_id, :h_index, :relevance_score, :works_count, :cited_by_count, :i10_index, :scopus, :orcid, :openalex);
    """)

    params = {
        'researcher_id': researcher_id,
        'h_index': h_index,
        'relevance_score': 0,
        'works_count': works_count,
        'cited_by_count': cited_by_count,
        'i10_index': i10_index,
        'scopus': scopus,
        'orcid': orcid,
        'openalex': open_alex,
    }

    session.execute(query, params)
    session.commit()

    print(f'Insert concluido! [{researcher_id}]')


if __name__ == '__main__':
    scrapping_researcher_data()
