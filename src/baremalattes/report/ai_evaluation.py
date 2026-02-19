import os

import polars as pl


def evaluation(lattes_id: str):
    file_path = f'data/raw/projects/{lattes_id}.pdf'

    expected_keys = [
        'publico_produto',
        'objetivos_metas_relevancia',
        'metodologia_gestao',
        'colaboracoes_financiamento',
        'potencial_inovacao_empreendedorismo',
        'demandas_escalabilidade',
        'maturidade_resultados',
        'organizacao_parcerias_extensao',
        'perfil_tecnologico',
    ]

    if not os.path.exists(file_path):
        return {key: 'Relatório não encontrado' for key in expected_keys}

    return {key: 'Dados extraídos do PDF' for key in expected_keys}


def run_aieval_process(researchers: pl.DataFrame):
    df_resultado = researchers.with_columns(
        pl
        .col('lattes_id')
        .map_elements(
            evaluation,
            return_dtype=pl.Struct([
                pl.Field('publico_produto', pl.String),
                pl.Field('objetivos_metas_relevancia', pl.String),
                pl.Field('metodologia_gestao', pl.String),
                pl.Field('colaboracoes_financiamento', pl.String),
                pl.Field('potencial_inovacao_empreendedorismo', pl.String),
                pl.Field('demandas_escalabilidade', pl.String),
                pl.Field('maturidade_resultados', pl.String),
                pl.Field('organizacao_parcerias_extensao', pl.String),
                pl.Field('perfil_tecnologico', pl.String),
            ]),
        )
        .alias('info_lattes')
    ).unnest('info_lattes')

    return df_resultado


if False:
    from langchain_community.document_loaders import PyMuPDFLoader
    from langchain_community.vectorstores import PGVector
    from langchain_core.messages import HumanMessage
    from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    from baremalattes.report.prompts import PROMPTS_AVALIACAO
    from baremalattes.settings import Settings

    SETTINGS = Settings()

    file_path = 'data/raw/projects/Projeto Eduardo.pdf'

    loader = PyMuPDFLoader(file_path)
    documents = loader.load()

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, chunk_overlap=200
    )
    splits = text_splitter.split_documents(documents)

    embeddings = OpenAIEmbeddings(api_key=SETTINGS.OPENAI_API_KEY)

    vectorstore = PGVector.from_documents(
        embedding=embeddings,
        documents=splits,
        connection_string=SETTINGS.DATABASE_URL,
    )

    retriever = vectorstore.as_retriever(
        search_kwargs={'k': 3, 'filter': {'source': file_path}}
    )

    llm = ChatOpenAI(
        model='gpt-5', temperature=0, api_key=SETTINGS.OPENAI_API_KEY
    )

    sessoes_relevantes = {}

    for chave, pergunta in PROMPTS_AVALIACAO.items():
        docs_relevantes = retriever.invoke(pergunta)

        paginas_identificadas = set()
        for doc in docs_relevantes:
            if 'page' in doc.metadata:
                paginas_identificadas.add(doc.metadata['page'])

        textos_paginas_completas = []
        for doc_original in documents:
            if doc_original.metadata.get('page') in paginas_identificadas:
                textos_paginas_completas.append(doc_original.page_content)

        contexto_recuperado = '\n\n---\n\n'.join(textos_paginas_completas)

        sessoes_relevantes[chave] = contexto_recuperado

        prompt_final = f"""
    Documento do projeto (Páginas Relevantes Integrais):

    {contexto_recuperado}

    Pergunta:

    {pergunta}
    """

        response = llm.invoke([HumanMessage(content=prompt_final)])

        print(f'\n{"=" * 60}')
        print(f'🔎 AVALIAÇÃO: {chave.upper()}')
        print(f'{"=" * 60}')
        print(
            f'📎 RAG: {len(docs_relevantes)} trecho(s) recuperado(s) apontando para {len(paginas_identificadas)} página(s) completa(s).'
        )
        print('-' * 60)

        print('Trechos identificados na busca vetorial:\n')
        for i, doc in enumerate(docs_relevantes, 1):
            texto_parcial = doc.page_content[:300].replace('\n', ' ')
            pagina_origem = doc.metadata.get('page', 'Desconhecida')
            print(f'[{i}] (Página {pagina_origem}): "{texto_parcial}..."\n')

        print('-' * 60)
        print('📝 RESULTADO DA ANÁLISE:\n')
        print(response.content)
        print(f'{"=" * 60}\n')
