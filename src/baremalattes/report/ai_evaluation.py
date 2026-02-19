import os

import polars as pl
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.messages import HumanMessage
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from baremalattes.report.prompts import PROMPTS_AVALIACAO
from baremalattes.settings import Settings

SETTINGS = Settings()


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
        resultados_erro = {}
        for key in expected_keys:
            resultados_erro[key] = 'Relatório não encontrado'
            resultados_erro[f'{key}_rag'] = 'Relatório não encontrado'
        return resultados_erro

    loader = PyMuPDFLoader(file_path, mode='single')
    documents = loader.load()

    documento_completo = '\n\n'.join([doc.page_content for doc in documents])

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, chunk_overlap=200
    )
    splits = text_splitter.split_documents(documents)

    embeddings = OpenAIEmbeddings(api_key=SETTINGS.OPENAI_API_KEY)

    vectorstore = InMemoryVectorStore.from_documents(splits, embeddings)
    retriever = vectorstore.as_retriever(search_kwargs={'k': 3})

    llm = ChatOpenAI(
        api_key=SETTINGS.OPENAI_API_KEY, model='gpt-4o-mini', temperature=0
    )

    resultados = {}

    for key in expected_keys:
        pergunta = PROMPTS_AVALIACAO.get(key, f'Descreva: {key}')

        docs_recuperados = retriever.invoke(pergunta)
        contexto = '\n\n'.join([doc.page_content for doc in docs_recuperados])

        mensagem = HumanMessage(
            content=f'Responda à pergunta utilizando o documento fornecido.\n\nDocumento: {documento_completo}\n\nPergunta: {pergunta}'
        )

        resposta = llm.invoke([mensagem])

        resultados[key] = resposta.content
        resultados[f'{key}_rag'] = contexto

    return resultados


def run_aieval_process(researchers: pl.DataFrame):
    df_resultado = researchers.with_columns(
        pl
        .col('lattes_id')
        .map_elements(
            evaluation,
            return_dtype=pl.Struct([
                pl.Field('publico_produto', pl.String),
                pl.Field('publico_produto_rag', pl.String),
                pl.Field('objetivos_metas_relevancia', pl.String),
                pl.Field('objetivos_metas_relevancia_rag', pl.String),
                pl.Field('metodologia_gestao', pl.String),
                pl.Field('metodologia_gestao_rag', pl.String),
                pl.Field('colaboracoes_financiamento', pl.String),
                pl.Field('colaboracoes_financiamento_rag', pl.String),
                pl.Field('potencial_inovacao_empreendedorismo', pl.String),
                pl.Field('potencial_inovacao_empreendedorismo_rag', pl.String),
                pl.Field('demandas_escalabilidade', pl.String),
                pl.Field('demandas_escalabilidade_rag', pl.String),
                pl.Field('maturidade_resultados', pl.String),
                pl.Field('maturidade_resultados_rag', pl.String),
                pl.Field('organizacao_parcerias_extensao', pl.String),
                pl.Field('organizacao_parcerias_extensao_rag', pl.String),
                pl.Field('perfil_tecnologico', pl.String),
                pl.Field('perfil_tecnologico_rag', pl.String),
            ]),
        )
        .alias('info_lattes')
    ).unnest('info_lattes')

    return df_resultado
