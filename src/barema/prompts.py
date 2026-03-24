INSTRUCAO_BASE = """
Responda à pergunta com base apenas no documento fornecido.
Elenque até três exemplos separados por virgulas
Pule duas linhas e transcreva o trecho exato que contem esses exemplos
"""

PROMPTS_AVALIACAO = {
    "publico_produto": f"""
Qual é o público-alvo e o produto exato proposto pelo projeto?
{INSTRUCAO_BASE}
""",
    "objetivos_metas_relevancia": f"""
    Quais são os objetivos, as metas e a relevância deste projeto para o setor produtivo?
    {INSTRUCAO_BASE}
""",
    "metodologia_gestao": f"""
    Como estão estruturadas a metodologia e a gestão da execução do projeto?
    {INSTRUCAO_BASE}
""",
    "colaboracoes_financiamento": f"""
    O projeto cita instituições colaboradoras, empresas financiadoras, ou algum financiamento anterior/atual de órgão de fomento?
    {INSTRUCAO_BASE}
""",
    "potencial_inovacao_empreendedorismo": f"""
    Qual é o potencial do projeto para a produção tecnológica, inovação e para ações de empreendedorismo inovador?
    {INSTRUCAO_BASE}
""",
    "demandas_escalabilidade": f"""
    O projeto atende a demandas reais do mercado ou sociedade? Existe indicação clara de como será a escalabilidade e a adoção em larga escala da solução?
    {INSTRUCAO_BASE}
""",
    "maturidade_resultados": f"""
    Qual é o nível de maturidade tecnológica atual (TRL) do projeto e quais resultados científicos e tecnológicos já foram alcançados?
    {INSTRUCAO_BASE}
""",
    "organizacao_parcerias_extensao": f"""
    Como é descrita a organização do projeto, a coerência com as pesquisas em desenvolvimento, as parcerias e a participação clara do proponente em atividades de desenvolvimento tecnológico ou extensão inovadora?
    {INSTRUCAO_BASE}
""",
    "perfil_tecnologico": f"""
    Qual é o perfil de enquadramento do projeto: EDU (Tecnologias Educacionais) ou SOC (Tecnologias Sociais)?
    {INSTRUCAO_BASE}
""",
}
