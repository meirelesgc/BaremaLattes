INSTRUCAO_BASE = """
Responda com base apenas no documento.
O texto deve ser sucinto, contendo itens separados por vírgulas, de forma estritamente descritiva.
Não utilize adjetivos.
"""

ORIENTACAO_GERAL = """
Baseie-se na seguinte orientação para avaliação do projeto (PESO 2 - 0 a 10):
- 0 a 4: Metodologia frágil, desenvolvimento tecnológico não evidente. Objetivos, metas e relevância para o setor produtivo não estão claros (se não tiver nada, até 3).
- 5 a 6: Metodologia estruturada, desenvolvimento tecnológico e geração de produto ou processo evidentes.
- 7 a 8: Desenvolvimento do produto viável, procedimentos de pesquisa explícitos, com equipe e parcerias estruturadas.
- Acima de 8: Projeto inovador, gera produto ou serviço tecnológico, relação percurso do pesquisador x projeto coerente. Nota 9 ou superior exige comprovação de demanda ou relação com empresa/entidade/terceiro setor/público.
"""

PROMPTS_AVALIACAO = {
    "publico_produto": f"""
    Identifique o público alvo e o produto.
    {INSTRUCAO_BASE}
    """,
    "objetivos_metas_relevancia": f"""
    Identifique os objetivos, as metas e a relevância para o setor produtivo.
    {INSTRUCAO_BASE}
    """,
    "metodologia_gestao": f"""
    Descreva a metodologia e a gestão da execução.
    {INSTRUCAO_BASE}
    """,
    "colaboracoes_financiamento": f"""
    Identifique instituições colaboradoras, empresas financiadoras, e financiamentos anteriores ou atuais de órgãos de fomento.
    {INSTRUCAO_BASE}
    """,
    "potencial_inovacao": f"""
    Identifique o potencial para a produção tecnológica, inovação e ações de empreendedorismo inovador.
    {INSTRUCAO_BASE}
    """,
    "atendimento_necessidades": f"""
    Identifique o atendimento a necessidades de criação ou melhoria de produtos, processos ou serviços demandadas por instituições do ambiente produtivo e social.
    {INSTRUCAO_BASE}
    """,
    "maturidade_resultados": f"""
    Identifique o nível de maturidade tecnológica atual do projeto e os resultados científicos ou tecnológicos alcançados.
    {INSTRUCAO_BASE}
    """,
    "organizacao_parcerias": f"""
    Identifique a organização, coerência com pesquisas em desenvolvimento, parcerias e participação do proponente em atividades de desenvolvimento tecnológico ou extensão inovadora.
    {INSTRUCAO_BASE}
    """,
    "aderencia_area": f"""
    Classifique a aderência à área de tecnologias sociais e educacionais exclusivamente como: alta, média ou baixa.
    {INSTRUCAO_BASE}
    """,
    "parecer_final": f"""
    Elabore o parecer final do projeto indicando os elementos encontrados que justificam a nota, utilizando as diretrizes de pontuação.
    {ORIENTACAO_GERAL}
    {INSTRUCAO_BASE}
    """,
}


PROMPT_BAREMA_NOVO = """
Você deve extrair informações do documento e retornar EXCLUSIVAMENTE um objeto JSON com as seguintes chaves:

1. "sumula": Texto com até 5 realizações, formação, histórico profissional, financiamentos, indicadores e links de bases.
2. "transferencia_tecnologia_nota": Número inteiro correspondente à nota de impacto/abrangência.
3. "transferencia_tecnologia_observacao": Justificativa textual da nota atribuída.
4. "extensao_inovadora_nota": Número inteiro correspondente à nota da ação de extensão.
5. "extensao_inovadora_observacao": Justificativa textual da nota atribuída.
6. "trajetoria_proponente": Número inteiro correspondente à nota de aderência.
7. "trajetoria_proponente_observacao": Justificativa textual da nota atribuída.

Seja rigido com os critérios.
Para as justificativas, enumere separado por ponto e virgula (;) de forma coesa e desctiva, sem juizo de valor.

Critérios para Transferência de Tecnologia (Impacto/Abrangência):
- 10: Nacional e Estado
- 9: Regional (mais de 1 município)
- 8: Município
- 7: Entidade/Organização

Critérios para Extensão Inovadora:
- 10: Processo formativo no contexto de transferência de tecnologia
- 9: Parcerias público/privadas para transferência de conhecimento
- 8: Relação com projetos de extensão
- 7: Popularização da ciência

Critérios para Trajetória do Proponente em Tecnologias Sociais e Educacionais:
- 10: Aderência com a área ALTA
- 7: Aderência com a área MÉDIA
- 4: Aderência com a área BAIXA
"""
