{# ============================================================================
   MACRO: gerar_chave_incremental
   Para modelos incrementais - mantém IDs antigos e gera novos sequenciais
   Compatível com ORACLE
   
   Parâmetros:
   - colunas_ordenacao: Lista de colunas para ordenação
   - nome_coluna: Nome da coluna de ID (padrão: 'id')
   - tabela_destino: Tabela de destino (padrão: this)
   - prefixo: Prefixo numérico opcional (ex: 100, 200, 5000)
============================================================================ #}
{% macro gerar_chave_incremental(
    colunas_ordenacao,
    nome_coluna='id',
    tabela_destino=this,
    prefixo=none
) %}
    {% set ordem = colunas_ordenacao | join(', ') %}
    
    {% if is_incremental() %}
        {% set max_id_query %}
            SELECT NVL(MAX({{ nome_coluna }}), {% if prefixo %}{{ prefixo }}{% else %}0{% endif %}) AS max_id
            FROM {{ tabela_destino }}
        {% endset %}
        
        {% set resultado = run_query(max_id_query) %}
        {% set max_id = resultado.columns[0].values()[0] if resultado and resultado.columns[0].values() | length > 0 else (prefixo if prefixo else 0) %}
        
        {{ max_id }} + ROW_NUMBER() OVER (ORDER BY {{ ordem }}) AS {{ nome_coluna }}
    {% else %}
        {% if prefixo %}
            {{ prefixo }} + ROW_NUMBER() OVER (ORDER BY {{ ordem }}) AS {{ nome_coluna }}
        {% else %}
            ROW_NUMBER() OVER (ORDER BY {{ ordem }}) AS {{ nome_coluna }}
        {% endif %}
    {% endif %}
{% endmacro %}