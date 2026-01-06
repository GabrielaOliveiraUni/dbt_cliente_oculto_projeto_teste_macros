{{ config(
    materialized = 'incremental',
    unique_key = 'CD_ESPECIALIDADE'
)}}

WITH CTE AS (
    select
C9_DS_ESPECIALIDADES AS DS_ESPECIALIDADE,
C9_NR_PRAZO_ANS AS NR_PRAZO,
CASE WHEN C9_NR_PRAZO_ANS = 7 THEN 'URGENTE' ELSE 'NÃO URGENTE' end AS GRUPO_ESPECIALIDADE
FROM {{source('APP_GESTAO_REDE','ST_LISTAS_CLIENTE_OCULTO')}}
)

SELECT
    /* =========================
       SURROGATE KEY
    ========================== */
    {{ gerar_chave_incremental(
        colunas_ordenacao = ['DS_ESPECIALIDADE'],
        nome_coluna       = 'CD_ESPECIALIDADE'
    ) }},

    /* =========================
       COLUNAS DE NEGÓCIO
    ========================== */
    DS_ESPECIALIDADE,
    NR_PRAZO,
    GRUPO_ESPECIALIDADE

FROM CTE

{% if is_incremental() %}
WHERE DS_ESPECIALIDADE NOT IN (
    SELECT DS_ESPECIALIDADE
    FROM {{ this }}
)
{% endif %}
