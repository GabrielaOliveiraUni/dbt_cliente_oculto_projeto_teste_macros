{{ config(
    materialized = 'incremental',
    unique_key = 'CD_TENTATIVA'
)}}

WITH CTE AS (
SELECT 
    DISTINCT  STATUS_TENTATIVA AS DS_STATUS_TENTATIVA
FROM {{source('APP_GESTAO_REDE','ST_CLIENTE_OCULTO')}}
)

SELECT
    /* =========================
       SURROGATE KEY
    ========================== */
    {{ gerar_chave_incremental(
        colunas_ordenacao = ['DS_STATUS_TENTATIVA'],
        nome_coluna       = 'CD_TENTATIVA'
    ) }},

    /* =========================
       COLUNAS DE NEGÓCIO
    ========================== */
        DS_STATUS_TENTATIVA


FROM CTE

{% if is_incremental() %}
WHERE DS_STATUS_TENTATIVA NOT IN (
    SELECT DS_STATUS_TENTATIVA
    FROM {{ this }}
)
{% endif %}
