{{ config(
    materialized = 'incremental',
    unique_key = 'CD_AGENDAMENTO'
)}}

WITH CTE AS (
    SELECT DISTINCT
    substr(co.POSSUI_AGENDA_PLANO, 7) AS DS_MOTIVO ,
	substr(co.POSSUI_AGENDA_PLANO, 1, 3) AS DS_AGENDA_PLANO,
    co.POSSUI_AGENDA_PLANO AS DS_MOTIVO_COMPLETO
FROM {{source('APP_GESTAO_REDE','ST_CLIENTE_OCULTO')}} CO
)
SELECT
    /* =========================
       SURROGATE KEY
    ========================== */
    {{ gerar_chave_incremental(
        colunas_ordenacao = ['DS_MOTIVO_COMPLETO'],
        nome_coluna       = 'CD_AGENDAMENTO'
    ) }},

    /* =========================
       COLUNAS DE NEGÓCIO
    ========================== */
    DS_MOTIVO,
    DS_AGENDA_PLANO,
    DS_MOTIVO_COMPLETO

FROM CTE

{% if is_incremental() %}
WHERE DS_MOTIVO_COMPLETO NOT IN (
    SELECT DS_MOTIVO_COMPLETO
    FROM {{ this }}
)
{% endif %}
