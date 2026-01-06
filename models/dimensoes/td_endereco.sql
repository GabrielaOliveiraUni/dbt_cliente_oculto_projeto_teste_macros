{{ config(
    materialized = 'incremental',
    unique_key = 'CD_ENDERECO'
)}}

with CTE AS (
SELECT DISTINCT
    -- 1 AS CD_ENDERECO,
    (MUNICIPIO || CEP|| BAIRRO || ENDERECO || NUMERO || UF ) as DS_ENDERECO,
    MUNICIPIO AS DS_MUNICIPIO,
    BAIRRO AS DS_BAIRRO,
    CEP AS CD_CEP,
    UF AS CD_UF,
    ENDERECO AS DS_RUA,
    NUMERO AS NR_NUMERO
FROM {{source('APP_GESTAO_REDE','ST_CLIENTE_OCULTO')}}
)

SELECT
    /* =========================
       SURROGATE KEY
    ========================== */
    {{ gerar_chave_incremental(
        colunas_ordenacao = ['DS_ENDERECO'],
        nome_coluna       = 'CD_ENDERECO',
        prefixo = 1000
    ) }},

    /* =========================
       COLUNAS DE NEGÓCIO
    ========================== */
    DS_MUNICIPIO,
    DS_BAIRRO,
    CD_CEP,
    CD_UF,
    DS_RUA,
    NR_NUMERO

FROM CTE

{% if is_incremental() %}
WHERE DS_ENDERECO  NOT IN (
    SELECT DS_ENDERECO 
    FROM {{ this }}
)
{% endif %}
