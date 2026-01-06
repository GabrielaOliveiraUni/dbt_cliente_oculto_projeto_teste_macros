{{
    config(
        materialized = 'incremental',
        unique_key = 'CHAVE_NATURAL'
    )
}}

WITH base AS (

    SELECT DISTINCT
        /* =========================
           CHAVE NATURAL
        ========================== */
        co.NOME_RAZAO_SOCIAL || '|' ||
        ende.CD_ENDERECO || '|' ||
        co.CD_PRESTADOR_RNP || '|' ||
        UPPER(co.CICLO_PESQUISA)        AS CHAVE_NATURAL,

        /* =========================
           ATRIBUTOS
        ========================== */
        co.NOME_RAZAO_SOCIAL            AS DS_RAZAO_SOCIAL,
        co.CPF_CNPJ                     AS CD_CPF_CNPJ,
        co.CD_PRESTADOR_RNP             AS CD_RNP,
        co.UNIMED_AREA_ACAO,
        UPPER(co.CICLO_PESQUISA)        AS DS_CICLO_PESQUISA,
        e.CD_ESPECIALIDADE,
        ende.CD_ENDERECO,
        co.telefone_01,

        CASE
            WHEN co.telefone_01 IS NOT NULL
                THEN '(' || co.ddd_telefone_01 || ') ' || co.telefone_01
            WHEN co.telefone_02 IS NOT NULL
                THEN '(' || co.ddd_telefone_02 || ') ' || co.telefone_02
            ELSE 'Não existe telefone'
        END AS DS_TELEFONE,

        CASE
            WHEN co.celular_whatsapp IS NOT NULL
                THEN '(' || co.ddd_celular_whatsapp || ') ' || co.celular_whatsapp
            ELSE 'Não existe Whatsapp'
        END AS DS_WHATSAPP

    FROM {{ source('APP_GESTAO_REDE','ST_CLIENTE_OCULTO') }} co
        LEFT JOIN {{ ref('td_especialidade') }} e
            ON e.DS_ESPECIALIDADE = co.ESPECIALIDADE
        LEFT JOIN {{ ref('td_endereco') }} ende
            ON ende.CD_CEP    = co.CEP
           AND ende.DS_BAIRRO = co.BAIRRO
           AND ende.DS_RUA    = co.ENDERECO
           AND ende.NR_NUMERO = co.NUMERO
)

SELECT
    /* =========================
       SURROGATE KEY
    ========================== */
    {{ gerar_chave_incremental(
        colunas_ordenacao = ['CHAVE_NATURAL'],
        nome_coluna       = 'CD_PRESTADOR'
    ) }},

    /* =========================
       COLUNAS DE NEGÓCIO
    ========================== */
    CHAVE_NATURAL,
    DS_RAZAO_SOCIAL,
    CD_CPF_CNPJ,
    CD_RNP,
    UNIMED_AREA_ACAO,
    DS_CICLO_PESQUISA,
    CD_ESPECIALIDADE,
    CD_ENDERECO,
    DS_TELEFONE,
    DS_WHATSAPP,
    telefone_01

FROM base

{% if is_incremental() %}
WHERE CHAVE_NATURAL NOT IN (
    SELECT CHAVE_NATURAL
    FROM {{ this }}
)
{% endif %}
