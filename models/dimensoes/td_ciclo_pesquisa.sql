{{ config(
    materialized = 'table',
    unique_key = 'CD_CICLO_PESQUISA'
)}}
WITH CTE AS (
SELECT
    DISTINCT 
	UPPER(CO.CICLO_PESQUISA) AS DS_CICLO_PESQUISA,
    CASE 
        WHEN 
            SUBSTR(CO.CICLO_PESQUISA, INSTR(CO.CICLO_PESQUISA, '-') + 2, 4)
            || LPAD(SUBSTR(CO.CICLO_PESQUISA, 1, INSTR(CO.CICLO_PESQUISA, 'º') - 1), 2, '0')
            =
            (
                SELECT MAX(
                    SUBSTR(CI.CICLO_PESQUISA, INSTR(CI.CICLO_PESQUISA, '-') + 2, 4)
                    || LPAD(SUBSTR(CI.CICLO_PESQUISA, 1, INSTR(CI.CICLO_PESQUISA, 'º') - 1), 2, '0')
                )
                FROM {{source('APP_GESTAO_REDE','ST_CLIENTE_OCULTO')}} CI
            )
        THEN 1 
        ELSE 0
    END AS FG_CICLO_ATUAL,
    SUBSTR(CO.CICLO_PESQUISA, -4) as NR_ANO
FROM
	{{source('APP_GESTAO_REDE','ST_CLIENTE_OCULTO')}}  CO
)

SELECT
    /* =========================
       SURROGATE KEY
    ========================== */
    {{ gerar_chave_incremental(
        colunas_ordenacao = ['DS_CICLO_PESQUISA'],
        nome_coluna       = 'CD_CICLO_PESQUISA',
        prefixo = 1000
    ) }},

    /* =========================
       COLUNAS DE NEGÓCIO
    ========================== */
    DS_CICLO_PESQUISA,
    FG_CICLO_ATUAL,
    NR_ANO

FROM CTE

{% if is_incremental() %}
WHERE DS_CICLO_PESQUISA NOT IN (
    SELECT DS_CICLO_PESQUISA
    FROM {{ this }}
)
{% endif %}
