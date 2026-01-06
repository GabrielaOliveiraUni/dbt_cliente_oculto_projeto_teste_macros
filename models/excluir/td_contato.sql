{{ config(
    materialized = 'incremental',
    unique_key = 'CD_CONTATO'
)}}

SELECT 
    DISTINCT {{ dbt_utils.generate_surrogate_key(['C7_DS_FALTA_CONTATO']) }} AS CD_CONTATO,
    C7_DS_FALTA_CONTATO AS DS_CONTATO
FROM {{source('APP_GESTAO_REDE','ST_LISTAS_CLIENTE_OCULTO')}}


