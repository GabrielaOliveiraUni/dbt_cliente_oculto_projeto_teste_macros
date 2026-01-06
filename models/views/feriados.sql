{{ config(
    materialized = 'table',
    unique_key = 'DT_FERIADOS'
)}}

SELECT 
    C11_DT_LISTA_FERIADOS AS DT_FERIADOS
FROM
    {{source('APP_GESTAO_REDE','ST_LISTAS_CLIENTE_OCULTO')}}
WHERE C11_DT_LISTA_FERIADOS <> '01/01/0001'