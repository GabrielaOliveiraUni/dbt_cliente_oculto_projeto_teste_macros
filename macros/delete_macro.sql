{% macro delete_macro(table_name) %}

    {% if execute %} 
    {% do log("--------------------------------------------------------------------------------", info=True) %}
    {% do log("### DELETE FATO - " ~ table_name ~ " ###", info=True) %}

    {% set sql_nr_competencia_inicial %}
        SELECT
            TO_CHAR(TRUNC(ADD_MONTHS(SYSDATE, -12), 'MM'), 'YYYYMM') AS NR_COMPETENCIA_INICIAL
        FROM DUAL
        -- SELECT '000001' AS NR_COMPETENCIA_INICIAL
        -- FROM DUAL

    {% endset %}

    {% set result = run_query(sql_nr_competencia_inicial) %}
    {% set v_nr_competencia_inicial = result.rows[0][0] %}
    {% do log("### Competencia Inicial: " ~ v_nr_competencia_inicial ~ " ###", info=True) %}

    {% set sql_nr_competencia_final %}
        SELECT
            TO_CHAR(TRUNC(ADD_MONTHS(SYSDATE, +1), 'MM'), 'YYYYMM') AS NR_COMPETENCIA_FINAL
        FROM DUAL
    {% endset %}

    {% set result2 = run_query(sql_nr_competencia_final) %}
    {% set v_nr_competencia_final = result2.rows[0][0] %}
    {% do log("### Competencia Final: " ~ v_nr_competencia_final ~ " ###", info=True) %}

    {% set delete_query %}
    DELETE {{ table_name }} D
     WHERE D.NR_DATA_CONTATO >= {{ v_nr_competencia_inicial }}
       AND D.NR_DATA_CONTATO <= {{ v_nr_competencia_final }}
    {% endset %}
    
    {% do run_query(delete_query) %}
    {% do log("### Registros deletados da tabela " ~ table_name ~ " ###", info=True) %}
    {% do log("--------------------------------------------------------------------------------", info=True) %}
    {% endif %} 

{% endmacro %}