{% macro create_index_safe(index_name, table_name, columns, unique=false) %}

{% set unique_sql = 'UNIQUE' if unique else '' %}

{% set sql %}
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM user_indexes
    WHERE index_name = UPPER('{{ index_name }}');

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE {{ unique_sql }} INDEX {{ index_name }}
            ON {{ table_name }} ({{ columns | join(", ") }})
        ';
    END IF;
END;
{% endset %}

{{ return(sql) }}

{% endmacro %}
