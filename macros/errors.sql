{% macro error_macro(model_name) %}
    select 
        case 
            when count(*) is null false
            when count(*)=0 false 
            else true 
        end as return_if_error
    from {{ ref(model_name) }}
{% endmacro %}