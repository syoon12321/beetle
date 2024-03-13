-- https://docs.getdbt.com/blog/managing-surrogate-keys
-- # in macros/generate_sequences.sql

{% macro generate_sequences() %}

    {% if execute %}
      
    {% set models = graph.nodes.values() | selectattr('resource_type', 'eq', 'model') %}
    {# parse through the graph object, find all models with the meta surrogate key config #}
    {% set sk_models = [] %}
    {% for model in models %}
        {% if model.config.meta.surrogate_key %}
          {% do sk_models.append(model) %}
        {% endif %}
    {% endfor %}

    {% endif %}

    {% for model in sk_models %}

        {% if flags.FULL_REFRESH or model.config.materialized == 'table' %}
        {# regenerate sequences if necessary #}

        create or replace sequence {{ model.database }}.{{ model.schema }}.seq_{{ model.name }}_key;

        {% else %}
        {# create only if not exists for incremental models #}
    
        create sequence if not exists {{ model.database }}.{{ model.schema }}.seq_{{ model.name }}_key;
        
        {% endif %}
    
    {% endfor %}
  
{% endmacro %}
