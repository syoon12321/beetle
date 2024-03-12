{% macro check_column_has_data(table_name, column_name) %}
  {# Use the ref function to dynamically reference the table #}
  {# Use the count function to count the number of non-null values in the specified column #}
  {% set count_query = "select count(" ~ column_name ~ ") as count_records from " ~ ref(table_name) %}
  
  {# Execute the count query and store the result in count_result #}
  {% set count_result = run_query(count_query) %}
  
  {# Extract the count value from the count_result #}
  {% set count_value = count_result.columns[0].values[0] %}
  
  {# Check if the count value is greater than 0 #}
  {% if count_value > 0 %}
    true
  {% else %}
    false  
  {% endif %}
{% endmacro %}
