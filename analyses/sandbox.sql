select
    case when count(*) > 0 then 'there are failures!' end as error_message,
    {% if error_macro('bc_lineitem_fails')=='true' %}
        object_construct(*) as test
    {% else %}
        0 as test
    {% endif %},
    count(*) as count_check

from {{ ref('bc_lineitem_fails') }}
