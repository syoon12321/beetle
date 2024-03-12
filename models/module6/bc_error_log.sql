with

    lif as (select * from {{ ref("bc_lineitem_fails") }}),

    bc as (select * from {{ ref("bc_balance_results") }}),

    error_lif as (
        select
            case when count(*) > 0 then 'there are failures!' end as error_message,
            {% if error_macro('bc_lineitem_fails')=='true' %}
                object_construct(*) as error_contents
            {% else %}
                null as error_contents
            {% endif %}
            --case when count(lif.l_orderkey) = 0 then null else object_construct(*) end as error_details            
        from lif
    ),

    error_bc as (
        select
            case
                when source_record_count != target_record_count
                then 'the source and target does not match!'
            end as error_message,
            case
                when source_record_count != target_record_count
                then object_construct(*) 
            end as error_contents
        from bc
    ),

    unioned_tables as (
        select * from error_lif
        union
        select * from error_bc
    ),

    final as (select * from unioned_tables)

select * from final
