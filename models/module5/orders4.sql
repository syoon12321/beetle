{{
    config(
        materialized='incremental'
    )
}}

with final as (
    select
        *,
        current_timestamp() as audit_insert_dttm,
        current_timestamp() as audit_update_dttm
    from {{ ref('stg_raw__il_order') }}

    {% if is_incremental() %}

    where o_updatedate > (select max(o_updatedate) from {{this}})

    {% endif %}
)

select * from final