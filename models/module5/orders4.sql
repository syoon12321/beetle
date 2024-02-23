{{
    config(
        materialized='incremental'
    )
}}

select
    *,
    current_timestamp() as audit_insert_dttm,
    current_timestamp() as audit_update_dttm
from {{ source('staging', 'stg_raw__il_order') }}