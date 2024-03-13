{% snapshot mmdw_contact_snapshot %}

{{
    config(
      target_schema='dbt_syoon_timeless_shipping',
      unique_key='contact_id',

      strategy='check',
      check_cols=['contact_id']
    )
}}

select * from {{ source('mmdw', 'contact') }}

{% endsnapshot %}