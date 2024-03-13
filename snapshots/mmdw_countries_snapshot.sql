{% snapshot mmdw_countries_snapshot %}

{{
    config(
      target_schema='dbt_syoon_timeless_shipping',
      unique_key='country_id',

      strategy='check',
      check_cols='all'
    )
}}

select * from {{ source('mmdw', 'countries') }}

{% endsnapshot %}