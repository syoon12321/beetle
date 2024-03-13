{% snapshot tsdw_shipment_snapshot %}

{{
    config(
      target_schema='dbt_syoon_timeless_shipping',
      unique_key="shipper||'&'||shipper_contact",

      strategy='check',
      check_cols=['shipper', 'shipper_contact']
    )
}}

select * from {{ source('tsdw', 'shipment') }}

{% endsnapshot %}