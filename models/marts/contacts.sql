{{
    config(
        materialized='incremental',
        unique_key='contact_key'
    )
}}

with 

--Import
tsdw_shipment_source as (
    select 
        shipper,
        shipper_contact,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('tsdw_shipment_snapshot') }}
),

mmdw_contact_source as (
    select
        contact_id, 
        first_name,
        last_name,
        company_name,
        business_phone_number,
        business_phone_extension,
        mobile_phone_number,
        home_phone_number,
        address_line_1,
        address_line_2,
        address_line_3,
        city,
        state,
        postal_code,
        country_id,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('mmdw_contact_snapshot') }}
),

countries_seed_data as (
    select * from {{ ref('countries') }}
),

--Logical
tsdw_contacts as (
    select 
        {{ increment_sequence() }} as contact_key,
        case 
            when position(',' in shipper)>0 then substring(shipper, position(',' in shipper)+1)
            when position(' ' in shipper)>0 then substring(shipper, 1, position(' ' in shipper)-1)
            else shipper end  
        as first_name,
        case 
            when position(',' in shipper)>0 then substring(shipper, 1, position(',' in shipper)-1)
            when position(' ' in shipper)>0 then substring(shipper, position(' ' in shipper)+1)
            else null end  
        as last_name,
        null as company_name,
        shipper_contact as business_phone_num,
        null as business_phone_extension,
        null as mobile_phone_num,
        null as home_phone_num,
        null as address_line_1,
        null as address_line_2,
        null as address_line_3,
        null as city,
        null as state,
        null as postal_code,
        null as country_key,
        'TSDW.shipping_load' as record_source_system,
        null as record_source_system_id,
        --Plumbing columns
        row_number() over (partition by contact_key order by dbt_valid_from) as record_version_num,
        dbt_valid_from as record_eff_start_dttm,
        coalesce(dbt_valid_to, '{{var('the_distant_future')}}') as record_eff_end_dttm,
        case when dbt_valid_to is null then 'Yes' else 'No' end as record_current_flg
    from tsdw_shipment_source
),

mmdw_contacts as (
    select
        {{increment_sequence()}} as contact_key,
        first_name,
        last_name,
        company_name,
        business_phone_number as business_phone_num,
        business_phone_extension,
        mobile_phone_number as mobile_phone_num,
        home_phone_number as home_phone_num,
        address_line_1,
        address_line_2,
        address_line_3,
        city,
        state,
        postal_code,
        countries_seed_data.country_key as country_key, --FIX!
        'MMDW.contact' as record_source_system,
        contact_id as record_source_system_id,
        --Plumbing columns
        row_number() over (partition by contact_key order by dbt_valid_from) as record_version_num,
        dbt_valid_from as record_eff_start_dttm,
        coalesce(dbt_valid_to, '{{var('the_distant_future')}}') as record_eff_end_dttm,
        case when dbt_valid_to is null then 'Yes' else 'No' end as record_current_flg
    from mmdw_contact_source
    join countries_seed_data on mmdw_contact_source.country_id = countries_seed_data.country_key
),

-- Final
final as (
    select * from tsdw_contacts
    union
    select * from mmdw_contacts
)

select * from final

{% if is_incremental() %}

where
  record_eff_start_dttm > (select max(record_eff_start_dttm) from {{ this }})

{% endif %}