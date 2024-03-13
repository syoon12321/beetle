with 

source as (

    select * from {{ source('mmdw', 'contact') }}

),

renamed as (

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
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
