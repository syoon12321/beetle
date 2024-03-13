with 

source as (

    select * from {{ source('tsdw', 'shipment') }}

),

renamed as (

    select
        _fivetran_id,
        ship_flag_country_code,
        ship_mmsi_number,
        origination_country,
        shipper_contact,
        destination_country,
        receiver,
        ship_name,
        ship_registration_number,
        origination_port,
        scheduled_departure,
        shipment_id,
        destination_port,
        shipper,
        ship_call_sign,
        ship_imo,
        ship_load_volume,
        actual_departure,
        scheduled_arrival,
        shipment_notes,
        ship_load_weight,
        actual_arrival,
        receiver_contact,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
