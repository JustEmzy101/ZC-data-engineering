{{ config(materialized='view') }}
with source as (
    select * from {{ source("staging",'output_2024-12')}}

),

 renamed as ( 

    select
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as  tripid,
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,    
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,	
        PULocationID,
        DOLocationID,
        payment_type,
        {{ get_payment_type_description("payment_type")}} as payment_description,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee

    from source
 )

 select * from renamed