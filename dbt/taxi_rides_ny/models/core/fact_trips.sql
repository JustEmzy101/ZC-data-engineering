{{
    config(
        materialized='table'
    )
}}

with november as (
    select *,
    'November' as fiscal_month
    from {{ ref('stg_2024_11') }}
),
december as (
    select *,
    'December' as fiscal_month
    from {{ ref('stg_2024_12') }}
),

trips_unioned as (
    select *
    from december
    union all
     select *
    from november
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where feed_borough != "Unknown"
)

select * 
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.PULocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.DOLocationID = pickup_zone.locationid