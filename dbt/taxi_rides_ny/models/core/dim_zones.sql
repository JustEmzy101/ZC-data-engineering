{{ config(materialized='table') }}

select 
    locationid as feed_loc_id, 
    borough as feed_borough, 
    zone as feed_zone, 
    replace(service_zone,'Boro','Green') as feed_service_zone 
from {{ ref('taxi_zone_lookup') }}