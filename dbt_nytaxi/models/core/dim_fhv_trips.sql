{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
     'Fhv' as service_type     
    from {{ ref('stg_fhv_tripdata') }}
    where dispatchid IS NOT NULL
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select  
    fhv_tripdata.dispatchid,
    fhv_tripdata.service_type,
    pickup_datetime,
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+00:00', pickup_datetime) as formatted_pickup_datetime,
    
            - -- New dimensions
    extract(year FROM pickup_datetime) AS year,
    extract(quarter FROM pickup_datetime) AS quarter,
    extract(month FROM pickup_datetime) AS month,    
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_datetime,
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+00:00', dropoff_datetime) as formatted_dropoff_datetime, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.sr_flag 
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid

