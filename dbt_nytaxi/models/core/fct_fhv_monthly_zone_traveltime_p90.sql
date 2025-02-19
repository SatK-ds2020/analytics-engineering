{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select *,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
), 
percentile_calculation as (
    select 
        pickup_datetime,
        formatted_pickup_datetime,
        formatted_dropoff_datetime,
        date_trunc(pickup_datetime, year) as year, 
        date_trunc(pickup_datetime, month) as month,
        pickup_locationid,
        pickup_zone, 
        dropoff_locationid,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY date_trunc(pickup_datetime, year), date_trunc(pickup_datetime, month), pickup_locationid, dropoff_locationid) as trip_duration_p90
    from fhv_trips
)

select
    pickup_datetime,
    formatted_pickup_datetime,
    formatted_dropoff_datetime,
    pickup_zone, 
    dropoff_zone, 
    year, 
    month, 
    row_number() over (partition by pickup_zone order by trip_duration_p90 desc) as row_num,
    dense_rank() over (partition by pickup_zone order by trip_duration_p90 desc) as dense_rnk,
    trip_duration_p90
from percentile_calculation
where pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
  and date_trunc(pickup_datetime, year) = '2019-01-01'
  and date_trunc(pickup_datetime, month) = '2019-11-01'


  
