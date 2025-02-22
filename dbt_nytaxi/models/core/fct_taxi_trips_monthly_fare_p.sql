{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
), 
valid_trips as (
    select *
    from trips_unioned
    where fare_amount > 0 
      and trip_distance > 0 
      and payment_type_description in ('Cash', 'Credit Card')
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
trips_with_zones as (
    select valid_trips.tripid, 
        valid_trips.vendorid, 
        valid_trips.service_type,
        valid_trips.ratecodeid, 
        valid_trips.pickup_locationid, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        valid_trips.dropoff_locationid,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        valid_trips.pickup_datetime, 
        valid_trips.dropoff_datetime, 
        valid_trips.store_and_fwd_flag, 
        valid_trips.passenger_count, 
        valid_trips.trip_distance, 
        valid_trips.trip_type, 
        valid_trips.fare_amount, 
        valid_trips.extra, 
        valid_trips.mta_tax, 
        valid_trips.tip_amount, 
        valid_trips.tolls_amount, 
        valid_trips.ehail_fee, 
        valid_trips.improvement_surcharge, 
        valid_trips.total_amount, 
        valid_trips.payment_type, 
        valid_trips.payment_type_description
    from valid_trips
    inner join dim_zones as pickup_zone
    on valid_trips.pickup_locationid = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on valid_trips.dropoff_locationid = dropoff_zone.locationid
),
percentile_calculation as (
    select 
        service_type,
        date_trunc(pickup_datetime, year) as year, 
        date_trunc(pickup_datetime, month) as month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, date_trunc(pickup_datetime, year), date_trunc(pickup_datetime, month)) as fare_amount_p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, date_trunc(pickup_datetime, year), date_trunc(pickup_datetime, month)) as fare_amount_p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, date_trunc(pickup_datetime, year), date_trunc(pickup_datetime, month)) as fare_amount_p90
    from trips_with_zones
    where date_trunc(pickup_datetime, year) = '2020-01-01'
    AND date_trunc(pickup_datetime, month) = '2020-04-01'
)

select 
    service_type, 
    year,
    month, 
    fare_amount_p97,
    fare_amount_p95,
    fare_amount_p90
from percentile_calculation
group by service_type, year, month, fare_amount_p97, fare_amount_p95, fare_amount_p90
