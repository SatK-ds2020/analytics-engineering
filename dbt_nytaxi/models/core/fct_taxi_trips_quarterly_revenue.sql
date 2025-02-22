{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+00:00', pickup_datetime) as formatted_pickup_datetime,
    date_trunc(pickup_datetime, quarter) as revenue_quarter, 
    service_type, 
    -- New dimensions
    extract(year FROM pickup_datetime) AS year,
    extract(quarter FROM pickup_datetime) AS quarter,
    CONCAT(CAST(extract(year FROM pickup_datetime) AS STRING), '/Q', CAST(extract(quarter FROM pickup_datetime) AS STRING)) AS year_quarter,
    extract(month FROM pickup_datetime) AS month,
    -- Revenue calculation 
    sum(fare_amount) as revenue_quarterly_fare,
    sum(extra) as revenue_quarterly_extra,
    sum(mta_tax) as revenue_quarterly_mta_tax,
    sum(tip_amount) as revenue_quarterly_tip_amount,
    sum(tolls_amount) as revenue_quarterly_tolls_amount,
    sum(ehail_fee) as revenue_quarterly_ehail_fee,
    sum(improvement_surcharge) as revenue_quarterly_improvement_surcharge,
    sum(total_amount) as revenue_quarterly_total_amount,
    -- Additional calculations
    count(tripid) as total_quarterly_trips,
    avg(passenger_count) as avg_quarterly_passenger_count,
    avg(trip_distance) as avg_quarterly_trip_distance
from 
    trips_data
group by 
    revenue_zone,
    formatted_pickup_datetime,
    revenue_quarter,
    service_type,
    year,
    quarter,
    year_quarter,
    month

    



