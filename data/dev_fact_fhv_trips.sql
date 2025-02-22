{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *, 
     'FHV' as service_type     
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select  
    fhv_data.dispatchid,
    fhv_data.service_type,
    fhv_data.pickup_datetime,    
    fhv_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_data.dropoff_datetime, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.sr_flag 
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid

-- dbt build --select fact_fhv_trip --vars '{'is_test_run': 'false'}'
---select count(*) from {{ ref('fact_fhv_trip') }} 22998722
---select count(*) from {{ ref('fact_trip)}}
---select service_type, count(*) from {{ ref('fact_trip)}} 
---where extract(year from pickup_datetime)=2019
--- AND extract(month from pickup_datetime)=7
```sql
with GY as (
    SELECT  service_type,count(*) as total_records
    FROM {{ ref('fact_trip') }}
    WHERE extract(year from pickup_datetime)=2019
    and extract(month from pickup_datetime)=7
    GROUP BY service_type
    ),
    fhv as (
    SELECT  service_type,count(*) as total_records
    FROM {{ ref('fact_fhv_trip') }}
    WHERE extract(year from pickup_datetime)=2019
    and extract(month from pickup_datetime)=7
    GROUP BY service_type
    )
    select * from GY
    union all
    select * from fhv


```
```sql
WITH GY AS (
  SELECT service_type, COUNT(*) AS total_records
  FROM `de-zoomcamp2025-448100.trips_data_all.fact_trips`
  WHERE pickup_datetime BETWEEN '2019-07-01' AND '2019-07-31'
  GROUP BY service_type
),
fhv AS (
  SELECT service_type, COUNT(*) AS total_records
  FROM `de-zoomcamp2025-448100.trips_data_all.fact_fhv_trips`
  WHERE pickup_datetime BETWEEN '2019-07-01' AND '2019-07-31'
  GROUP BY service_type
)
SELECT * FROM GY
UNION ALL
SELECT * FROM fhv
```
------------------------------------------------------------------------------------------------

{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *,      
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_data.fhvtripid, 
    fhv_data.dispatchid,
    fhv_data.affliationid,    
    fhv_data.pickup_datetime,    
    fhv_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_data.dropoff_datetime, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.sr_flag 
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid

-- dbt build --select fact_fhv_trip --vars '{'is_test_run': 'false'}'