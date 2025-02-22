# DBT WorkFlow


# modified dbt-projcet.yml
```

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'taxi_rides_ny'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: 'default'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In dbt, the default materialization for a model is a view. This means, when you run 
# dbt run or dbt build, all of your models will be built as a view in your data platform. 
# The configuration below will override this setting for models in the example folder to 
# instead be materialized as tables. Any models you add to the root of the models folder will 
# continue to be built as views. These settings can be overridden in the individual model files
# using the `{{ config(...) }}` macro.

models:
  taxi_rides_ny:

```

# created initial schema schema.yml
```
version: 2

sources:
  - name: staging
    database: de-zoomcamp2025-448100
    schema: trips_data_all
      # loaded_at_field: record_loaded_at
    tables:
      - name: green_tripdata
      - name: yellow_tripdata
```
# created macro get_payement_type_description
```
{#
    This macro returns the description of the payment_type 
#}

{% macro get_payment_type_description(payment_type) -%}

    case cast( {{payment_type}} as integer )
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end

{%- endmacro %}
```
# created packages.yml for dependencies
```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

# created initial model stg_green_tripdata.sql
```
with 

source as (
  select * from {{source('staging','green_tripdata')}}
     
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid','lpep_pickup_datetime']) }} as trip_id,

        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_desc,
        trip_type,
        congestion_surcharge

    from source
)

select * from renamed
```


```
SELECT pickup_datetime,pickup_zone, dropoff_zone,row_num,dense_rnk,trip_duration_p90 FROM `de-zoomcamp2025-448100.trips_data_all.fct_fhv_monthly_zone_traveltime_p90` 
WHERE dense_rnk=2
ORDER BY trip_duration_p90 desc;


SELECT service_type, fare_amount_p97,fare_amount_p95,fare_amount_p90 
FROM {{ ref('fct_taxi_trips_monthly_fare_p95')}} 
WHERE EXTRACT(YEAR from year)= 2020
  AND EXTRACT(MONTH from month) = 4



SELECT 
    latest_year.year_quarter AS Year_Quarter,
    latest_year.service_type,
    latest_year.quarter,
    latest_year.revenue_total_amount AS revenue_2020, 
    previous_year.revenue_total_amount AS revenue_2019, 
    ((latest_year.revenue_total_amount - previous_year.revenue_total_amount) / previous_year.revenue_total_amount) * 100 AS YoY_Growth
FROM 
    {{ ref('fct_taxi_trips_quarterly_revenue')}} AS latest_year
JOIN 
    {{ ref('fct_taxi_trips_quarterly_revenue')}} AS previous_year
ON 
    latest_year.year_quarter = previous_year.year_quarter
AND 
    latest_year.service_type = previous_year.service_type
AND 
    latest_year.year = 2020
AND 
    previous_year.year = 2019



SELECT pickup_datetime,pickup_zone, dropoff_zone,row_num,dense_rnk,trip_duration_p90 FROM `de-zoomcamp2025-448100.trips_data_all.fct_fhv_monthly_zone_traveltime_p90` 
WHERE dense_rnk=2
ORDER BY trip_duration_p90 desc;

```
```
WITH quarterly_revenue AS (
    SELECT 
        year_quarter,
        year,
        quarter,
        service_type,
        SUM(revenue_quarterly_total_amount) AS agg_total_amount
    FROM 
        `de-zoomcamp2025-448100.trips_data_all.fct_taxi_trips_quarterly_revenue`
    GROUP BY 
        year_quarter, year, quarter, service_type
),
revenue_growth AS (
    SELECT 
        latest_year.year_quarter,
        latest_year.service_type,
        latest_year.quarter,
        latest_year.agg_total_amount AS revenue_2020, 
        previous_year.agg_total_amount AS revenue_2019, 
        ((latest_year.agg_total_amount - previous_year.agg_total_amount) / previous_year.agg_total_amount) * 100 AS YoY_Growth
    FROM 
        quarterly_revenue AS latest_year
    JOIN 
        quarterly_revenue AS previous_year
    ON 
        latest_year.quarter = previous_year.quarter
    AND 
        latest_year.service_type = previous_year.service_type
    AND 
        latest_year.year = 2020
    AND 
        previous_year.year = 2019
),

best_worst_quarters AS (
    SELECT
        service_type,
        year_quarter,
        YoY_Growth,
        ROW_NUMBER() OVER (PARTITION BY service_type ORDER BY YoY_Growth DESC) AS rn_best,
        ROW_NUMBER() OVER (PARTITION BY service_type ORDER BY YoY_Growth ASC) AS rn_worst
    FROM
        revenue_growth
)
SELECT
    service_type,
    MAX(CASE WHEN rn_best = 1 THEN year_quarter END) AS best_quarter,
    MAX(CASE WHEN rn_best = 1 THEN ROUND(YoY_Growth, 2) END) AS best_YoY_Growth,
    MAX(CASE WHEN rn_worst = 1 THEN year_quarter END) AS worst_quarter,
    MAX(CASE WHEN rn_worst = 1 THEN ROUND(YoY_Growth, 2) END) AS worst_YoY_Growth
FROM
    best_worst_quarters
GROUP BY
    service_type;

```
WITH quarterly_revenue AS (
    SELECT 
        year_quarter,
        year,
        quarter,
        service_type,
        SUM(revenue_quarterly_total_amount) AS agg_total_amount
    FROM 
        `de-zoomcamp2025-448100.trips_data_all.fct_taxi_trips_quarterly_revenue`
    GROUP BY 
        year_quarter, year, quarter, service_type
),
revenue_growth AS (
    SELECT 
        latest_year.year_quarter,
        latest_year.service_type,
        latest_year.quarter,
        latest_year.agg_total_amount AS revenue_2020, 
        previous_year.agg_total_amount AS revenue_2019, 
        ((latest_year.agg_total_amount - previous_year.agg_total_amount) / previous_year.agg_total_amount) * 100 AS YoY_Growth
    FROM 
        quarterly_revenue AS latest_year
    JOIN 
        quarterly_revenue AS previous_year
    ON 
        latest_year.quarter = previous_year.quarter
    AND 
        latest_year.service_type = previous_year.service_type
    AND 
        latest_year.year = 2020
    AND 
        previous_year.year = 2019
)
SELECT 
    year_quarter,
    service_type,
    ROUND(YoY_Growth, 2) AS YoY_Growth,
    CASE 
        WHEN YoY_Growth >= 0 THEN CONCAT('In ', year_quarter, ', ', service_type, ' Taxi had +', ROUND(YoY_Growth, 2), '% revenue growth compared to 2019/Q', quarter)
        ELSE CONCAT('In ', year_quarter, ', ', service_type, ' Taxi had ', ROUND(YoY_Growth, 2), '% revenue growth compared to 2019/Q', quarter)
    END AS growth_statement
FROM 
    revenue_growth
ORDER BY 
    year_quarter, service_type;
```





```