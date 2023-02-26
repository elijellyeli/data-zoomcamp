{{ config(materialized="view") }}

-- row_number() over(partition by Pickup_datetime) as rn
with tripdata as (select * from {{ source("staging", "fhv_tripdata") }})

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["pulocationid", "Pickup_datetime"]) }} as tripid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(sr_flag as string) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number

from tripdata
-- where rn = 1
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
