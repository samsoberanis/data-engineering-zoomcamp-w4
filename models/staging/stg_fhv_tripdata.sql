{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatchingbasenum,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("string")) }} as srflag,
    {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("string")) }} as affiliatedbase_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

from tripdata
where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
-- {% if var('is_test_run', default=true) %}

--    limit 100

-- {% endif %}