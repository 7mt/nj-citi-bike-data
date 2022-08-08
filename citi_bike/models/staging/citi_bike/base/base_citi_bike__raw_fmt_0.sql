with citi_bike_raw as (
    select
        *
    from {{ source('stg_citi_bike', 'raw_citi_bike_fmt_0') }}
),
final as (
    select
        CAST(NULL AS STRING) as rideable_type,
        start_time AS started_at,
        stop_time AS ended_at,
        CAST(start_station_id AS STRING) AS start_station_id,
        start_station_name,
        start_station_latitude AS start_lat,
        start_station_longitude AS start_lng,
        CAST(end_station_id AS STRING) AS end_station_id,
        end_station_name,
        end_station_latitude AS end_lat,
        end_station_longitude AS end_lng,
        CASE
            user_type
            WHEN 'Subscriber' then 'member'
            when 'Customer' THEN 'casual'
        END AS member_casual,
        birth_year,
        CASE
            gender
            WHEN 1 THEN 'Male'
            WHEN 2 THEN 'Female'
        END AS gender
    from
        citi_bike_raw
)
select
    *
from
    final