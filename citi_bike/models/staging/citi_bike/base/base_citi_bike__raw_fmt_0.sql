with citi_bike_raw as (
    select
        *
    from {{ source('stg_citi_bike', 'raw_citi_bike_fmt_0') }}
),
final as (
    select
        NULL as rideable_type,
        "Start Time" AS started_at,
        "Stop Time" AS ended_at,
        "Start Station ID" AS start_station_id,
        "Start Station Name" AS start_station_name,
        "Start Station Latitude" AS start_lat,
        "Start Station Longitude" AS start_lng,
        "End Station ID" AS end_station_id,
        "End Station Name" AS end_station_name,
        "End Station Latitude" AS end_lat,
        "End Station Longitude" AS end_lng,
        CASE
            "User Type"
            WHEN 'Subscriber' then 'member'
            when 'Customer' THEN 'casual'
        END AS member_casual,
        "Birth Year" AS birth_year,
        CASE
            "Gender"
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