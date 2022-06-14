with citi_bike_raw as (
    select
        *
    from {{ source('citi_bike', 'citi_bike_raw_fmt_1') }}
),
final as (
    select
        NULL as "rideable_type",
        "starttime" AS "started_at",
        "stoptime" AS "ended_at",
        "start station id" AS "start_station_id",
        "start station name" AS "start_station_name",
        "start station latitude" AS "start_lat",
        "start station longitude" AS "start_lng",
        "end station id" AS "end_station_id",
        "end station name" AS "end_station_name",
        "end station latitude" AS "end_lat",
        "end station longitude" AS "end_lng",
        CASE
            "usertype"
            WHEN 'Subscriber' then 'member'
            when 'Customer' THEN 'casual'
        END AS "member_casual",
        "birth year" AS "birth_year",
        CASE
            "gender"
            WHEN 1 THEN 'Male'
            WHEN 2 THEN 'Female'
        END AS "gender"
    from
        citi_bike_raw
)
select
    *
from
    final