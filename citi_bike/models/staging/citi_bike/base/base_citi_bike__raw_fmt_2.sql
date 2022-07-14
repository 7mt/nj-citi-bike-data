with citi_bike_raw as (
    select
        *
    from {{ source('stg_citi_bike', 'raw_citi_bike_fmt_2') }}
),
final as (
    select
        "rideable_type" AS rideable_type,
        "started_at" AS started_at,
        "ended_at" AS ended_at,
        "start_station_id" AS start_station_id,
        "start_station_name" AS start_station_name,
        "start_lat" AS start_lat,
        "start_lng" AS start_lng,
        "end_station_id" AS end_station_id,
        "end_station_name" AS end_station_name,
        "end_lat" AS end_lat,
        "end_lng" AS end_lng,
        "member_casual" AS member_casual,
        NULL AS birth_year,
        NULL AS gender
    from
        citi_bike_raw
)
select
    *
from
    final