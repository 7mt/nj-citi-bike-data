with citi_bike_raw as (
    select
        *
    from {{ source('stg_citi_bike', 'raw_citi_bike_fmt_2') }}
),
final as (
    select
        rideable_type,
        started_at,
        ended_at,
        start_station_id,
        start_station_name,
        start_lat,
        start_lng,
        end_station_id,
        end_station_name,
        end_lat,
        end_lng,
        member_casual,
        NULL AS birth_year,
        CAST(NULL AS STRING) AS gender
    from
        citi_bike_raw
)
select
    *
from
    final