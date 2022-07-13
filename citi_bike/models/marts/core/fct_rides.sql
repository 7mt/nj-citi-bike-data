with citi_bike_stg as (
    select
        *
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
 seq.nextval AS ride_id
, stg.*
, DATEDIFF('min', started_at, ended_at) AS ride_time_min
, HAVERSINE(start_lat, start_lng, end_lat, end_lng) * 0.621371 AS straight_line_distance_mi
, IFNULL((HAVERSINE(start_lat, start_lng, end_lat, end_lng) * 0.621371) / NULLIF(DATEDIFF('min', started_at, ended_at), 0) * 60, 0) AS straight_line_avg_speed_mph
from
    citi_bike_stg stg,
    table(getnextval(seq)) seq