with citi_bike_stg as (
    select
        DATE(started_at) AS started_at_foreign_key,
        TIME(started_at) AS started_at_time,
        DATE(ended_at) AS ended_at_foreign_key,
        TIME(ended_at) AS ended_at_time,
        {{ dbt_utils.star(
            from=ref('stg_citi_bike__citi_bike')
        ) }}
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    seq.nextval AS ride_id,
    {{ dbt_utils.surrogate_key(['rideable_type']) }} rideable_type_key,
    {{ dbt_utils.surrogate_key(['started_at_foreign_key']) }} started_at_date_key,
    started_at_time,
    {{ dbt_utils.surrogate_key(['ended_at_foreign_key']) }} ended_at_date_key,
    ended_at_time,
    {{ dbt_utils.surrogate_key(['start_station_name']) }} start_station_key,
    start_lat,
    start_lng,
    {{ dbt_utils.surrogate_key(['end_station_name']) }} end_station_key,
    end_lat,
    end_lng,
    {{ dbt_utils.surrogate_key(['member_casual']) }} member_casual_key,
    birth_year,
    {{ dbt_utils.surrogate_key(['gender']) }} gender_key,
    DATEDIFF('min', started_at, ended_at) AS ride_time_min,
    HAVERSINE(start_lat, start_lng, end_lat, end_lng) * 0.621371 AS straight_line_distance_mi,
    IFNULL((HAVERSINE(start_lat, start_lng, end_lat, end_lng) * 0.621371) / NULLIF(DATEDIFF('min', started_at, ended_at), 0) * 60, 0) AS straight_line_avg_speed_mph
from
    citi_bike_stg stg,
    table(getnextval(seq)) seq