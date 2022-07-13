with dim as (
    select
        distinct end_station_name, end_lat, end_lng
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    {{ dbt_utils.surrogate_key(['end_station_name']) }} end_station_key,
    *
from
    dim