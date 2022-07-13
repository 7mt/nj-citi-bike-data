with dim as (
    select
        distinct start_station_name, start_lat, start_lng
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    {{ dbt_utils.surrogate_key(['start_station_name']) }} start_station_key,
    *
from
    dim