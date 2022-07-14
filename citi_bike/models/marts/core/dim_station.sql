with dim as (
    select
        distinct start_station_name AS station_name
    from
        {{ ref('stg_citi_bike__citi_bike') }}
    union
     select
        distinct end_station_name AS station_name
    from
        {{ ref('stg_citi_bike__citi_bike') }}


)
select
    {{ dbt_utils.surrogate_key(['station_name']) }} station_key,
    *
from
    dim