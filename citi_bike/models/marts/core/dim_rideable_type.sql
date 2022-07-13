with dim as (
    select
        distinct rideable_type
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    {{ dbt_utils.surrogate_key(['rideable_type']) }} rideable_type_key,
    rideable_type
from
    dim