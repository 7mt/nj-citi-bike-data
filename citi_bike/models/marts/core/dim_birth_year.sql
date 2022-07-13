with dim as (
    select
        distinct birth_year
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    {{ dbt_utils.surrogate_key(['birth_year']) }} birth_year_key,
    birth_year
from
    dim