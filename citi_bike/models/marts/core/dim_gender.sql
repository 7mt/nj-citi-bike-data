with dim as (
    select
        distinct gender
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    {{ dbt_utils.surrogate_key(['gender']) }} gender_key,
    gender
from
    dim