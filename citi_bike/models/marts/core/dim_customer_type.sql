with dim as (
    select
        distinct member_casual
    from
        {{ ref('stg_citi_bike__citi_bike') }}
)
select
    {{ dbt_utils.surrogate_key(['member_casual']) }} member_casual_key,
    member_casual
from
    dim