with citi_bike_base as (
    select
        *
    from
        {{ ref('base_citi_bike__raw_fmt_0') }}
    UNION ALL
    select
        *
    from
        {{ ref('base_citi_bike__raw_fmt_1') }}
    UNION ALL
    select
        *
    from
        {{ ref('base_citi_bike__raw_fmt_2') }}
)
select
    *
from
    citi_bike_base