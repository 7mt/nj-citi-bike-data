select
    {{ dbt_utils.surrogate_key(['date_day']) }} date_key,
    *
from {{ ref('get_date_dimension') }}