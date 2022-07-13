-- depends_on: {{ ref('stg_citi_bike__citi_bike') }}

{%- call statement('date_range_query', fetch_result=True) -%}
    select
        min(date(started_at)) min_date,
        max(date(ended_at)) max_date
    from {{ ref('stg_citi_bike__citi_bike') }}
{%- endcall -%}

{%- set start_date = load_result('date_range_query')['data'][0][0] -%}
{%- set end_date = load_result('date_range_query')['data'][0][1] -%}

{{ dbt_date.get_date_dimension(start_date, end_date) }}