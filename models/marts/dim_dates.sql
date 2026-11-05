{{ config(distributed_replicated=true) }}

with date_series as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-03-01' as date)",
        end_date="cast('2026-07-12' as date)"
    ) }}
)

select
    date_day::date as date_day,
    extract(year from date_day)::int as calendar_year,
    extract(quarter from date_day)::int as calendar_quarter,
    extract(month from date_day)::int as calendar_month,
    to_char(date_day, 'YYYY-MM') as year_month,
    extract(isodow from date_day)::int as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from date_series