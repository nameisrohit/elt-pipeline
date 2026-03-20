with staged as (
    select * from {{ ref('stg_housing') }}
),

summary as (
    select
        area,
        quarter_label,
        sum(dwelling_count) as total_completions,
        cast(left(quarter_label, 4) as INT64) as year
    from staged
    group by area, quarter_label
)

select * from summary
order by quarter_label desc, area
