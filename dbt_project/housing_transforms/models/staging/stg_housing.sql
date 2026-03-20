with source as (
    select * from {{ source('housing_raw', 'dwelling_completions') }}
),

cleaned as (
    select
        STATISTIC as statistic_label,
        C03349V04063 as area,
        TLIST_Q1 as quarter_label,
        cast(value as FLOAT64) as dwelling_count,
        loaded_at
    from source
    where value is not null
)

select * from cleaned
