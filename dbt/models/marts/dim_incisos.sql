-- Mart: Dimension table for government agencies (incisos)

with budget as (
    select distinct
        inciso,
        denominacion_inciso
    from {{ ref('int_budget_unified') }}
    where denominacion_inciso is not null
),

-- Deduplicate: pick the most common denomination per inciso
ranked as (
    select
        inciso,
        denominacion_inciso,
        row_number() over (
            partition by inciso
            order by denominacion_inciso
        ) as rn
    from budget
)

select
    inciso,
    denominacion_inciso
from ranked
where rn = 1
order by inciso
