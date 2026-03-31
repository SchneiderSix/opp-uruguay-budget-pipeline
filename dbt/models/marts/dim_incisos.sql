-- Mart: Dimension table for government agencies (incisos)
-- Prioritize names from credits sources over PDF extractions (LLM output is noisy)

with budget as (
    select distinct
        inciso,
        denominacion_inciso,
        data_source
    from {{ ref('int_budget_unified') }}
    where denominacion_inciso is not null
),

ranked as (
    select
        inciso,
        denominacion_inciso,
        row_number() over (
            partition by inciso
            order by
                case
                    when data_source like 'credits%' then 1
                    else 2
                end,
                denominacion_inciso
        ) as rn
    from budget
)

select
    inciso,
    denominacion_inciso
from ranked
where rn = 1
order by inciso
