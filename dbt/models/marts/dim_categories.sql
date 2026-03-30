-- Mart: Dimension table for spending categories

with budget as (
    select distinct
        categoria
    from {{ ref('int_budget_unified') }}
    where categoria is not null
)

select
    categoria,
    row_number() over (order by categoria) as category_id
from budget
order by categoria
