-- Intermediate model: Enrich unified budget data with derived metrics

with budget as (
    select * from {{ ref('int_budget_unified') }}
),

with_lag as (
    select
        *,
        lag(credito_vigente) over (
            partition by inciso, categoria
            order by fiscal_year
        ) as prev_year_credito,
        lag(ejecucion) over (
            partition by inciso, categoria
            order by fiscal_year
        ) as prev_year_ejecucion
    from budget
),

enriched as (
    select
        *,
        case
            when prev_year_credito is not null and prev_year_credito > 0
            then round((credito_vigente - prev_year_credito) / prev_year_credito * 100, 2)
            else 0
        end as credito_yoy_pct_change,
        case
            when credito_vigente is not null and credito_vigente > 0
            then round(ejecucion / credito_vigente * 100, 2)
            else 0
        end as execution_rate_pct
    from with_lag
)

select * from enriched
