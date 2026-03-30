-- Mart: Fact table for budget allocations and execution by inciso and fiscal year
-- Partitioned by fiscal_year, clustered by inciso and categoria

{{
    config(
        materialized='table',
        partition_by={
            "field": "fiscal_year",
            "data_type": "int64",
            "range": {
                "start": 2005,
                "end": 2030,
                "interval": 1
            }
        },
        cluster_by=["inciso", "categoria"]
    )
}}

with enriched as (
    select * from {{ ref('int_budget_enriched') }}
),

aggregated as (
    select
        fiscal_year,
        inciso,
        denominacion_inciso,
        categoria,
        sum(credito_vigente) as total_credito_vigente,
        sum(ejecucion) as total_ejecucion,
        avg(execution_rate_pct) as avg_execution_rate_pct,
        avg(credito_yoy_pct_change) as avg_credito_yoy_pct_change,
        count(*) as record_count,
        count(distinct data_source) as source_count
    from enriched
    group by 1, 2, 3, 4
)

select
    {{ dbt_utils.generate_surrogate_key(['fiscal_year', 'inciso', 'denominacion_inciso', 'categoria']) }} as execution_id,
    *
from aggregated
