-- Intermediate model: Union budget credits + PDF extractions into consistent schema

with credits as (
    select
        fiscal_year,
        org_id as inciso,
        org_nombre as denominacion_inciso,
        tipo_gasto as categoria,
        monto_vigente as credito_vigente,
        monto_ejecutado as ejecucion,
        data_source
    from {{ ref('stg_budget_credits') }}
),

pdf as (
    select
        fiscal_year,
        inciso,
        denominacion_inciso,
        categoria,
        monto as credito_vigente,
        cast(null as float64) as ejecucion,
        data_source
    from {{ ref('stg_pdf_extractions') }}
),

unioned as (
    select * from credits
    union all
    select * from pdf
)

select
    {{ dbt_utils.generate_surrogate_key(['inciso', 'fiscal_year', 'categoria', 'data_source']) }} as budget_id,
    *
from unioned
