-- Intermediate model: Union budget credits + PDF extractions into consistent schema
-- PDF denominacion_inciso is normalized using canonical names from credits sources

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

-- Canonical inciso names from credits (most reliable source)
canonical_names as (
    select distinct
        org_id as inciso,
        first_value(org_nombre) over (
            partition by org_id
            order by org_nombre
        ) as canonical_name
    from {{ ref('stg_budget_credits') }}
    where org_nombre is not null
      and length(trim(org_nombre)) > 3
),

pdf as (
    select
        p.fiscal_year,
        p.inciso,
        -- Replace noisy LLM names with canonical credits name when available
        coalesce(cn.canonical_name, p.denominacion_inciso) as denominacion_inciso,
        p.categoria,
        p.monto as credito_vigente,
        cast(null as float64) as ejecucion,
        p.data_source
    from {{ ref('stg_pdf_extractions') }} p
    left join canonical_names cn on p.inciso = cn.inciso
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
