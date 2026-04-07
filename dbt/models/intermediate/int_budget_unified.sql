-- Intermediate model: Union budget data from best available source per year
-- Source priority per year (avoids double-counting):
--   2011-2019: credits_2020_resumen (~1.8K rows/yr, consistent growth, has ejecutado)
--   2020, 2023: CGN SIIF (authoritative government totals, no category detail)
--   2021: credits_2021
--   2005-2010, 2022, 2024: PDF extractions (only available source)
-- Note: credits_2011_2019 excluded (incomplete for 2013-2018)
-- Note: credits_2020_resumen for 2020 excluded (34K rows, 2x inflated vs CGN)

with credits_resumen as (
    select
        fiscal_year,
        org_id as inciso,
        org_nombre as denominacion_inciso,
        tipo_gasto as categoria,
        monto_vigente as credito_vigente,
        coalesce(monto_ejecutado, 0) as ejecucion,
        cast(0 as float64) as inversion,
        data_source
    from {{ ref('stg_budget_credits') }}
    where data_source = 'credits_2020_resumen'
      and fiscal_year between 2011 and 2019
),

credits_2021 as (
    select
        fiscal_year,
        org_id as inciso,
        org_nombre as denominacion_inciso,
        tipo_gasto as categoria,
        monto_vigente as credito_vigente,
        coalesce(monto_ejecutado, 0) as ejecucion,
        cast(0 as float64) as inversion,
        data_source
    from {{ ref('stg_budget_credits') }}
    where data_source = 'credits_2021'
),

cgn as (
    select
        fiscal_year,
        inciso,
        denominacion_inciso,
        cast(null as string) as categoria,
        credito_vigente,
        obligado_ejecutado as ejecucion,
        cast(0 as float64) as inversion,
        data_source
    from {{ ref('stg_cgn_execution') }}
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
        coalesce(cn.canonical_name, p.denominacion_inciso) as denominacion_inciso,
        p.categoria,
        coalesce(p.credito_vigente, 0) as credito_vigente,
        coalesce(p.credito_ejecutado, 0) as ejecucion,
        coalesce(p.inversion, 0) as inversion,
        p.data_source
    from {{ ref('stg_pdf_extractions') }} p
    left join canonical_names cn on p.inciso = cn.inciso
    -- Only years not covered by credits or CGN
    where p.fiscal_year < 2011
       or p.fiscal_year in (2022, 2024)
),

unioned as (
    select * from credits_resumen
    union all
    select * from credits_2021
    union all
    select * from cgn
    union all
    select * from pdf
)

select
    {{ dbt_utils.generate_surrogate_key(['inciso', 'fiscal_year', 'categoria', 'data_source']) }} as budget_id,
    *
from unioned
