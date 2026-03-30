-- Staging model: Unify budget credits 2011-2021 into consistent schema
-- Column AÑO stored as A_O in BigQuery due to encoding
-- 2011-2019: MONTO columns are INT64; 2020+resumen: MONTO columns are STRING

with credits_int as (
    select
        A_O as fiscal_year,
        cast(ORG_ID as int64) as org_id,
        trim(ORG_NOMBRE) as org_nombre,
        cast(UE_ID as int64) as ue_id,
        trim(UE_NOMBRE) as ue_nombre,
        trim(AP_NOMBRE) as area_programatica,
        trim(PROGRAMA_NOMBRE) as programa,
        trim(TIPO_GASTO_NOMBRE) as tipo_gasto,
        cast(MONTO_APROBADO as float64) as monto_aprobado,
        cast(MONTO_VIGENTE as float64) as monto_vigente,
        cast(MONTO_EJECUTADO as float64) as monto_ejecutado,
        'credits_2011_2019' as data_source
    from {{ source('raw', 'raw_budget_credits_int') }}
    where ORG_ID is not null
      and A_O is not null
),

credits_str as (
    select
        A_O as fiscal_year,
        cast(ORG_ID as int64) as org_id,
        trim(ORG_NOMBRE) as org_nombre,
        cast(UE_ID as int64) as ue_id,
        trim(UE_NOMBRE) as ue_nombre,
        trim(AP_NOMBRE) as area_programatica,
        trim(PROGRAMA_NOMBRE) as programa,
        trim(TIPO_GASTO_NOMBRE) as tipo_gasto,
        safe_cast(MONTO_APROBADO as float64) as monto_aprobado,
        safe_cast(MONTO_VIGENTE as float64) as monto_vigente,
        safe_cast(MONTO_EJECUTADO as float64) as monto_ejecutado,
        'credits_2020_resumen' as data_source
    from {{ source('raw', 'raw_budget_credits_str') }}
    where ORG_ID is not null
      and A_O is not null
),

credits_2021 as (
    select
        a_o as fiscal_year,
        cast(organismo_codigo as int64) as org_id,
        trim(organismo_nombre) as org_nombre,
        cast(ue_codigo as int64) as ue_id,
        trim(ue_nombre) as ue_nombre,
        trim(ap_nombre) as area_programatica,
        trim(programa_nombre) as programa,
        trim(tipo_gasto_nombre) as tipo_gasto,
        safe_cast(credito as float64) as monto_aprobado,
        safe_cast(credito as float64) as monto_vigente,
        safe_cast(ejecutado as float64) as monto_ejecutado,
        'credits_2021' as data_source
    from {{ source('raw', 'raw_budget_credits_2021') }}
    where organismo_codigo is not null
      and a_o is not null
)

select *, current_timestamp() as loaded_at from credits_int
union all
select *, current_timestamp() as loaded_at from credits_str
union all
select *, current_timestamp() as loaded_at from credits_2021
