-- Staging model: Clean Qwen-extracted PDF budget data (v3 multi-column)
-- Filters out LLM hallucinations: invalid incisos, garbage values
-- credito_vigente/credito_ejecutado/inversion replace the old single monto column
-- Denominacion_inciso is normalized to canonical names in int_budget_unified

select
    cast(inciso as int64) as inciso,
    trim(denominacion_inciso) as denominacion_inciso,
    trim(categoria) as categoria,
    cast(credito_vigente as float64) as credito_vigente,
    cast(credito_ejecutado as float64) as credito_ejecutado,
    cast(inversion as float64) as inversion,
    cast(fiscal_year as int64) as fiscal_year,
    trim(pdf_source_file) as pdf_source_file,
    'pdf_extraction' as data_source,
    current_timestamp() as loaded_at
from {{ source('raw', 'raw_pdf_extractions') }}
where inciso is not null
  and fiscal_year is not null
  and denominacion_inciso is not null
  and cast(inciso as int64) between 1 and 36
  and cast(fiscal_year as int64) between 2000 and 2030
  -- At least one amount column must be positive
  and (
      coalesce(cast(credito_vigente as float64), 0) > 0
      or coalesce(cast(credito_ejecutado as float64), 0) > 0
      or coalesce(cast(inversion as float64), 0) > 0
  )
  -- Filter garbage denominacion_inciso: equipment descriptions used as agency names
  and length(trim(denominacion_inciso)) > 3
  and not regexp_contains(lower(trim(denominacion_inciso)),
      r'^(tic |hardware|software|notebook|pc |unidad |lockers|cajoneras|heladera|sill|computador|reloj |cambio de)'
  )
