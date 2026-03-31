-- Staging model: Clean Qwen-extracted PDF budget data
-- Filters out LLM hallucinations: invalid incisos, garbage values

select
    cast(inciso as int64) as inciso,
    trim(denominacion_inciso) as denominacion_inciso,
    trim(categoria) as categoria,
    cast(monto as float64) as monto,
    cast(fiscal_year as int64) as fiscal_year,
    trim(pdf_source_file) as pdf_source_file,
    'pdf_extraction' as data_source,
    current_timestamp() as loaded_at
from {{ source('raw', 'raw_pdf_extractions') }}
where inciso is not null
  and monto is not null
  and fiscal_year is not null
  and cast(inciso as int64) between 1 and 36
  and cast(fiscal_year as int64) between 2000 and 2030
  and cast(monto as float64) > 0
