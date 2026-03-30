-- Staging model: Clean Qwen-extracted PDF budget data

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
