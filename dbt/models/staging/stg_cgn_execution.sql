-- Staging model: CGN SIIF budget execution reference data
-- Official per-inciso execution data from Contaduria General de la Nacion
-- Used as authoritative validation source and to fill coverage gaps

select
    cast(fiscal_year as int64) as fiscal_year,
    cast(inciso as int64) as inciso,
    trim(denominacion_inciso) as denominacion_inciso,
    cast(credito_vigente as float64) as credito_vigente,
    cast(obligado_ejecutado as float64) as obligado_ejecutado,
    'cgn_siif' as data_source,
    current_timestamp() as loaded_at
from {{ source('raw', 'raw_cgn_execution') }}
where inciso is not null
  and fiscal_year is not null
  and cast(inciso as int64) between 1 and 36
  and cast(fiscal_year as int64) between 2000 and 2030
