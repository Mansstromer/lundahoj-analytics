select *
from {{ ref('stg_station_readings') }}
