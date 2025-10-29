{% test station_capacity_valid(model, column_name, capacity_col, bikes_col, docks_col) %}

-- Test that capacity = bikes + docks (with tolerance for nulls)
select
    *
from {{ model }}
where {{ capacity_col }} is not null
  and {{ bikes_col }} is not null
  and {{ docks_col }} is not null
  and {{ capacity_col }} != ({{ bikes_col }} + {{ docks_col }})

{% endtest %}

-- Returns rows that dont have any nulls for capacity, bikes and docks AND where capacity != bikes + docks