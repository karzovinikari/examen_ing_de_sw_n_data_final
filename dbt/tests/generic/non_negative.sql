{% test non_negative(model, column_name) %}

-- TODO: Implementar el test para verificar que los valores en la columna son no negativos y no nulos.

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0 or {{ column_name }} is null

{% endtest %}
