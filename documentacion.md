## 📄 Documentación del trabajo realizado

### Capa Bronze (Ingesta y Limpieza)

Para la primera etapa del pipeline (Bronze $\rightarrow$ Silver), implementamos una lógica modular utilizando **Python** y **Pandas** dentro de Airflow.

#### Lógica de la Tarea en el DAG (`bronze_clean`)

* **Operador:** `PythonOperator`.
* **Función Wrapper:** Desarrollamos `run_bronze_clean(ds_nodash: str)` dentro del DAG para orquestar la llamada.
* **Transformación:** La lógica pesada de limpieza se desacopló en el módulo `include.transformations` (función `clean_daily_transactions`).
* **Manejo de Fechas:** Utilizamos la variable de template de Airflow `{{ ds_nodash }}` (ej: `20251206`) para identificar qué archivo crudo procesar.

#### Flujo de Datos

1.  **Lectura:** El DAG recibe la fecha de ejecución, la convierte a objeto `datetime` y busca el archivo correspondiente en `data/raw/`.
2.  **Procesamiento:** Se invoca a la función de limpieza externa.
3.  **Persistencia:** Se guarda el resultado en formato **Parquet** en `data/clean/transactions_<YYYYMMDD>_clean.parquet`.
4.  **Comunicación (XCom):** La tarea retorna la ruta absoluta del archivo Parquet generado. Esto se almacena automáticamente en **XCom** para que las tareas siguientes (Silver/Gold) puedan localizar el archivo sin necesidad de *hardcodear* rutas.

#### Snippet de Implementación

```python
# Definición del PythonOperator para la capa Bronze
bronze_clean_task = PythonOperator(
    task_id='bronze_clean',
    python_callable=run_bronze_clean,
    op_kwargs={
        'ds_nodash': '{{ ds_nodash }}'  # Inyección de fecha de ejecución
    }
)
```
#### Lógica de Limpieza (Capa Bronze)

La capa Bronze (`bronze_clean`) no solo convierte formatos, sino que aplica reglas de negocio estrictas para asegurar la calidad antes de que los datos entren al Data Warehouse.

Se implementa un script de Python con **Pandas** que realiza las siguientes transformaciones sobre el archivo Raw (`transactions_<ds>.csv`) antes de guardarlo como Parquet:

1.  **Normalización de Montos (`amount`):**
    * En el archivo de origen (datos crudos) no hay filas donde el formato del monto tenga decimales con coma (`,`) pero igualmente se normalizan a punto (`.`) antes de la conversión numérica, por si en el futuro los datos de entrada cambian de formato.
    * Se utiliza `pd.to_numeric(..., errors='coerce')` para manejar valores no numéricos.
    * **Regla de Negocio:** Decidimos que los valores vacíos o inválidos en `amount` no se descarten, sino que se imputen con **`0`** (`fillna(0)`). Al tener tan pocos datos (solo 6 transacciones en el ejemplo), creemos que descartar una fila es perder demasiada información.

2.  **Normalización de Texto (`status`):**
    * Se estandariza la columna `status` a **minúsculas** para evitar inconsistencias en las agregaciones de dbt (ej: evitar tener "PLACED" y "placed" como estados distintos).

3.  **Filtrado de Registros Inválidos:**
    * Se eliminan filas solo si faltan datos críticos de identidad o estado: `transaction_id`, `customer_id` o `status`.
    * *Nota:* No se eliminan filas por falta de `amount` ya que estas fueron tratadas en el paso 1 según indicamos en nuestra decisión.

**Resultado:** El archivo resultante en `data/clean/` es un Parquet con tipos de datos correctos (float para montos, string para ids), listo para ser ingerido por dbt sin necesidad de *casting* complejo.

Estas transformaciones tambien manejan los casos en donde no haya archivos para un día (dentro de la función clean_daily_transactions)


> **Nota:** 
> Si el archivo del día no existe o viene con un nombre inesperado, la tarea de limpieza lanza un `AirflowSkipException`, lo que marca la tarea como skipped. Dado que las tareas downstream utilizan el trigger rule por defecto (`all_success`), también quedan skipeadas. Esto evita procesamientos incompletos o inconsistentes y garantiza que el DAG solo avance cuando la ingesta diaria es válida.

> Desde la propia UI de Airflow es posible re-ejecutar el DAG para la fecha del archivo faltante, permitiendo cargarlo correctamente una vez que haya sido depositado. En un flujo productivo ,y dado que utilizamos una estrategia "upsert" para mantener el historial (como explicaremos mas adelante), es importante que no se procese la data de días posteriores hasta que el archivo faltante sea corregido, porque el pipeline asume una secuencia temporal completa y consistente.

> Alternativamente, el data owner podría generar un archivo de reparación que incluya todas las transacciones del día faltante, considerando estados actualizados o correcciones, evitando así la dependencia estricta del archivo original.

### Capas Silver y Gold (Modelado con dbt)

Una vez generado el archivo Parquet limpio en la capa Bronze, utilizamos **dbt Core** con el adaptador de DuckDB para modelar los datos, estructurando el proyecto en dos capas lógicas para cumplir con la arquitectura Medallion.

#### 1.1. Capa Silver (Staging): `stg_transactions`

El modelo `stg_transactions.sql` funciona para estandarizar técnicamente los datos provenientes del Parquet.

* **Lectura:** Utiliza la función `read_parquet` de DuckDB apuntando al archivo generado por la tarea anterior.
* **Casteo de Tipos:** Se aseguran los tipos de datos correctos para el análisis:
    * `transaction_id` y `customer_id` se castean explícitamente a **TEXT/STRING**.
    * `amount` se castea a **DOUBLE**.
    * Los campos de fecha se convierten a tipos `DATE` o `TIMESTAMP` según corresponda.
* **Normalización:** Se asegura que el campo `status` esté uniformemente en minúsculas (refuerzo de la lógica aplicada en Bronze).
* **Filtrado de Calidad:** Se descartan filas que aún posean valores nulos en campos críticos (IDs) que no hayan podido ser resueltos en la capa anterior.

#### 1.2 Capa Silver (Historical): `hist_transactions`

El modelo `hist_transactions.sql` cumple la función de almacenar y mantener el historial completo de transacciones, a diferencia del modelo stg_transactions, que solo representa una vista del archivo Parquet correspondiente al día procesado.

Sus características principales son:

* **Fuente:** Consume directamente la vista stg_transactions, que provee los datos ya estandarizados para la fecha específica procesada por el pipeline.

* **Logica Upsert:** El modelo está configurado como incremental con estrategia merge, utilizando transaction_id como unique_key. Esto permite:

    - Insertar nuevas transacciones que no existían previamente.

    - Actualizar transacciones existentes cuyo contenido haya cambiado en días posteriores.

* **Motivación del Enfoque:** Una alternativa habría sido leer todos los Parquet históricos cada vez que se construye la tabla FCT (por ejemplo, todo un rango de fechas). Sin embargo, este enfoque no escala bien: obliga a releer archivos constantemente y una misma transaction_id puede aparecer en varios días, lo que requiere revisar múltiples Parquets para encontrar su versión más reciente. Esto aumenta mucho el costo de operacion y el tiempo de procesamiento. El enfoque incremental con merge evita este problema al procesar solo las novedades del día y mantener la tabla histórica actualizada de manera eficiente

#### 2. Capa Gold (Marts): `fct_customer_transactions`

El modelo `fct_customer_transactions.sql` consume los datos ya estandarizados y almacenados en `hist_transactions` para generar una tabla de hechos historicos agregada por cliente, lista para consumo analítico.

* **Granularidad:** Una fila por `customer_id`.
* **Métricas Calculadas:**
    * `transaction_count`: Cantidad total de transacciones por cliente.
    * `total_amount_all`: Suma total del monto de todas las transacciones.
    * `total_amount_completed`: Suma condicional del monto donde `status = 'completed'`, para diferenciar el volumen de ventas real/confirmado.

#### Ejecución en el DAG (`silver_dbt_run`)

Esta etapa se ejecuta mediante un `BashOperator` que corre `dbt run`. Esto materializa ambas tablas (Silver y Gold) en el archivo `warehouse/medallion.duckdb` de manera secuencial, respetando las dependencias definidas por las referencias `{{ ref() }}` de dbt.

### Estrategia de Testing (Capa Gold)

Para garantizar la calidad de los datos en la capa final (Gold), implementamos una estrategia de validación en dbt que combina tests estructurales y reglas de negocio personalizadas.

1. Tests de Esquema (schema.yml)
En el modelo fct_customer_transactions, definimos restricciones estrictas sobre la clave primaria para asegurar la integridad de la agregación.

* Test unique en customer_id: Dado que esta tabla presenta métricas agregadas por cliente, es fundamental garantizar que no existan filas duplicadas para un mismo customer_id.

* Test not_null: (Ya existente) Asegura que no haya identificadores de cliente nulos.

2. Test de Integridad de Negocio (Custom SQL)
Implementamos un test de datos personalizado (tests/assert_amounts_logic.sql) para validar la coherencia de los montos calculados.

Regla: El monto total de transacciones completadas (total_amount_completed) nunca puede ser mayor al monto total absoluto (total_amount_all).

Implementación: El test busca "registros fallidos", es decir, aquellos donde completed > all. Si la consulta devuelve filas, dbt alerta el error.

Snippet del Test (assert_amounts_logic.sql):

```SQL
-- Este test falla si encuentra clientes donde el monto completado supera al total
select
    customer_id,
    total_amount_completed,
    total_amount_all
from {{ ref('fct_customer_transactions') }}
where total_amount_completed > total_amount_all
```
### Ideas de Escalabilidad y Modelado

Como mejora a futuro para un entorno productivo de alto volumen, proponemos las siguientes evoluciones a la arquitectura actual:

* **Orquestación escalable:** Migrar de `airflow standalone` a una arquitectura distribuida con una base de datos más robusta. Esto permitiría soportar un mayor volumen de DAGs concurrentes y paralelizar corridas históricas (*backfills*) sin bloquear el *scheduler*.

* **Bronze particionado:** Persistir los archivos limpios en un formato columnar optimizado como **Parquet**, particionado físicamente por fecha (`/year=2025/month=12/day=06/`) o cliente. Esto, combinado con almacenamiento en la nube y un catálogo, habilitaría lecturas mucho más rápidas y eficientes.

* **Gold consolidado:** Enriquecer la capa de presentación expandiendo las métricas agregadas por cliente.
    * **Desglose por estado:** Calcular totales diferenciados para cada estado (`completed`, `pending`, `failed`) para permitir análisis financiero y de conversión más precisos.
    * **Métricas de ciclo de vida:** Incluir `first_transaction_date` y `last_transaction_date` para facilitar análisis.


* **Observabilidad y Alerting:** Evolucionar el manejo de Data Quality. En lugar de solo guardar un JSON local, enviar los resultados de los tests a un tópico de eventos o a un *bucket* versionado. Esto permitiría auditar tendencias de calidad a lo largo del tiempo y disparar alertas automáticas (via Slack, por ejemplo) ante anomalías en los datos.

### Modo de trabajo y colaboración

Durante el desarrollo del examen, al estar sentados al lado, Ceci y Ari trabajaron de manera colaborativa, discutiendo y acordando cada decisión de implementación. El flujo de trabajo no se basó en commits intermedios ni en un uso intensivo de Git, sino en avanzar iterativamente, probando las implementaciones paso a paso en las distintas herramientas (Airflow, dbt, etc.).

Ceci fue quien realizó los pushes al repositorio con todas las implementaciones desarrolladas en conjunto, mientras que Ari realizó el push del archivo de documentación que fuimos elaborando a medida que avanzábamos en las diferentes etapas.

Ambas desarrollaron el modelo inicial. Luego, durante la semana, contamos con el aporte de Fran, quien complementó el trabajo incorporando la lógica de manejo de datos históricos en la capa Silver, junto con otros ajustes menores. Esta mejora permitió construir una Golden Layer mucho más rica, completa y útil para la toma de decisiones.