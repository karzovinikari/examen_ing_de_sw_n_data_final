# Medallion Architecture Demo (Airflow + dbt + DuckDB)

Este proyecto crea un pipeline de 3 pasos que replica la arquitectura medallion:

1. **Bronze**: Airflow lee un CSV crudo según la fecha de ejecución y aplica una limpieza básica con Pandas guardando un archivo parquet limpio.
2. **Silver**: Un `dbt run` carga el parquet en DuckDB y genera modelos intermedios.
3. **Gold**: `dbt test` valida la tabla final y escribe un archivo con el resultado de los data quality checks.

## Estructura

```
├── dags/
│   └── medallion_medallion_dag.py
├── data/
│   ├── raw/
│   │   └── transactions_20251205.csv
│   ├── clean/
│   └── quality/
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
├── include/
│   └── transformations.py
├── profiles/
│   └── profiles.yml
├── warehouse/
│   └── medallion.duckdb (se genera en tiempo de ejecución)
└── requirements.txt
```

## Requisitos

- Python 3.10+
- DuckDB CLI opcional para inspeccionar la base.

Instala dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuración de variables de entorno

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export DBT_PROFILES_DIR=$(pwd)/profiles
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

## Inicializar Airflow

```bash
airflow standalone
```

En el output de `airflow standalone` buscar la contraseña para loguearse. Ej:
```
standalone | Airflow is ready
standalone | Login with username: admin  password: pPr9XXxFzgrgGd6U
```


## Ejecutar el DAG

1. Coloca/actualiza el archivo `data/raw/transactions_YYYYMMDD.csv`.


3. Desde la UI o CLI dispara el DAG usando la fecha deseada:

```bash
airflow dags trigger medallion_pipeline --run-id manual_$(date +%s)
```

El DAG ejecutará:

- `bronze_clean`: llama a `clean_daily_transactions` para crear `data/clean/transactions_<ds>_clean.parquet`.
- `silver_dbt_run`: ejecuta `dbt run` apuntando al proyecto `dbt/` y carga la data en DuckDB.
- `gold_dbt_tests`: corre `dbt test` y escribe `data/quality/dq_results_<ds>.json` con el status (`passed`/`failed`).

Si un test falla, el archivo igual se genera y el task termina en error para facilitar el monitoreo.

## Ejecutar dbt manualmente

```bash
cd dbt
dbt run
DBT_PROFILES_DIR=../profiles dbt test
```

Asegúrate de exportar `CLEAN_DIR`, `DS_NODASH` y `DUCKDB_PATH` si necesitas sobreescribir valores por defecto:

```bash
export CLEAN_DIR=$(pwd)/../data/clean
export DS_NODASH=20251205
export DUCKDB_PATH=$(pwd)/../warehouse/medallion.duckdb
```

## Observabilidad de Data Quality

Cada corrida crea `data/quality/dq_results_<ds>.json` similar a:

```json
{
  "ds_nodash": "20251205",
  "status": "passed",
  "stdout": "...",
  "stderr": ""
}
```

Ese archivo puede ser ingerido por otras herramientas para auditoría o alertas.


## Verificación de resultados por capa

### Bronze
1. Revisa que exista el parquet más reciente:
    ```bash
    $ find data/clean/ | grep transactions_*
    data/clean/transactions_20251201_clean.parquet
    ```
2. Inspecciona las primeras filas para confirmar la limpieza aplicada:
    ```bash
    duckdb -c "
      SELECT *
      FROM read_parquet('data/clean/transactions_20251201_clean.parquet')
      LIMIT 5;
    "
    ```

### Silver
1. Abre el warehouse y lista las tablas creadas por dbt:
    ```bash
    duckdb warehouse/medallion.duckdb -c ".tables"
    ```
2. Ejecuta consultas puntuales para validar cálculos intermedios:
    ```bash
    duckdb warehouse/medallion.duckdb -c "
      SELECT *
      FROM fct_customer_transactions
      LIMIT 10;
    "
    ```

### Gold
1. Revisa que exista el parquet más reciente:
    ```bash
    $ find data/quality/*.json
    data/quality/dq_results_20251201.json
    ```

2. Confirma la generación del archivo de data quality:
    ```bash
    cat data/quality/dq_results_20251201.json | jq
    ```

3. En caso de fallos, inspecciona `stderr` dentro del mismo JSON o revisa los logs del task en la UI/CLI de Airflow para identificar la prueba que reportó error.


## Formato y linting

Usa las herramientas incluidas en `requirements.txt` para mantener un estilo consistente y detectar problemas antes de ejecutar el DAG.

### Black (formateo)

Aplica Black sobre los módulos de Python del proyecto. Añade rutas extra si incorporas nuevos paquetes.

```bash
black dags include
```

### isort (orden de imports)

Ordena automáticamente los imports para evitar diffs innecesarios y mantener un estilo coherente.

```bash
isort dags include
```

### Pylint (estático)

Ejecuta Pylint sobre las mismas carpetas para detectar errores comunes y mejorar la calidad del código.

```bash
pylint dags/*.py include/*.py
```

Para ejecutar ambos comandos de una vez puedes usar:

```bash
isort dags include && black dags include && pylint dags/*.py include/*.py
```

## TODOs
Necesarios para completar el workflow:
- [ ] Implementar tareas de Airflow.
- [ ] Implementar modelos de dbt según cada archivo schema.yml.
- [ ] Implementar pruebas de dbt para asegurar que las tablas gold estén correctas.
- [ ] Documentar mejoras posibles para el proceso considerado aspectos de escalabilidad y modelado de datos.
Nice to hace:
- [ ] Manejar el caso que no haya archivos para el dia indicado.
