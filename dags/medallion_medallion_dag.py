"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (
    clean_daily_transactions,
)  # pylint: disable=wrong-import-position

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Build environment variables needed by dbt commands."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """Execute a dbt command and return the completed process."""
    env = _build_env(ds_nodash)
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def run_bronze_clean(ds_nodash: str) -> str:
    """Genera el parquet limpio para la fecha indicada."""

    execution_date = datetime.strptime(ds_nodash, "%Y%m%d").date()
    try:
        output_path = clean_daily_transactions(
            execution_date=execution_date,
            raw_dir=RAW_DIR,
            clean_dir=CLEAN_DIR,
        )
    except FileNotFoundError as exc:
        raise AirflowSkipException(str(exc)) from exc

    return str(output_path)


def run_silver_models(ds_nodash: str) -> str:
    """Ejecuta `dbt run` para materializar modelos staging/marts."""

    result = _run_dbt_command("run", ds_nodash)
    if result.returncode != 0:
        raise AirflowException(
            f"dbt run failed for {ds_nodash}:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    return result.stdout


def run_gold_tests(ds_nodash: str) -> str:
    """Corre `dbt test`, persiste el reporte y falla si hubo errores."""

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    result = _run_dbt_command("test", ds_nodash)

    report = {
        "ds_nodash": ds_nodash,
        "status": "passed" if result.returncode == 0 else "failed",
        "stdout": result.stdout,
        "stderr": result.stderr,
    }

    report_path = QUALITY_DIR / f"dq_results_{ds_nodash}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if result.returncode != 0:
        raise AirflowException(
            f"dbt test failed for {ds_nodash}: see {report_path} for details."
        )

    return str(report_path)


def build_dag() -> DAG:
    """Construct the medallion pipeline DAG with bronze/silver/gold tasks."""
    with DAG(
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
    ) as medallion_dag:

        bronze_clean = PythonOperator(
            task_id="bronze_clean",
            python_callable=run_bronze_clean,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        silver_dbt_run = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=run_silver_models,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        gold_dbt_tests = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=run_gold_tests,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        bronze_clean >> silver_dbt_run >> gold_dbt_tests

    return medallion_dag


dag = build_dag()
