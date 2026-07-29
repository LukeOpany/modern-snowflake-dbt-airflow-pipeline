from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_EXECUTABLE = PROJECT_ROOT / ".venv/bin/python"
INGESTION_SCRIPT = PROJECT_ROOT / "ingestion/load_to_snowflake.py"
DBT_EXECUTABLE = PROJECT_ROOT / ".venv/bin/dbt"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt/modern_snowflake_pipeline"
DBT_PROFILES_DIR = Path.home() / ".dbt"

def dbt_command(command: str) -> str:
    return (
        f"{DBT_EXECUTABLE} {command} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    )

with DAG(
    dag_id="snowflake_dbt_pipeline",
    description="Ingest, transform, and test the Snowflake dbt pipeline",
    start_date=pendulum.datetime(2026, 7, 12, tz="Africa/Nairobi"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "dbt", "ingestion"],
) as dag:

    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"{PYTHON_EXECUTABLE} {INGESTION_SCRIPT}",
    )

    validate_connection = BashOperator(
        task_id="validate_dbt_connection",
        bash_command=dbt_command("debug"),
    )

    run_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=dbt_command("run"),
    )

    test_models = BashOperator(
        task_id="test_dbt_models",
        bash_command=dbt_command("test"),
    )

    ingest_raw_data >> validate_connection >> run_models >> test_models
