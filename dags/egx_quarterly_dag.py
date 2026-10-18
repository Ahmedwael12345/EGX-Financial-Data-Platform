
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/Users/ahmedwael/Documents/EGX_Finance/egx-financial-data-platform"
PROJECT_PYTHON = f"{PROJECT_DIR}/.venv/bin/python"

with DAG(
    dag_id="egx_quarterly_financial_pipeline",
    schedule="0 6 1 1,4,7,10 *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Cairo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["EGX", "StockAnalysis"],
) as dag:

    scrape = BashOperator(
        task_id="scrape_stockanalysis",
        bash_command=f'cd "{PROJECT_DIR}" && "{PROJECT_PYTHON}" src/egx_scraper.py',
        execution_timeout=pendulum.duration(hours=2),
    )

    silver = BashOperator(
        task_id="transform_silver",
        bash_command=f'cd "{PROJECT_DIR}" && "{PROJECT_PYTHON}" src/silver_transform.py',
        execution_timeout=pendulum.duration(hours=2),
    )

    gold = BashOperator(
        task_id="load_gold_star_schema",
        bash_command=f'cd "{PROJECT_DIR}" && "{PROJECT_PYTHON}" src/gold_transform.py',
        execution_timeout=pendulum.duration(hours=2),
    )

    scrape >> silver >> gold

