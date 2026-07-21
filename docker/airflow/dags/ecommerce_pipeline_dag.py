from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# These imports rely on PYTHONPATH=/opt/airflow/project being set
from src.ingestion.bronze import run_bronze_ingestion
from src.transformations.silver import run_silver_transformations
from src.transformations.gold_dims import run_gold_dimensions
from src.transformations.gold_facts import run_gold_facts
from src.transformations.gold_views import run_gold_views_transformations

default_args = {
    "owner": "vinay",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_pipeline_dag",
    description="End-to-end medallion pipeline: Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="0 2 * * *",           # daily at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "medallion"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
    )

    silver = PythonOperator(
        task_id="silver_transforms",
        python_callable=run_silver_transformations,
    )

    gold_dims = PythonOperator(
        task_id="gold_dimensions",
        python_callable=run_gold_dimensions,
    )

    gold_facts = PythonOperator(
        task_id="gold_facts",
        python_callable=run_gold_facts,
    )

    gold_views = PythonOperator(
        task_id="gold_views",
        python_callable=run_gold_views_transformations,
    )

    bronze >> silver >> gold_dims >> gold_facts >> gold_views