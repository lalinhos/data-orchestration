from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from workers import extract_worker, transform_worker, load_worker


with DAG(
    dag_id="pncp_pipeline",
    description="Coleta e processa contratações do PNCP: Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "pncp",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    params={
        "ano": Param(None, type=["null", "integer"], minimum=2020, maximum=2100),
        "mes": Param(None, type=["null", "integer"], minimum=1, maximum=12),
        "dia": Param(None, type=["null", "integer"], minimum=1, maximum=31),
    },
    tags=["pncp"],
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_bronze",
        python_callable=extract_worker.run,
    )

    t_transform = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_worker.run,
    )

    t_gold = PythonOperator(
        task_id="build_gold",
        python_callable=load_worker.run,
    )

    t_ingest >> t_transform >> t_gold