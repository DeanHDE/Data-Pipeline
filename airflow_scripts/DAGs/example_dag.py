from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

script_paths = [
    "/opt/airflow/scripts/script1.sh",
    "/opt/airflow/scripts/script2.sh",
    "/opt/airflow/scripts/script3.sh",
]

with DAG(
    dag_id="templated_script_runner",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["templated"],
) as dag:
    previous = None
    for i, path in enumerate(script_paths):
        task = BashOperator(task_id=f"run_script_{i+1}", bash_command=f"bash {path}")
        if previous:
            previous >> task
        previous = task
