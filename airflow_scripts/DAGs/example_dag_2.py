from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.utils import run_queries_from_plan

QUERY_PLAN_CREATE = [
    {
        "function": "exec_crud",
        "query": "example_query_3.sql",
    },
]

QUERY_PLAN_INSERT = [
    {
        "function": "exec_crud_spark",
        "query": "example_query_4.sql",
        "tables": ["test"],
        "update_flag": True,
        "table_to_update": "test",
        "mode": "append",
    },
]


def run_create():
    run_queries_from_plan(QUERY_PLAN_CREATE)


def run_insert():
    run_queries_from_plan(QUERY_PLAN_INSERT)


with DAG(
    dag_id="example_exec_queries_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    t_create = PythonOperator(
        task_id="create_table",
        python_callable=run_create,
    )
    t_insert = PythonOperator(
        task_id="insert_data",
        python_callable=run_insert,
    )

    t_create >> t_insert
