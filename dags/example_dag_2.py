from airflow import DAG
from airflow.providers.apache.spark.operators import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.utils import ExecuteQuery

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_table_callable():
    ExecuteQuery().run_queries_from_plan(
        [
            {
                "function": "exec_crud",
                "query": "example_query_3.sql",
            }
        ]
    )


def get_spark_conf():
    eq = ExecuteQuery()
    insert_query = open("/opt/airflow/sql/example_query_4.sql").read()
    return eq.spark_submit_config(
        query=insert_query,
        tables=["test"],  # all tables referenced in your query
        table_to_update="test",
        mode="append",
        update_flag=True,
    )


with DAG(
    dag_id="example_exec_queries_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
) as dag:
    t_create = PythonOperator(
        task_id="create_table",
        python_callable=create_table_callable,
    )

    spark_conf = get_spark_conf()
    t_insert = SparkSubmitOperator(
        task_id="insert_data",
        conn_id="spark_default",
        **spark_conf,
    )

    t_create >> t_insert
