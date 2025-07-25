from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.utils import ExecuteQuery, get_sql_queries_dir
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_table_callable():
    ExecuteQuery(app_name="PGAppCreateExample").run_queries_from_plan(
        [
            {
                "function": "exec_crud",
                "query": "example_query_3.sql",
            }
        ]
    )


def insert_table_2_callable():
    ExecuteQuery(app_name="PGAppInsertExample").run_queries_from_plan(
        [
            {
                "function": "exec_crud_spark",
                "query": "example_query_4.sql",
                "tables": ["test"],
                "update_flag": True,
                "table_to_update": "test",
                "mode": "append",
            }
        ]
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

    """t_insert = SparkSubmitOperator(
        task_id="insert_table",
        application=spark_conf["application"],
        conf=spark_conf["conf"],
        jars=spark_conf["jars"],
        query=spark_conf["application_args"][3],  # query is the SQL string
        application_args=spark_conf["application_args"],
        env_vars=spark_conf["env_vars"],
    )"""

    t_insert = PythonOperator(
        task_id="insert_table",
        python_callable=insert_table_2_callable,
    )

    t_create >> t_insert
