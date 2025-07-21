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
    ExecuteQuery().run_queries_from_plan(
        [
            {
                "function": "exec_crud",
                "query": "example_query_3.sql",
            }
        ]
    )


sql_queries_dir = get_sql_queries_dir()

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

    # Use utils.py to generate Spark config for the insert
    spark_conf = ExecuteQuery().spark_submit_config(
        query=open(os.path.join(sql_queries_dir, "example_query_4.sql")).read(),
        tables=["your_table"],  # Replace with actual table(s) needed for the query
        table_to_update="your_table",  # Replace with the table being inserted into
        mode="append",
        update_flag=True,
    )

    t_insert = SparkSubmitOperator(
        task_id="insert_table",
        application=spark_conf["application"],
        conf=spark_conf["conf"],
        jars=spark_conf["jars"],
        application_args=spark_conf["application_args"],
        env_vars=spark_conf["env_vars"],
    )

    t_create >> t_insert
