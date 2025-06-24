from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.StreamHandler(),  # This keeps logging to terminal as well
    ],
)
logger = logging.getLogger(__name__)

logger.info("Starting to import sys and os")
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
logger.info("Added parent directory to sys.path")
from src.utils import ExecuteQuery

logger.info("Starting to import ExecuteQuery from src.utils")

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
    logger.info("Starting run_create")
    exec_query = ExecuteQuery()
    logger.info("Created ExecuteQuery instance in run_create")
    exec_query.run_queries_from_plan(QUERY_PLAN_CREATE)
    logger.info("Finished run_create")


def run_insert():
    logger.info("Starting run_insert")
    exec_query = ExecuteQuery()
    logger.info("Created ExecuteQuery instance in run_insert")
    exec_query.run_queries_from_plan(QUERY_PLAN_INSERT)
    logger.info("Finished run_insert")


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
