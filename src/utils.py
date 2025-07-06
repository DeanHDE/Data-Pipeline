import os
import subprocess
import logging
from typing import List, Optional
from dotenv import load_dotenv
from functools import wraps
from pydantic import validate_call
import psycopg2
import polars as pl
from src.settings import get_sql_queries_dir

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def check_env_vars() -> None:
    """Check if the required environment variables for PostgreSQL are set."""
    REQUIRED = {
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "AIRFLOW__API__SECRET_KEY",
    }
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    env_path = os.path.abspath(env_path)

    if not os.path.isfile(env_path):
        logger.info(
            f"❌ .env file not found at {env_path} or one of variables {', '.join(REQUIRED)} not set."
        )
        return

    with open(env_path) as f:
        content = f.read()

    found = {
        line.split("=")[0].strip()
        for line in content.splitlines()
        if "=" in line and not line.strip().startswith("#")
    }
    missing = REQUIRED - found

    if missing:
        logger.info(f"❌ Missing environment variables: {', '.join(missing)}")
        return


check_env_vars()

pg_user = os.environ.get("POSTGRES_USER")
pg_password = os.environ.get("POSTGRES_PASSWORD")
pg_database = os.environ.get("POSTGRES_DB")


class ExecuteQuery:
    """A class to execute SQL queries using psycopg2, polars, or by submitting Spark jobs."""

    def __init__(self):
        self.conn = psycopg2.connect(
            host="postgres",
            port=5432,
            user=pg_user,
            password=pg_password,
            dbname=pg_database,
        )
        # JDBC config for PG
        self.pg_jdbc_url = f"jdbc:postgresql://postgres:5432/postgres"
        self.pg_jdbc_props = {
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        }
        self.spark_master = "spark://spark:7077"
        self.spark_jars = "/opt/spark/jars/postgresql-42.7.3.jar"

    @validate_call
    def _jdbc_options(self, table: str) -> dict:
        return {
            "url": self.pg_jdbc_url,
            "dbtable": table,
            "user": self.pg_jdbc_props["user"],
            "password": self.pg_jdbc_props["password"],
            "driver": self.pg_jdbc_props["driver"],
        }

    @validate_call
    def exec_crud(self, query: str):
        """Execute a SQL statement (INSERT/UPDATE/DELETE) in Postgres directly."""
        with self.conn.cursor() as cur:
            logger.info(f"Executing query in postgres directly: {query}")
            cur.execute(query)
            self.conn.commit()

    @validate_call
    def exec_select(self, query: str):
        """Execute a SELECT statement in Postgres and print the result as a Polars DataFrame."""
        logger.info(f"SELECTING table from postgres directly using polars df: {query}")
        df = pl.read_database(query, connection=self.conn)
        logger.info(df)

    def submit_spark_job(
        self,
        script_path: str,
        tables: List[str],
        query: str,
        table_to_update: Optional[str] = None,
        mode: Optional[str] = None,
        update_flag: Optional[bool] = None,
        extra_args: Optional[List[str]] = None,
    ):
        """
        Submit a Spark job to the cluster using spark-submit.
        Passes all relevant arguments to the Spark script.
        """
        spark_submit_cmd = [
            "spark-submit",
            "--master",
            self.spark_master,
            "--jars",
            self.spark_jars,
            script_path,
            "--pg_url",
            self.pg_jdbc_url,
            "--pg_user",
            self.pg_jdbc_props["user"],
            "--pg_password",
            self.pg_jdbc_props["password"],
            "--tables",
            ",".join(tables),
            "--query",
            query,
        ]
        if table_to_update:
            spark_submit_cmd += ["--pg_table", table_to_update]
        if mode:
            spark_submit_cmd += ["--mode", mode]
        if update_flag is not None:
            spark_submit_cmd += ["--update_flag", str(update_flag)]
        if extra_args:
            spark_submit_cmd.extend(extra_args)
        logger.info(f"Submitting Spark job: {' '.join(spark_submit_cmd)}")
        result = subprocess.run(spark_submit_cmd, capture_output=True, text=True)
        logger.info(f"Spark job stdout:\n{result.stdout}")
        if result.stderr:
            logger.error(f"Spark job stderr:\n{result.stderr}")
        if result.returncode != 0:
            raise RuntimeError(f"Spark job failed with exit code {result.returncode}")

    @validate_call
    def exec_crud_spark(
        self,
        query: str,
        tables: List[str],
        update_flag: bool,
        table_to_update: str,
        mode: str,
        spark_script: str = "/opt/airflow/dags/scripts/pg_insert_job.py",
    ):
        """
        Submit a Spark job to perform an INSERT using Spark SQL.
        All Spark logic (including loading tables) should be in the script.
        """
        if not query.lower().strip().startswith("insert"):
            logger.info(
                "Only INSERT allowed in SPARK CRUD operations. Attempted: " + query
            )
            raise AssertionError("Only INSERT allowed in SPARK CRUD operations.")

        self.submit_spark_job(
            script_path=spark_script,
            tables=tables,
            query=query,
            table_to_update=table_to_update,
            mode=mode,
            update_flag=update_flag,
        )

    @validate_call
    def exec_select_spark(
        self,
        query: str,
        tables: List[str],
        spark_script: str = "/opt/airflow/dags/scripts/pg_select_job.py",
    ):
        """
        Submit a Spark job to perform a SELECT using Spark SQL.
        All Spark logic (including loading tables) should be in the script.
        """
        self.submit_spark_job(
            script_path=spark_script,
            tables=tables,
            query=query,
        )

    def run_queries_from_plan(self, query_plan: list):
        """
        query_plan: List of dicts, each with:
        - "function": name of ExecuteQuery method to call (str)
        - "query": SQL filename (str) or SQL string
        - ...other kwargs for the method (optional)
        """
        sql = ExecuteQuery()
        sql_queries_dir = get_sql_queries_dir()

        for step in query_plan:
            func_name = step["function"]
            query = step["query"]
            # If query is a filename, load its contents
            if (
                isinstance(query, str)
                and query.endswith(".sql")
                and os.path.isfile(os.path.join(sql_queries_dir, query))
            ):
                with open(os.path.join(sql_queries_dir, query)) as f:
                    query = f.read()
            kwargs = {k: v for k, v in step.items() if k not in ("function", "query")}
            func = getattr(sql, func_name)
            func(query=query, **kwargs)
