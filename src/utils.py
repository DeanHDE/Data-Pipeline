import os
import subprocess
from pyspark.sql import SparkSession
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
        self.spark_jars = "/opt/airflow/jars/postgresql-42.7.3.jar"

        self.spark = (
            SparkSession.builder.appName("PGSpark")
            .master("spark://spark:7077")
            .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar")
            .getOrCreate()
        )

    @validate_call
    def _load_pg_tables_to_spark(self, tables: List[str]):
        """
        Loads one or more PostgreSQL tables into Spark and registers them as temp views.
        """
        for table in tables:
            try:
                df = (
                    self.spark.read.format("jdbc")
                    .options(**self._jdbc_options(table))
                    .load()
                )
                df.createOrReplaceTempView(table)
            except Exception as e:
                logger.info(f"Failed to load table '{table}': {e}")
                raise

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

    @validate_call
    def write_spark_df_to_pg(self, df, table: str, mode: str):
        """
        Write a single Spark DataFrame to a PostgreSQL table using JDBC.
        mode: "append", "overwrite", "ignore", or "error"
        """
        df.write.format("jdbc").options(**self._jdbc_options(table)).mode(mode).save()

    @validate_call
    def exec_crud_spark(
        self,
        query: str,
        tables: List[str],
        update_flag: bool,
        table_to_update: str,
        mode: str,
    ):
        """
        Execute a SQL statement INSERT using Spark SQL.
        """
        if not query.lower().strip().startswith("insert"):
            logger.info(
                "Only INSERT allowed in SPARK CRUD operations. Attempted: " + query
            )
            raise AssertionError("Only INSERT allowed in SPARK CRUD operations.")

        self._load_pg_tables_to_spark(tables)
        self.spark.sql(query)
        df = self.spark.sql(f"SELECT * FROM {table_to_update}")
        df.cache()
        df.count()  # force caching with action
        if update_flag:
            logger.info(
                "Running INSERT operation in SPARK on table: " + table_to_update
            )
            # If update_flag is True, rewrite the DataFrame to the specified table
            if mode == "overwrite":
                mode = "append"
                self.exec_crud(query=f"TRUNCATE TABLE {table_to_update};")
                self.write_spark_df_to_pg(df=df, table=table_to_update, mode=mode)

    @validate_call
    def exec_select_spark(self, query: str, tables: List[str]):
        """
        Execute a SELECT statement using Spark SQL and print the DataFrame.
        """
        self._load_pg_tables_to_spark(tables)
        df = self.spark.sql(query)
        logger.info(f"Loaded {df.count()} rows from SPARK:")
        df.show()

    @validate_call
    def spark_submit_config(
        self,
        query: str,
        tables: List[str],
        table_to_update: str,
        mode: str,
        update_flag: bool,
        extra_args: Optional[List[str]] = None,
    ):
        """Prepare Spark submit configuration for executing a query."""
        application_args = [
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
            "--pg_table",
            table_to_update,
            "--mode",
            mode,
            "--update_flag",
            str(update_flag),
        ]
        if extra_args:
            application_args.extend(extra_args)
        return {
            # "application": "../spark_job_init.py",
            "conf": {
                "spark.master": self.spark_master,
                "spark.jars": self.spark_jars,
            },
            "jars": self.spark_jars,
            "application_args": application_args,
            "env_vars": {
                "PYSPARK_PYTHON": "python3",
                "PYSPARK_DRIVER_PYTHON": "python3",
            },
        }

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
