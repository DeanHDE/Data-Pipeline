import psycopg2
from typing import List
import logging
import polars as pl
from pyspark.sql import SparkSession
from pydantic import validate_call

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExecuteQuery:
    """A class to execute a SQL query using psycopg2 and return the results."""

    def __init__(self):
        self.conn = psycopg2.connect(
                            host="postgres",
                            port=5432,
                            user="postgres",
                            password="postgres",
                            dbname="postgres"
                    )
        self.spark = SparkSession.builder \
                    .appName("PGSpark") \
                    .master("spark://spark:7077") \
                    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
                    .getOrCreate()
        # JDBC config for PG
        self.pg_jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
        self.pg_jdbc_props = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

    @validate_call
    def _jdbc_options(self, table:str):
        return {
            "url": self.pg_jdbc_url,
            "dbtable": table,
            "user": self.pg_jdbc_props["user"],
            "password": self.pg_jdbc_props["password"],
            "driver": self.pg_jdbc_props["driver"],
        }

    @validate_call
    def exec_crud(self, query: str):
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    @validate_call
    def exec_select(self, query: str):
        conn = self.conn
        df = pl.read_database(query, connection=conn)
        print (df) 
        logger.info(df)   

    # --- Spark versions below ---

    @validate_call
    def _load_pg_tables_to_spark(self, tables: List[str]):
        """
        Loads one or more PostgreSQL tables into Spark and registers them as temp views.
        """
        for table in tables:
            try:
                df = self.spark.read.format("jdbc").options(**self._jdbc_options(table)).load()
                df.createOrReplaceTempView(table)
            except Exception as e:
                logger.info(f"Failed to load table '{table}': {e}")
                raise

    @validate_call
    def write_spark_df_to_pg(self, df, table: str, mode: str):
        """
        Write a single Spark DataFrame to a PostgreSQL table using JDBC.
        mode: "append", "overwrite", "ignore", or "error"
        """
        df.write.format("jdbc") \
            .options(**self._jdbc_options(table)) \
            .mode(mode) \
            .save()

    @validate_call
    def exec_crud_spark(self, query: str, tables: List[str], update_flag: bool, table_to_update: str ,mode: str):
        """
        Execute a SQL statement (INSERT/UPDATE/DELETE) using Spark SQL.
        """
        self._load_pg_tables_to_spark(tables)
        self.spark.sql(query)
        if update_flag:
            # If update_flag is True, rewrite the DataFrame to the specified table
            df = self.spark.table(table_to_update)
            self.write_spark_df_to_pg(df=df, table=table_to_update, mode=mode)

    @validate_call
    def exec_select_spark(self, query: str, tables: List[str]):
        """
        Execute a SELECT statement using Spark SQL and print the DataFrame.
        """
        self._load_pg_tables_to_spark(tables)
        df = self.spark.sql(query)
        print(f"Loaded {df.count()} rows")

        print(df)
        logger.info(df)
        df.show()