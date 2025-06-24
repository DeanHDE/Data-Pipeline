from src.utils import ExecuteQuery
import os

sql = ExecuteQuery()


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
    exec_query = ExecuteQuery()
    exec_query.run_queries_from_plan(QUERY_PLAN_CREATE)


def run_insert():
    exec_query = ExecuteQuery()
    exec_query.run_queries_from_plan(QUERY_PLAN_INSERT)


def run_queries():
    sql.exec_crud(query="DROP TABLE IF EXISTS test;")
    sql.exec_crud(
        query="CREATE TABLE test (id INT, name VARCHAR(100), v varchar(100));"
    )
    sql.exec_crud(query="INSERT INTO test (id, name , v) VALUES (1,'test_name', 'pg');")
    sql.exec_select(query="SELECT * FROM test;")

    # Spark: Insert a new row
    sql.exec_crud_spark(
        query="INSERT INTO test VALUES (2, 'spark_row_1', 'spark');",
        tables=["test"],
        update_flag=True,
        table_to_update="test",
        mode="append",
    )

    # Spark: Select to verify update
    sql.exec_select_spark(query="SELECT * FROM test", tables=["test"])

    sql.exec_select(query="SELECT * FROM test;")

    sql.exec_crud_spark(
        query="INSERT INTO test VALUES (3, 'spark_row_2', 'spark');",  # "DELETE FROM test WHERE id = 1;",
        tables=["test"],
        update_flag=True,
        table_to_update="test",
        mode="overwrite",
    )

    sql.exec_crud(query="DROP TABLE IF EXISTS test;")

    run_create()
    run_insert()


if __name__ == "__main__":
    # run_queries()
    sql.exec_select(query="SELECT * FROM test;")
