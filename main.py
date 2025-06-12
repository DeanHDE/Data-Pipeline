print('hello world')
from src.utils import ExecuteQuery

sql = ExecuteQuery()

def run_queries():
    sql.exec_crud(query="CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, name VARCHAR(100));")
    sql.exec_crud(query="INSERT INTO test (name) VALUES ('test_name');")
    sql.exec_select(query="SELECT * FROM test;")
    sql.exec_crud(query="DROP TABLE IF EXISTS test;") 

    # Spark: Insert a new row
    sql.exec_crud_spark(
        query="INSERT INTO test VALUES (DEFAULT, 'spark_row_1')",
        tables=["test"],
        update_flag=True,
        table_to_update="test",
        mode="append"
    )

    # Spark: Select to verify update
    sql.exec_select_spark(
        query="SELECT * FROM test",
        tables=["test"]
    )


if __name__ == '__main__':
    print('This is the main module.')   
    run_queries()