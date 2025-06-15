print('hello world')
from src.utils import ExecuteQuery

sql = ExecuteQuery()

def run_queries():
    sql.exec_crud(query="DROP TABLE IF EXISTS test;") 
    sql.exec_crud(query="CREATE TABLE test (id INT, name VARCHAR(100));")
    sql.exec_crud(query="INSERT INTO test (id, name) VALUES (1,'test_name');")
    sql.exec_select(query="SELECT * FROM test;")

    # Spark: Insert a new row
    sql.exec_crud_spark(
        query="INSERT INTO test VALUES (2, 'spark_row_1')",
        tables=["test"],
        update_flag=True,
        table_to_update="test",
        mode="overwrite"
    )

    # Spark: Select to verify update
    sql.exec_select_spark(
        query="SELECT * FROM test",
        tables=["test"]
    )

    sql.exec_select(query="SELECT * FROM test;")


    sql.exec_crud(query="DROP TABLE IF EXISTS test;") 



if __name__ == '__main__':
    print('This is the main module.')   
    run_queries()