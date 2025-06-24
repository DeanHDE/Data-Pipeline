import os


def get_sql_queries_dir():
    """
    Returns the path to the SQL queries directory depending on the environment.
    - If IS_MAIN_CONTAINER is set to "true" (case-insensitive), use the container path.
    - Otherwise, use the local (host/dev) path.
    """
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "airflow_scripts", "sql_queries")
    )


SQL_QUERIES_DIR = get_sql_queries_dir()
