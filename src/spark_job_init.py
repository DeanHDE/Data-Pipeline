from utils import ExecuteQuery
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg_url", required=True)
    parser.add_argument("--pg_user", required=True)
    parser.add_argument("--pg_password", required=True)
    parser.add_argument("--tables", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--pg_table", required=False)
    parser.add_argument("--mode", required=False, default="append")
    parser.add_argument("--update_flag", required=False, default="False")
    args = parser.parse_args()
