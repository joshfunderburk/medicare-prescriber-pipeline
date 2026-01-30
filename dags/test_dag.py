from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    "tutorial",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    t1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from Airflow'",
    )

    t2 = SnowflakeOperator(
        task_id="run_snowflake_query",
        snowflake_conn_id="snowflake_default",
        sql="SELECT 1",
    )

    t1 >> t2