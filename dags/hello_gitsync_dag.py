"""
git-sync 동작 확인용 테스트 DAG
BashOperator 만 사용하여 간단히 echo 출력
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_gitsync",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "gitsync"],
) as dag:

    hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from git-sync! 이강혁 데엔7기 $(date)"',
    )

    check_env = BashOperator(
        task_id="check_env",
        bash_command='echo "Airflow Home: $AIRFLOW_HOME" && echo "Hostname: $(hostname)"',
    )

    hello >> check_env
