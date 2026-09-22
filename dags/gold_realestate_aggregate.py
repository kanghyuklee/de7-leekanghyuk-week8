"""
Q3 Gold — Silver Parquet → 5개 집계 → PostgreSQL JDBC
- ExternalTaskSensor: silver_realestate_transform 완료 대기
- SparkSubmitOperator: gold_spark_sql.py 실행
- 검증 task: 각 테이블 row count > 0 확인
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.sensors.external_task import ExternalTaskSensor

S3_BUCKET = os.environ.get("S3_BUCKET", "")


def _latest_silver_exec_date(logical_date, **kwargs):
    from airflow.models import DagRun
    runs = DagRun.find(dag_id="silver_realestate_transform", state="success")
    if runs:
        runs.sort(key=lambda r: r.execution_date, reverse=True)
        return runs[0].execution_date
    return logical_date


GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def _verify_tables(**kwargs):
    """적재 후 검증: 각 테이블 row count > 0"""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("GOLD_DB_HOST", "postgres"),
        port=int(os.environ.get("GOLD_DB_PORT", "5432")),
        dbname=os.environ.get("GOLD_DB_NAME", "gold"),
        user=os.environ.get("GOLD_DB_USER", "gold_user"),
        password=os.environ.get("GOLD_DB_PASSWORD", "gold_pass"),
    )
    cur = conn.cursor()
    for table in GOLD_TABLES:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        cnt = cur.fetchone()[0]
        print(f"[Gold 검증] {table}: {cnt} rows")
        assert cnt > 0, f"{table} is empty!"
    cur.close()
    conn.close()
    print("[Gold 검증] 5개 테이블 모두 OK")


with DAG(
    dag_id="gold_realestate_aggregate",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "realestate", "medallion"],
) as dag:

    wait_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        execution_date_fn=_latest_silver_exec_date,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=30,
        mode="reschedule",
    )

    spark_gold = SparkSubmitOperator(
        task_id="aggregate_gold",
        application="/opt/airflow/scripts/q3/gold_spark_sql.py",
        name="gold_realestate_aggregate",
        conn_id="spark_default",
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
            ),
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
        },
        application_args=[
            "--bucket", S3_BUCKET,
            "--db-host", os.environ.get("GOLD_DB_HOST", "postgres"),
            "--db-port", os.environ.get("GOLD_DB_PORT", "5432"),
            "--db-name", os.environ.get("GOLD_DB_NAME", "gold"),
            "--db-user", os.environ.get("GOLD_DB_USER", "gold_user"),
            "--db-password", os.environ.get("GOLD_DB_PASSWORD", "gold_pass"),
        ],
        jars=(
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/spark/jars/postgresql-42.6.0.jar"
        ),
        verbose=True,
    )

    verify = PythonOperator(
        task_id="verify_tables",
        python_callable=_verify_tables,
    )

    wait_silver >> spark_gold >> verify
