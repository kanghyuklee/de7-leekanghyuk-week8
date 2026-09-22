"""
Q2 Silver - XML -> PySpark 정제 + UDF 2개 -> Parquet
- ExternalTaskSensor: bronze_realestate_collect 완료 대기
- SparkSubmitOperator: silver_spark.py 실행
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

try:
    from airflow.providers.apache.spark.operators.spark_submit import (
        SparkSubmitOperator,
    )
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

S3_BUCKET = os.environ.get("S3_BUCKET", "")


def _latest_bronze_exec_date(logical_date, **kwargs):
    from airflow.models import DagRun
    runs = DagRun.find(dag_id="bronze_realestate_collect", state="success")
    if runs:
        runs.sort(key=lambda r: r.execution_date, reverse=True)
        return runs[0].execution_date
    return logical_date


with DAG(
    dag_id="silver_realestate_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "realestate", "medallion"],
) as dag:

    wait_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        execution_date_fn=_latest_bronze_exec_date,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=30,
        mode="reschedule",
    )

    if HAS_SPARK:
        spark_silver = SparkSubmitOperator(
            task_id="transform_silver",
            application="/opt/airflow/scripts/q2/silver_spark.py",
            name="silver_realestate_transform",
            conn_id="spark_default",
            conf={
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
                ),
                "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            },
            application_args=["--bucket", S3_BUCKET],
            jars=(
                "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
            ),
            verbose=True,
        )
    else:
        spark_silver = BashOperator(
            task_id="transform_silver",
            bash_command='echo "SparkSubmitOperator unavailable - silver transform placeholder"',
        )

    wait_bronze >> spark_silver