"""
Q1 Bronze — 국토교통부 실거래가 API → S3 Bronze (XML 원본)
- TaskGroup: 시군구별 그룹 (6개 병렬 수집)
- Dynamic Task Mapping: 연-월 동적 생성
- BranchPythonOperator: 응답 0건/XML 파싱 실패 시 분기
- schedule: @monthly, catchup=True
"""
from __future__ import annotations

import os
from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

# ── 환경변수 (.env → docker-compose) ──
API_KEY = os.environ.get("REALESTATE_API_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")

API_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)

SIGUNGU_CODES = ["11680", "11650", "11710", "11440", "11170", "11200"]


def _fetch_xml(lawd_cd: str, deal_ymd: str) -> str | None:
    """API 호출 → XML 텍스트 반환 (실패 시 None)"""
    print(
        f"collector=이강혁, time={datetime.now()}, lawd={lawd_cd}"
    )
    resp = requests.get(
        API_URL,
        params={
            "serviceKey": API_KEY,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": 10000,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def _check_response(lawd_cd: str, deal_ymd: str, **kwargs) -> str:
    """응답 0건 / XML 파싱 실패 시 분기"""
    try:
        xml_text = _fetch_xml(lawd_cd, deal_ymd)
        if xml_text is None or "<item>" not in xml_text:
            return f"sg_{lawd_cd}.skip"
    except Exception as e:
        print(f"[{lawd_cd}] {deal_ymd} 파싱 실패: {e}")
        return f"sg_{lawd_cd}.skip"
    return f"sg_{lawd_cd}.upload"


@task
def upload_to_s3(lawd_cd: str, deal_ymd: str) -> str:
    """XML 원본 그대로 S3 bronze 저장"""
    print(
        f"collector=이강혁, time={datetime.now()}, lawd={lawd_cd}"
    )
    xml_text = _fetch_xml(lawd_cd, deal_ymd)
    s3_key = f"bronze/{deal_ymd}/{lawd_cd}.xml"
    boto3.client("s3").put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=xml_text.encode("utf-8"),
        ContentType="application/xml",
    )
    print(f"[{lawd_cd}] {deal_ymd} → s3://{S3_BUCKET}/{s3_key}")
    return s3_key


with DAG(
    dag_id="bronze_realestate_collect",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=True,
    tags=["bronze", "realestate", "medallion"],
) as dag:

    for code in SIGUNGU_CODES:
        with TaskGroup(group_id=f"sg_{code}") as tg:

            check = BranchPythonOperator(
                task_id="check",
                python_callable=_check_response,
                op_kwargs={
                    "lawd_cd": code,
                    "deal_ymd": "{{ ds_nodash[:6] }}",
                },
            )

            do_upload = upload_to_s3.override(task_id="upload")(
                lawd_cd=code,
                deal_ymd="{{ ds_nodash[:6] }}",
            )

            skip = EmptyOperator(task_id="skip")

            done = EmptyOperator(
                task_id="done",
                trigger_rule="none_failed_min_one_success",
            )

            check >> [do_upload, skip] >> done
