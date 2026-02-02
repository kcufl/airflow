from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="01_01_sequential_basic_rich",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    tags=["flow-sample", "01-sequential"],
)
def sequential_basic_rich():
    """
    [샘플 목적]
    - 가장 기본적인 순차 실행(Sequential Flow) 패턴을 “운영 감각”으로 보여줌
      Prepare -> Run -> Validate
    - 단계 간 값을 전달(XCom)하고, 실행 컨텍스트(run_id 등)를 로그로 남기는 형태

    [핵심 포인트]
    - TaskFlow(@task) 반환값은 다음 task 입력으로 전달되며 내부적으로 XCom을 사용
    - context['dag_run'].run_id 등 컨텍스트를 이용해 “실행 단위 식별자”를 만들 수 있음
    - 검증(validate)에서 실패를 명확히 발생시키면 재시도/장애 원인 파악이 쉬워짐
    """

    @task
    def prepare(**context) -> Dict[str, str]:
        dag_run = context["dag_run"]
        payload = {
            "run_id": dag_run.run_id,
            "logical_date": str(context["logical_date"]),
            "created_at_utc": datetime.utcnow().isoformat(),
            "mode": "sample",
        }
        print("[prepare] payload:", payload)
        return payload

    @task
    def run(payload: Dict[str, str]) -> Dict[str, str]:
        print("[run] received payload:", payload)

        result = {
            **payload,
            "status": "OK",
            "artifact_id": f"artifact::{payload['run_id']}",
        }
        print("[run] result:", result)
        return result

    @task
    def validate(result: Dict[str, str]) -> None:
        print("[validate] checking result:", result)

        if result.get("status") != "OK":
            raise AirflowFailException("validation failed: status != OK")

        if not result.get("artifact_id"):
            raise AirflowFailException("validation failed: missing artifact_id")

        print("[validate] passed ✅")

    validate(run(prepare()))


sequential_basic_rich()
