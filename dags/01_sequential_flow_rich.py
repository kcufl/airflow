from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="01_sequential_flow_rich",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # 샘플은 수동 실행
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    tags=["flow-sample", "01-sequential"],
)
def sequential_flow_rich():
    """
    순차 실행 예제:
    1) 준비(prepare)  -> 2) 실행(run) -> 3) 검증(validate)
    - 단계 간 결과값(XCom)을 dict로 전달
    - 실행 컨텍스트(dag_run, run_id 등) 로그 출력
    """

    @task
    def prepare() -> Dict[str, str]:
        # 실제 업무에선 여기서 "실행 파라미터"를 만든다고 보면 됨
        payload = {
            "request_id": "REQ-12345",
            "mode": "sample",
            "generated_at": datetime.utcnow().isoformat(),
        }
        print("[prepare] payload:", payload)
        return payload

    @task
    def run(payload: Dict[str, str]) -> Dict[str, str]:
        # 단계 2: payload를 받아 실제 작업 수행
        print("[run] received payload:", payload)

        # 샘플로 결과 생성
        result = {
            **payload,
            "status": "OK",
            "artifact": "result-file-or-id",
        }
        print("[run] produced result:", result)
        return result

    @task
    def validate(result: Dict[str, str]) -> None:
        # 단계 3: 결과 검증(샘플)
        print("[validate] checking result:", result)

        if result.get("status") != "OK":
            raise AirflowFailException("validation failed: status != OK")

        if "artifact" not in result:
            raise AirflowFailException("validation failed: missing artifact")

        print("[validate] validation passed ✅")

    payload = prepare()
    result = run(payload)
    validate(result)


sequential_flow_rich()
