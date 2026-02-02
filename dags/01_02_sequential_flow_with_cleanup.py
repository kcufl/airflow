from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task


@dag(
    dag_id="01_02_sequential_with_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["flow-sample", "01-sequential", "cleanup"],
)
def sequential_with_cleanup():
    """
    [샘플 목적]
    - 순차 실행 흐름에서 “마지막 정리/알림/리소스 해제”를
      성공/실패 상관없이 반드시 수행하는 운영형 패턴을 보여줌
      Step A -> Step B -> Step C -> Cleanup(always)

    [핵심 포인트]
    - cleanup에 trigger_rule="all_done"을 주면 upstream 성공/실패/스킵과 무관하게 실행됨
    - 운영에서는 임시 리소스 삭제, 결과 아카이브, 알림/메트릭 전송 등을 cleanup에 넣는 경우가 많음
    - (테스트용) step_c에서 일부러 예외를 던져도 cleanup이 도는지 확인 가능
    """

    @task
    def step_a() -> Dict[str, str]:
        data = {"token": "T-001", "ts": datetime.utcnow().isoformat()}
        print("[A] data:", data)
        return data

    @task
    def step_b(data: Dict[str, str]) -> Dict[str, str]:
        print("[B] got:", data)
        out = {**data, "processed": "yes"}
        print("[B] out:", out)
        return out

    @task
    def step_c(out: Dict[str, str]) -> None:
        print("[C] final:", out)
        # 실패 케이스 테스트하려면 아래 주석 해제
        # raise RuntimeError("intentional failure for testing")

    @task(trigger_rule="all_done")
    def cleanup(**context) -> None:
        print("[cleanup] always runs")
        print("[cleanup] run_id:", context["dag_run"].run_id)
        print("[cleanup] do notify/cleanup/archive/metrics here")

    c = step_c(step_b(step_a()))
    c >> cleanup()


sequential_with_cleanup()
