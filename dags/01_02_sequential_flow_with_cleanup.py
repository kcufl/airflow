from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="01_02_sequential_flow_with_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["flow-sample", "01-sequential"],
)
def sequential_with_cleanup():

    start = EmptyOperator(task_id="start")

    @task
    def step_a() -> Dict[str, str]:
        data = {"token": "T-001", "created": datetime.utcnow().isoformat()}
        print("[A] data:", data)
        return data

    @task
    def step_b(data: Dict[str, str]) -> Dict[str, str]:
        print("[B] got:", data)
        # 일부러 결과를 만들었다고 가정
        out = {**data, "processed": "yes"}
        print("[B] out:", out)
        return out

    @task
    def step_c(out: Dict[str, str]) -> None:
        print("[C] final:", out)
        # 샘플: 정상 종료

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        # 성공/실패 상관없이 반드시 실행
        print("[cleanup] always runs (success or failure)")
        print("[cleanup] do cleanup/notify/archive/etc")

    a = step_a()
    b = step_b(a)
    c = step_c(b)

    start >> a >> b >> c
    c >> cleanup()


sequential_with_cleanup()
