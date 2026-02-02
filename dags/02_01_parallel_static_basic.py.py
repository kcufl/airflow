from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="02_01_parallel_static_basic",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "02-parallel", "static"],
)
def parallel_static_basic():
    """
    [샘플 목적]
    - 고정 병렬(Static Parallel)의 가장 기본 형태를 보여줌
      Start -> (B, C, D 병렬 실행) -> Join -> End

    [핵심 포인트]
    - start >> [b(), c(), d()] : 리스트 형태로 downstream을 묶으면 병렬로 실행됨
    - join은 기본 trigger_rule=all_success
      => 병렬 중 하나라도 실패하면 join 이후 단계는 실행되지 않음(기본 안전 동작)
    """

    start = EmptyOperator(task_id="start")

    @task
    def b():
        print("[B] done")

    @task
    def c():
        print("[C] done")

    @task
    def d():
        print("[D] done")

    join = EmptyOperator(task_id="join")
    end = EmptyOperator(task_id="end")

    start >> [b(), c(), d()] >> join >> end


parallel_static_basic()
