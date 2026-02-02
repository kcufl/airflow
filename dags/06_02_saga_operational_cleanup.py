from __future__ import annotations

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="06_02_saga_operational_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "06-saga", "ops"],
)
def saga_operational_cleanup():
    """
    [샘플 목적]
    - 운영형 Saga 패턴:
      1) forward 단계 수행
      2) 실패 시 보상(compensation) 단계 수행
      3) cleanup/알림 단계는 성공/실패 무관하게 항상 실행

    [핵심 포인트]
    - compensate 구간은 trigger_rule을 이용해 "실패할 때만" 실행
    - cleanup은 trigger_rule="all_done"으로 항상 실행
    - 보상 단계는 역순으로 배치하는 것이 일반적
    """

    start = EmptyOperator(task_id="start")

    @task
    def forward_1() -> str:
        print("[forward_1] create temp resource / stage")
        return "created"

    @task
    def forward_2() -> str:
        print("[forward_2] apply change / write intermediate")
        return "applied"

    @task
    def forward_3() -> str:
        print("[forward_3] finalize (intentional fail)")
        raise RuntimeError("finalize failed")

    @task(trigger_rule="one_failed")
    def compensate_2(state_2: str):
        if state_2 == "applied":
            print("[compensate_2] revert step 2")
        else:
            print("[compensate_2] skip")

    @task(trigger_rule="one_failed")
    def compensate_1(state_1: str):
        if state_1 == "created":
            print("[compensate_1] delete temp resource from step 1")
        else:
            print("[compensate_1] skip")

    @task(trigger_rule="all_done")
    def cleanup_notify():
        print("[cleanup] always runs")
        print("[cleanup] send notification / emit metrics / finalize logs")

    f1 = forward_1()
    f2 = forward_2()
    f3 = forward_3()

    start >> f1 >> f2 >> f3

    # 실패 시 보상(역순)
    f3 >> compensate_2(f2)
    compensate_2(f2) >> compensate_1(f1)

    # 최종 정리/알림은 항상 실행
    [f3, compensate_1(f1), compensate_2(f2)] >> cleanup_notify()


saga_operational_cleanup()
