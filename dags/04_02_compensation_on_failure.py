from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="04_02_compensation_on_failure_fixed",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "04-compensation"],
)
def compensation_on_failure_fixed():
    """
    [샘플 목적]
    - 실패하면 보상(compensate)을 실행하고,
      cleanup은 성공/실패 무관하게 항상 실행하는 운영 패턴

    [핵심 포인트]
    - TaskFlow 함수는 "한 번만 호출"해서 태스크 핸들을 변수로 잡고 의존성 연결
    - commit: 성공 시에만 실행(기본 all_success)
    - compensate: main 실패 시에만 실행(one_failed)
    - cleanup: 항상 실행(all_done)
    """

    start = EmptyOperator(task_id="start")

    @task
    def main():
        print("[main] doing work...")
        # 샘플: 의도적으로 실패
        #raise RuntimeError("intentional failure in main")

    @task
    def commit():
        print("[commit] apply final commit/publish/promote etc")

    @task(trigger_rule="one_failed")
    def compensate():
        print("[compensate] rollback/cleanup partial state")
        print("[compensate] e.g., delete temp data, revert pointer, release lock")

    @task(trigger_rule="all_done")
    def cleanup():
        print("[cleanup] always runs")
        print("[cleanup] final cleanup / metrics / notify")

    # ---- DAG wiring (중요: 호출은 1번만!) ----
    m = main()
    cmt = commit()
    comp = compensate()
    cln = cleanup()

    start >> m
    m >> cmt
    m >> comp

    # commit이 스킵/실패/성공이든, compensate가 스킵/실패/성공이든 cleanup은 돌아야 하므로
    [cmt, comp] >> cln


compensation_on_failure_fixed()
