from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="04_compensation_on_failure",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "04-compensation"],
)
def compensation_on_failure():
    """
    [샘플 목적]
    - "실패하면 보상(정리/되돌리기)을 실행"하는 운영 패턴
    - Airflow는 트랜잭션 엔진이 아니라 오케스트레이터이므로
      롤백은 자동이 아니라 '보상 작업을 DAG에 명시'해서 구현한다.

    [흐름]
    start
      -> main (성공/실패 발생 가능)
         -> commit (성공시에만)
         -> compensate (실패시에만)
      -> cleanup (항상 실행)

    [핵심 포인트]
    - commit: trigger_rule=all_success (기본값) => 성공 시에만 실행
    - compensate: trigger_rule=one_failed => upstream 중 실패가 있으면 실행
    - cleanup: trigger_rule=all_done => 항상 실행
    """

    start = EmptyOperator(task_id="start")

    @task
    def main():
        print("[main] doing work...")
        # 샘플: 실패를 보고 싶으면 아래 주석 해제
        raise RuntimeError("intentional failure in main")

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

    m = main()

    start >> m
    m >> commit()
    m >> compensate()
    [commit(), compensate()] >> cleanup()


compensation_on_failure()
