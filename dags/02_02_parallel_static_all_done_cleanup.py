from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="02_parallel_static_all_done_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "02-parallel", "static", "trigger-rule"],
)
def parallel_static_all_done_cleanup():
    """
    [샘플 목적]
    - 병렬 구간에서 일부 task가 실패해도,
      "정리/알림/집계" 같은 후처리 단계는 반드시 실행시키는 운영 패턴을 보여줌

      Start -> (B, C, D 병렬) -> Join(anyway) -> Cleanup(always)

    [핵심 포인트]
    - join_anyway: trigger_rule="all_done"
      => upstream이 success/failed/skipped 어떤 상태든 join을 실행
    - cleanup 또한 trigger_rule="all_done"
      => 워크플로우 최종 정리/알림/리소스 해제 단계로 사용하기 좋음
    """

    start = EmptyOperator(task_id="start")

    @task
    def b():
        print("[B] OK")

    @task
    def c():
        print("[C] OK")

    @task
    def d():
        # 일부러 실패를 발생시켜도 join/cleanup이 실행되는지 확인하는 샘플
        print("[D] FAIL (intentional)")
        raise RuntimeError("intentional failure in D")

    join_anyway = EmptyOperator(task_id="join_anyway", trigger_rule="all_done")

    @task(trigger_rule="all_done")
    def cleanup():
        print("[cleanup] always runs even if some parallel tasks failed")
        print("[cleanup] do notify/cleanup/reporting here")

    start >> [b(), c(), d()] >> join_anyway >> cleanup()


parallel_static_all_done_cleanup()
