from __future__ import annotations

from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="08_02_event_target_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # ✅ 외부/상위 DAG 트리거 전용
    catchup=False,
    tags=["flow-sample", "08-event", "target"],
)
def event_target_dag():
    """
    [샘플 목적]
    - 다른 DAG(또는 외부 시스템)가 TriggerDagRunOperator로 실행시킬 대상 DAG
    - 전달받은 conf를 읽어 로직을 수행하는 구조
    """

    @task
    def run_target(**context):
        conf = (context["dag_run"].conf or {})
        print("[target] conf:", conf)
        print("[target] do target workflow logic...")

    run_target()


event_target_dag()
