from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="08_02_event_orchestrator_trigger_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flow-sample", "08-event", "orchestrator"],
)
def event_orchestrator_trigger_dag():
    """
    [샘플 목적]
    - 한 DAG가 다른 DAG를 이벤트처럼 "트리거"하는 패턴
    - TriggerDagRunOperator로 target DAG를 실행시키고 conf를 전달

    [핵심 포인트]
    - trigger_dag_id: 실행할 대상 DAG ID
    - conf: 대상 DAG로 전달할 payload
    - wait_for_completion: True로 두면 대상 DAG 완료까지 대기(운영 상황에 따라 선택)
    """

    @task
    def build_conf(**context):
        # 상위 DAG에서 만든 실행 컨텍스트를 하위 DAG로 전달한다고 보면 됨
        run_id = context["dag_run"].run_id
        return {"source": "orchestrator", "parent_run_id": run_id, "action": "run_target"}

    conf = build_conf()

    trigger_target = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="08_event_target_dag",
        conf=conf,
        wait_for_completion=False,  # 필요하면 True로
    )

    trigger_target


event_orchestrator_trigger_dag()
