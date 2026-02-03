from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="03_02_conditional_short_circuit",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "03-conditional", "shortcircuit"],
)
def conditional_short_circuit():
    """
    [샘플 목적]
    - "할 가치가 없으면 여기서 끝"을 보여주는 조기 종료(Early Stop) 패턴
      Start -> Gate(조건) -> (Downstream 전체)  [조건 False면 전부 SKIPPED]

    [핵심 포인트]
    - ShortCircuitOperator는 조건이 True면 downstream을 정상 실행한다.
    - 조건이 False면 downstream 전체(작업 + 종료 마커 포함)를 강제로 SKIPPED 처리한다.
      => 실패(failed)가 아니라 스킵(skipped)이므로, "불필요한 작업을 성공적으로 건너뜀"을 표현할 때 유용하다.

    [관찰 포인트]
    - should_continue=False:
        start: success
        gate:  success
        work_1/work_2/end: skipped   (end도 downstream이므로 함께 스킵되는 것이 정상)
    - should_continue=True:
        start/gate/work_1/work_2/end: success
    """

    start = EmptyOperator(task_id="start")

    def should_continue(**_context) -> bool:
        # False로 두면 downstream 전체가 skipped 되는 것을 확인할 수 있음
        return False  # True/False 바꿔가며 확인

    gate = ShortCircuitOperator(
        task_id="gate",
        python_callable=should_continue,
        # 기본 동작: False면 downstream 전체를 SKIPPED 처리
        # (여기서는 데모 목적이라 기본값을 그대로 사용)
        # ignore_downstream_trigger_rules=True (default)
    )

    @task
    def work_1():
        print("[work_1] executed")

    @task
    def work_2():
        print("[work_2] executed")

    # 종료 마커(End)도 downstream에 포함되어, gate=False면 함께 SKIPPED 된다.
    end = EmptyOperator(task_id="end")

    start >> gate >> [work_1(), work_2()] >> end


conditional_short_circuit()
