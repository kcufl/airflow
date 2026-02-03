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
    - 조건이 false면 downstream 전체를 "스킵 성공" 처리하는 패턴
      Start -> Gate(조건) -> (Downstream 작업들) -> End

    [핵심 포인트]
    - ShortCircuitOperator는 True면 downstream 진행, False면 downstream 전부 skipped
    - End는 downstream이 전부 skipped여도 "정상 종료(success)"로 마무리되도록 trigger_rule을 조정
    """

    start = EmptyOperator(task_id="start")

    def should_continue(**_context) -> bool:
        # 샘플: False면 downstream이 전부 skipped 되는 걸 확인
        return False  # True/False 바꿔가며 확인

    gate = ShortCircuitOperator(
        task_id="gate",
        python_callable=should_continue,
    )

    @task
    def work_1():
        print("[work_1] executed")

    @task
    def work_2():
        print("[work_2] executed")

    # ✅ downstream이 모두 skipped여도 end는 실행되어 success로 마무리
    # - upstream 실패가 없으면(success/skipped) 실행 가능
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    start >> gate >> [work_1(), work_2()] >> end


conditional_short_circuit()
