from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="03_01_conditional_branch_join",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "03-conditional", "branch"],
)
def conditional_branch_join():
    """
    [샘플 목적]
    - 조건에 따라 실행 경로를 갈라타는 "if/else" 스타일 분기
      Branch -> (path_a OR path_b) -> Join -> End

    [핵심 포인트]
    - BranchPythonOperator는 다음에 실행할 task_id를 '문자열'로 반환
    - 선택되지 않은 경로는 skipped 상태가 됨
    - 따라서 합류(Join) 단계는 기본(all_success)로 두면 막힐 수 있어
      => Join에 trigger_rule을 조정해 "스킵이 있어도 진행"하도록 해야 함
         (예: none_failed_min_one_success)
    """

    def choose_path(**_context):
        # 샘플: 항상 A 경로 선택 (원하면 조건 로직으로 바꿔도 됨)
        return "path_a"

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_path,
    )

    path_a = EmptyOperator(task_id="path_a")
    path_b = EmptyOperator(task_id="path_b")

    # 분기 후 합류 시 "스킵이 있어도" 진행되도록 trigger_rule 설정
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end")

    branch >> [path_a, path_b]
    [path_a, path_b] >> join >> end


conditional_branch_join()
