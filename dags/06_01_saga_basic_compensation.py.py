from __future__ import annotations

from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="06_01_saga_basic_compensation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "06-saga", "basic"],
)
def saga_basic_compensation():
    """
    [샘플 목적]
    - Saga의 핵심인 "정방향 단계(forward) + 실패 시 보상(compensation)"을 DAG로 구현
    - Airflow는 자동 롤백이 없으므로, 보상 단계를 명시적으로 설계해야 함

    [흐름 개념]
    forward_1 -> forward_2 -> forward_3
                      (실패 가능)
    실패 시:
      compensate_2 (forward_2의 보상)
      compensate_1 (forward_1의 보상)

    [핵심 포인트]
    - forward 단계가 성공적으로 끝날 때마다 "체크포인트/플래그"를 XCom으로 남김
    - 보상 단계는 그 플래그를 보고 "실제로 되돌릴 게 있으면" 수행
    - 보상은 보통 역순으로 실행 (2를 되돌리고 1을 되돌림)
    """

    @task
    def forward_1() -> str:
        print("[forward_1] do step 1")
        # 성공하면 "완료 표시"를 반환 (보상에서 참고)
        return "done"

    @task
    def forward_2() -> str:
        print("[forward_2] do step 2")
        return "done"

    @task
    def forward_3() -> str:
        print("[forward_3] do step 3 (intentional fail)")
        # 샘플: 일부러 실패 발생
        raise RuntimeError("fail at forward_3")

    @task(trigger_rule="one_failed")
    def compensate_2(f2_state: str):
        if f2_state == "done":
            print("[compensate_2] rollback step 2")
        else:
            print("[compensate_2] nothing to rollback")

    @task(trigger_rule="one_failed")
    def compensate_1(f1_state: str):
        if f1_state == "done":
            print("[compensate_1] rollback step 1")
        else:
            print("[compensate_1] nothing to rollback")

    f1 = forward_1()
    f2 = forward_2()
    f3 = forward_3()

    f1 >> f2 >> f3

    # forward_3가 실패하면(one_failed) 보상 실행
    f3 >> compensate_2(f2)
    compensate_2(f2) >> compensate_1(f1)


saga_basic_compensation()
