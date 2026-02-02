from __future__ import annotations

from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="06_01_saga_basic_compensation_fixed",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "06-saga", "basic"],
)
def saga_basic_compensation_fixed():
    """
    [샘플 목적]
    - forward_3 실패 시 보상을 "역순"으로 확실히 실행:
      forward_1 -> forward_2 -> forward_3(실패)
                           ↓
                    compensate_2 -> compensate_1

    [핵심 포인트]
    - TaskFlow 함수는 '한 번만 호출'해서 태스크 핸들을 변수로 잡고,
      그 변수로 의존성을 연결해야 그래프/실행 순서가 의도대로 고정됨.
    - compensate_2는 forward_3 실패 시 실행되도록(one_failed) 구성
    - compensate_1은 compensate_2가 끝난 뒤 실행되도록 체인으로 강제
    """

    @task
    def forward_1() -> str:
        print("[forward_1] do step 1")
        return "done"

    @task
    def forward_2() -> str:
        print("[forward_2] do step 2")
        return "done"

    @task
    def forward_3() -> None:
        print("[forward_3] do step 3 (intentional fail)")
        raise RuntimeError("fail at forward_3")

    @task(trigger_rule="one_failed")
    def compensate_2(f2_state: str) -> str:
        # f2_state는 forward_2가 성공했을 때만 "done"이 들어옴
        if f2_state == "done":
            print("[compensate_2] rollback step 2")
            return "rolled_back_2"
        print("[compensate_2] nothing to rollback")
        return "noop_2"

    # compensate_1은 "compensate_2 완료 후" 실행되도록 trigger_rule을 기본(all_success)로 둠
    # -> compensate_2가 실행되면 성공으로 끝나고, 그 다음 compensate_1이 실행됨
    @task
    def compensate_1(f1_state: str, _c2_result: str) -> None:
        if f1_state == "done":
            print("[compensate_1] rollback step 1")
        else:
            print("[compensate_1] nothing to rollback")

    f1 = forward_1()
    f2 = forward_2()
    f3 = forward_3()

    # forward chain
    f1 >> f2 >> f3

    # compensation chain (guaranteed order)
    c2 = compensate_2(f2)           # c2는 f3 실패 시(one_failed) 실행됨 (업스트림에 f3 연결)
    f3 >> c2                        # "실패 트리거"를 f3에 확실히 연결
    c1 = compensate_1(f1, c2)       # c1은 c2가 끝난 다음 실행(인자로 묶어서 의존성 확정)

    # (선택) 그래프 가독성: c2 >> c1도 추가로 걸어줘도 좋음
    c2 >> c1


saga_basic_compensation_fixed()
