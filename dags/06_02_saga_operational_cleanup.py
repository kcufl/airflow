from __future__ import annotations

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="06_02_saga_operational_cleanup_fixed",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "06-saga", "ops"],
)
def saga_operational_cleanup_fixed():
    """
    [샘플 목적]
    - 운영형 Saga 패턴(정방향 + 실패 시 보상 + 항상 실행되는 cleanup)

    [핵심 포인트]
    - TaskFlow 함수 호출은 "한 번만" 해서 태스크 핸들을 변수로 잡고 연결한다.
    - forward_3 실패 시: compensate_2 -> compensate_1 역순 보상
    - cleanup_notify는 성공/실패 무관(all_done)하게 항상 실행
    """

    start = EmptyOperator(task_id="start")

    @task
    def forward_1() -> str:
        print("[forward_1] create temp resource / stage")
        return "created"

    @task
    def forward_2() -> str:
        print("[forward_2] apply change / write intermediate")
        return "applied"

    @task
    def forward_3() -> None:
        print("[forward_3] finalize (intentional fail)")
        raise RuntimeError("finalize failed")

    # 실패 시에만 실행되도록(one_failed)
    @task(trigger_rule="one_failed")
    def compensate_2(state_2: str) -> str:
        if state_2 == "applied":
            print("[compensate_2] revert step 2")
            return "rolled_back_2"
        print("[compensate_2] skip")
        return "noop_2"

    # compensate_1은 compensate_2 다음에 실행되게 "체인으로 강제"하는 게 목적이라
    # trigger_rule을 기본(all_success)로 두고, c2 결과를 입력으로 받아 의존성을 고정한다.
    @task
    def compensate_1(state_1: str, _c2_result: str) -> str:
        if state_1 == "created":
            print("[compensate_1] delete temp resource from step 1")
            return "rolled_back_1"
        print("[compensate_1] skip")
        return "noop_1"

    @task(trigger_rule="all_done")
    def cleanup_notify(_c1: str | None = None, _c2: str | None = None) -> None:
        print("[cleanup] always runs")
        print("[cleanup] c2:", _c2, "c1:", _c1)
        print("[cleanup] send notification / emit metrics / finalize logs")

    # ---- DAG wiring (중요: 호출은 1번만!) ----
    f1 = forward_1()
    f2 = forward_2()
    f3 = forward_3()

    start >> f1 >> f2 >> f3

    # 실패 트리거는 f3 -> c2로 명시
    c2 = compensate_2(f2)
    f3 >> c2

    # 보상 역순 체인: c2 -> c1
    c1 = compensate_1(f1, c2)
    c2 >> c1

    # cleanup은 항상 실행되며, forward_3 / 보상 결과를 모두 받아 로깅/알림 가능
    # (인자로 받으면 의존성이 확실해지고 UI도 깔끔해짐)
    cleanup_notify(c1, c2)


saga_operational_cleanup_fixed()
