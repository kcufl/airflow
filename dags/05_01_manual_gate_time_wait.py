from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.time_delta import TimeDeltaSensor


@dag(
    dag_id="05_01_manual_gate_time_wait",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "05-manual-gate", "time"],
)
def manual_gate_time_wait():
    """
    [샘플 목적]
    - 사람이 누르는 승인 대신, "일정 시간 대기 후 진행"하는 Gate 패턴
    - 운영에서 쿨다운(cooldown), 외부 시스템 반영 대기, 지연 실행 등에 유용

    [핵심 포인트]
    - TimeDeltaSensor로 일정 시간 동안 워크플로우를 대기 상태로 유지
    - mode="reschedule"로 설정하면 센서가 워커/슬롯을 점유하지 않고 효율적으로 대기
    """

    @task
    def before():
        print("[before] do pre-check/report generation etc")

    gate = TimeDeltaSensor(
        task_id="gate_wait_1m",
        delta=timedelta(minutes=1),  # 샘플: 1분 대기
        mode="reschedule",
    )

    @task
    def after():
        print("[after] continue after gate")

    before() >> gate >> after()


manual_gate_time_wait()
