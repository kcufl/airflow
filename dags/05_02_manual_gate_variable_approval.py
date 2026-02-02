from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.python import PythonSensor
from airflow.models import Variable


@dag(
    dag_id="05_02_manual_gate_variable_approval",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "05-manual-gate", "approval"],
)
def manual_gate_variable_approval():
    """
    [샘플 목적]
    - 사람이 "승인"해야 다음 단계로 넘어가는 Manual Gate 패턴
    - 승인 신호를 Airflow Variable로 구현 (가장 단순한 Human-in-the-loop)

    [동작 방식]
    - Variable 키: approval:run_id (예: approval:manual__2026-... )
    - 운영자가 UI(또는 CLI)에서 값 입력:
        * approve  -> 진행
        * reject   -> 실패 처리(여기서는 예시로 실패 발생)
        * 그 외/없음 -> 계속 대기

    [핵심 포인트]
    - PythonSensor + mode="reschedule": 대기 중 리소스 점유 최소화
    - poke_interval: 몇 초마다 승인 여부 확인할지
    - timeout: 일정 시간 지나도 승인 안 되면 실패 처리
    """

    @task
    def before(**context):
        run_id = context["dag_run"].run_id
        key = f"approval:{run_id}"
        print("[before] approval key:", key)
        print("[before] set Variable to 'approve' or 'reject' to continue")
        return key

    def wait_for_approval(approval_key: str) -> PokeReturnValue:
        val = Variable.get(approval_key, default_var="")
        val = (val or "").strip().lower()

        if val == "approve":
            return PokeReturnValue(is_done=True, xcom_value="approved")
        if val == "reject":
            # reject면 센서 자체를 실패시키는 방식도 가능하지만,
            # 여기서는 센서 완료 후 downstream에서 처리하도록 값을 반환하는 방식으로 둠
            return PokeReturnValue(is_done=True, xcom_value="rejected")

        # 아직 승인값이 없으면 계속 대기
        return PokeReturnValue(is_done=False)

    approval_gate = PythonSensor(
        task_id="approval_gate",
        python_callable=wait_for_approval,
        op_args=["{{ ti.xcom_pull(task_ids='before') }}"],
        mode="reschedule",
        poke_interval=10,   # 10초마다 체크
        timeout=60 * 30,    # 30분 안에 승인 없으면 실패
    )

    @task
    def after(approval_result: str):
        if approval_result == "rejected":
            raise RuntimeError("approval rejected by operator")
        print("[after] approved ✅ continue work")

    key = before()
    gate = approval_gate
    after(gate.output)


manual_gate_variable_approval()
