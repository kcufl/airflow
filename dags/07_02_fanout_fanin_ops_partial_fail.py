from __future__ import annotations

from datetime import datetime
from typing import Dict, List

from airflow.decorators import dag, task


@dag(
    dag_id="07_02_fanout_fanin_ops_partial_fail",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "07-fanout-fanin", "ops", "partial-fail"],
)
def fanout_fanin_ops_partial_fail():
    """
    [샘플 목적]
    - 운영형 Fan-out/Fan-in:
      "부분 실패를 허용"하면서도,
      실패 목록을 Fan-in 단계에서 집계해 운영 조치(알림/재처리)로 연결하는 패턴

    [핵심 포인트]
    - process는 실패 시 예외로 DAG 전체를 깨지 않고,
      결과를 {ok: False, message: "..."} 형태로 반환
      => Fan-in에서 성공/실패를 한 번에 집계 가능
    - cleanup은 trigger_rule="all_done"으로 항상 실행
    """

    @task
    def generate_items() -> List[str]:
        # 샘플: 일부 항목은 실패하도록 구성
        return ["ok-1", "ok-2", "fail-1", "ok-3", "fail-2"]

    @task
    def process(item: str) -> Dict[str, str]:
        # 실패를 예외로 던지지 않고, 결과로 표현하는 운영형 패턴
        if item.startswith("fail"):
            return {"item": item, "ok": "false", "message": "simulated failure"}
        return {"item": item, "ok": "true", "message": "success"}

    @task
    def aggregate(results: List[Dict[str, str]]) -> Dict[str, List[str]]:
        ok_items = [r["item"] for r in results if r.get("ok") == "true"]
        fail_items = [r["item"] for r in results if r.get("ok") != "true"]

        summary = {"ok": ok_items, "fail": fail_items}

        print("[aggregate] ok:", ok_items)
        print("[aggregate] fail:", fail_items)
        print("[aggregate] summary:", summary)

        return summary

    @task(trigger_rule="all_done")
    def cleanup(summary: Dict[str, List[str]] | None = None) -> None:
        print("[cleanup] always runs")
        if summary:
            print("[cleanup] summary:", summary)
        print("[cleanup] notify / metrics / housekeeping")

    items = generate_items()
    results = process.expand(item=items)
    summary = aggregate(results)
    cleanup(summary)


fanout_fanin_ops_partial_fail()
