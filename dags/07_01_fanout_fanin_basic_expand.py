from __future__ import annotations

from datetime import datetime
from typing import List

from airflow.decorators import dag, task


@dag(
    dag_id="07_01_fanout_fanin_basic_expand",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "07-fanout-fanin", "basic"],
)
def fanout_fanin_basic_expand():
    """
    [샘플 목적]
    - Fan-out / Fan-in의 기본 패턴:
      1) 처리 대상 목록(items) 생성
      2) 동적 병렬 처리(process)로 Fan-out
      3) 모든 결과를 merge로 Fan-in

    [핵심 포인트]
    - process.expand(item=items): items 길이만큼 task 인스턴스를 동적으로 생성
    - merge는 mapped task들의 결과(list)를 받아 집계/후처리
    """

    @task
    def generate_items() -> List[str]:
        items = ["A", "B", "C", "D"]
        print("[generate_items] items:", items)
        return items

    @task
    def process(item: str) -> str:
        print(f"[process] working on {item}")
        # 샘플 결과 반환
        return f"processed-{item}"

    @task
    def merge(results: List[str]) -> None:
        print("[merge] got results:", results)
        print("[merge] done ✅")

    items = generate_items()
    results = process.expand(item=items)
    merge(results)


fanout_fanin_basic_expand()
