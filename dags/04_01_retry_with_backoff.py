from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="04_01_retry_with_backoff",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flow-sample", "04-retry"],
)
def retry_with_backoff():
    """
    [샘플 목적]
    - 일시적 실패(네트워크/RateLimit 등)에 대비한 재시도 패턴
    - Airflow의 retry 설정을 "운영형"으로 보여줌

    [핵심 포인트]
    - retries: 총 재시도 횟수(실행 1회 + 재시도 N회)
    - retry_delay: 기본 재시도 간격
    - retry_exponential_backoff=True: 재시도 간격이 점점 증가(지수 백오프)
    - max_retry_delay: 재시도 간격 최대값 제한
    """

    @task(
        retries=4,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )
    def unstable_task():
        # 샘플: 항상 실패하도록 만들어 재시도 동작 확인
        # 실제로는 외부 API/DB 연결 같은 곳에서 일시 오류가 나면 이 패턴이 유효
        raise RuntimeError("temporary failure (intentional)")

    @task(trigger_rule="all_done")
    def after():
        # 실패하더라도 후속 정리/알림이 필요할 때 사용
        print("[after] runs even if unstable_task finally fails")
        print("[after] notify/cleanup/report etc")

    unstable_task() >> after()


retry_with_backoff()
