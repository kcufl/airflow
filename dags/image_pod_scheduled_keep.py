from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="image_pod_scheduled_keep",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",   # 매 10분
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["sample", "k8s", "image", "schedule"],
) as dag:

    run = KubernetesPodOperator(
        task_id="run_in_image_and_keep_pod_5m",
        name="image-pod-keep-5m",
        namespace="airflow",
        image="python:3.12-slim",
        cmds=["bash", "-lc"],
        arguments=[
            # 실제 작업
            "python - <<'PY'\n"
            "import time\n"
            "print('hello from image')\n"
            "print('do some work...')\n"
            "PY\n"
            # ✅ 작업 후 5분 유지(디버깅/로그 확인용)
            "echo 'keeping pod alive for 300s...' && sleep 300\n"
            "echo 'done'\n"
        ],
        get_logs=True,
        is_delete_operator_pod=True,  # ✅ 종료되면 삭제(하지만 위 sleep 때문에 5분 뒤에 종료)
    )