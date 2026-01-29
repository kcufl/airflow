from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="image_pod_sample",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sample", "image"],
) as dag:

    run = KubernetesPodOperator(
        task_id="run_in_image",
        name="image-pod-sample",
        namespace="airflow",
        image="python:3.12-slim",  # ✅ 여기 이미지 바꾸면 됨
        cmds=["bash", "-lc"],
        arguments=[
            "python -V && "
            "python - <<'PY'\n"
            "import os, platform\n"
            "print('hello from image')\n"
            "print('platform:', platform.platform())\n"
            "print('env AIRFLOW_CTX_DAG_ID:', os.getenv('AIRFLOW_CTX_DAG_ID'))\n"
            "PY\n"
        ],
        get_logs=True,
        is_delete_operator_pod=True,  # 끝나면 삭제
    )