from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="sample_hello",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sample"],
)

hello = BashOperator(
    task_id="hello",
    bash_command="echo 'hello airflow 3 on k8s!' && date",
    dag=dag,
)

bye = BashOperator(
    task_id="bye",
    bash_command="echo 'bye!'",
    dag=dag,
)

hello >> bye