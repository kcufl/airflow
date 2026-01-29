from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task

@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def trigger_flow():

    trigger = TriggerDagRunOperator(
        task_id="trigger_next",
        trigger_dag_id="target_dag",
        conf={"source": "event"},
    )

    trigger

trigger_flow()
