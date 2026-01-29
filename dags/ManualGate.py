from airflow.sensors.external_task import ExternalTaskSensor
from airflow.decorators import dag, task

@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def manual_gate_flow():

    wait_for_approval = ExternalTaskSensor(
        task_id="wait_for_approval",
        external_dag_id="approval_dag",
        external_task_id="approve",
        mode="reschedule",
    )

    @task
    def after_approval():
        print("approved, continue")

    wait_for_approval >> after_approval()

manual_gate_flow()