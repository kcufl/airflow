from airflow.operators.branch import BranchPythonOperator

@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def conditional_flow():

    def choose_path(**context):
        return "task_a"

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_path,
    )

    @task
    def task_a():
        print("A path")

    @task
    def task_b():
        print("B path")

    branch >> [task_a(), task_b()]

conditional_flow()