from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def sequential_flow():

    @task
    def step_a():
        print("A")

    @task
    def step_b():
        print("B")

    @task
    def step_c():
        print("C")

    step_a() >> step_b() >> step_c()

sequential_flow()
