@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def parallel_flow():

    @task
    def start():
        print("start")

    @task
    def task_x(name):
        print(f"run {name}")

    s = start()
    s >> [task_x("B"), task_x("C"), task_x("D")]

parallel_flow()