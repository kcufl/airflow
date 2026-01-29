@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def saga_flow():

    @task
    def step_1():
        print("step1")

    @task
    def step_2():
        raise Exception("fail at step2")

    @task(trigger_rule="one_failed")
    def compensate_1():
        print("compensate step1")

    s1 = step_1()
    s2 = step_2()

    s1 >> s2
    s2 >> compensate_1()

saga_flow()