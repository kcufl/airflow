@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def compensation_flow():

    @task
    def main_task():
        raise Exception("fail")

    @task(trigger_rule="one_failed")
    def compensate():
        print("rollback / cleanup")

    main_task() >> compensate()

compensation_flow()