@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def retry_flow():

    @task(
        retries=3,
        retry_delay=timedelta(seconds=10),
    )
    def unstable_task():
        raise Exception("temporary failure")

    unstable_task()

retry_flow()