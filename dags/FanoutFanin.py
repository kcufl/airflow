@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def fanout_fanin_flow():

    @task
    def generate_items():
        return ["a", "b", "c"]

    @task
    def process(item):
        print(f"process {item}")
        return item

    @task
    def merge(results):
        print("merged:", results)

    items = generate_items()
    results = process.expand(item=items)
    merge(results)

fanout_fanin_flow()