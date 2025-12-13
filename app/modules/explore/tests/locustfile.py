import random

from locust import HttpUser, TaskSet, task

from core.environment.host import get_host_for_locust_testing
from core.locust.common import get_csrf_token


class ExploreBehavior(TaskSet):
    def on_start(self):
        self.ensure_logged_out()
        self.login()

    def ensure_logged_out(self):
        self.client.get("/logout")

    def login(self):
        response = self.client.get("/login")
        csrf_token = get_csrf_token(response)
        self.client.post(
            "/login",
            data={"email": "user1@example.com", "password": "1234", "csrf_token": csrf_token},
        )

    @task(3)
    def view_explore_page(self):
        response = self.client.get("/explore")
        if response.status_code != 200:
            print(f"Failed to load explore page: {response.status_code}")

    @task(4)
    def filter_datasets(self):
        response = self.client.get("/explore")
        if response.status_code != 200:
            print(f"Failed to load explore page for POST: {response.status_code}")
            return

        csrf_token = get_csrf_token(response)

        criteria = {
            "query": random.choice(["genomics", "protein", "climate", "mermaid"]),
            "sorting": random.choice(["newest", "oldest", "trending_week", "trending_month", "trending_all_time"]),
            "diagram_type": random.choice(["any", "flowchart", "sequence"]),
            "tags": random.sample(["biology", "chemistry", "physics", "ai"], k=random.randint(0, 2)),
            "csrf_token": csrf_token,
        }

        with self.client.post("/explore", json=criteria, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Filter datasets failed with status {response.status_code}")


class ExploreUser(HttpUser):
    tasks = [ExploreBehavior]
    min_wait = 5000
    max_wait = 9000
    host = get_host_for_locust_testing()
