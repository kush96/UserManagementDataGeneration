import time
from locust import HttpUser, task, between

class WebsiteUser(HttpUser):
        wait_time = between(1,5)

        @task
        def postgres_scope_api(self):
                self.client.get(url="/postgres/scope")

        @task
        def mongo_scope_api(self):
                self.client.get(url="/mongo/scope")

