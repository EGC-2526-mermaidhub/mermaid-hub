import io
import zipfile

from locust import HttpUser, TaskSet, task

from core.environment.host import get_host_for_locust_testing
from core.locust.common import fake, get_csrf_token


class DatasetBehavior(TaskSet):
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
    def view_upload_page(self):
        response = self.client.get("/dataset/upload")
        if response.status_code != 200:
            print(f"Failed to load upload page: {response.status_code}")

    @task(2)
    def upload_single_mmd_file(self):
        mmd_content = f"flowchart TD\n    A[Start {fake.word()}]-->B[End {fake.word()}]"
        filename = f"diagram_{fake.uuid4()[:8]}.mmd"

        files = {"file": (filename, io.BytesIO(mmd_content.encode()), "application/octet-stream")}

        with self.client.post(
            "/dataset/file/upload",
            files=files,
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Upload failed with status {response.status_code}")

    @task(2)
    def upload_zip_with_multiple_diagrams(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for i in range(3):
                content = f"flowchart TD\n    A{i}[Node {fake.word()}]-->B{i}[Node {fake.word()}]"
                zf.writestr(f"diagram_{i}_{fake.uuid4()[:8]}.mmd", content)

        zip_buffer.seek(0)
        filename = f"diagrams_{fake.uuid4()[:8]}.zip"

        files = {"file": (filename, zip_buffer, "application/zip")}

        with self.client.post(
            "/dataset/file/upload",
            files=files,
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                json_data = response.json()
                if "filenames" in json_data and len(json_data["filenames"]) == 3:
                    response.success()
                else:
                    response.failure(f"ZIP upload returned unexpected data: {json_data}")
            else:
                response.failure(f"ZIP upload failed with status {response.status_code}")

    @task(1)
    def upload_zip_with_mixed_files(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("valid.mmd", "sequenceDiagram\n    Alice->>Bob: Hello")
            zf.writestr("invalid.mmd", "This is not a valid diagram")
            zf.writestr("readme.txt", "This file should be ignored")

        zip_buffer.seek(0)
        filename = f"mixed_{fake.uuid4()[:8]}.zip"

        files = {"file": (filename, zip_buffer, "application/zip")}

        with self.client.post(
            "/dataset/file/upload",
            files=files,
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                json_data = response.json()
                if "filenames" in json_data and "rejected" in json_data:
                    response.success()
                else:
                    response.failure(f"Mixed ZIP upload returned unexpected data: {json_data}")
            else:
                response.failure(f"Mixed ZIP upload failed with status {response.status_code}")

    @task(1)
    def upload_invalid_file_type(self):
        files = {"file": ("document.txt", io.BytesIO(b"plain text content"), "text/plain")}

        with self.client.post(
            "/dataset/file/upload",
            files=files,
            catch_response=True,
        ) as response:
            if response.status_code == 400:
                response.success()
            else:
                response.failure(f"Invalid file type should return 400, got {response.status_code}")

    @task(1)
    def upload_mmd_without_diagram(self):
        files = {"file": ("nodgragram.mmd", io.BytesIO(b"Just plain text"), "application/octet-stream")}

        with self.client.post(
            "/dataset/file/upload",
            files=files,
            catch_response=True,
        ) as response:
            if response.status_code == 400:
                response.success()
            else:
                response.failure(f"Invalid MMD should return 400, got {response.status_code}")

    @task(1)
    def view_dataset_list(self):
        response = self.client.get("/dataset/list")
        if response.status_code != 200:
            print(f"Failed to load dataset list: {response.status_code}")


class DatasetUser(HttpUser):
    tasks = [DatasetBehavior]
    min_wait = 5000
    max_wait = 9000
    host = get_host_for_locust_testing()
