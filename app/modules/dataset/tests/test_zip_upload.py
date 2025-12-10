"""
Unit tests for ZIP upload functionality in dataset module.
These tests verify the feature to upload single .mmd files and ZIP archives
containing multiple Mermaid diagram files.
"""

from io import BytesIO
from zipfile import ZipFile

import pytest


@pytest.fixture(scope="function")
def authenticated_client():
    """Fixture that provides a fresh authenticated test client for each test"""
    from app import create_app, db
    from app.modules.auth.models import User

    # Create fresh test app and database for each test
    app = create_app("testing")

    with app.app_context():
        # Create fresh database
        db.drop_all()
        db.create_all()

        # Create test user
        user = User(email="test@example.com", password="test1234")
        db.session.add(user)
        db.session.commit()

    # Create test client and login
    with app.test_client() as client:
        # Login
        login_response = client.post(
            "/login",
            data={"email": "test@example.com", "password": "test1234"},
            follow_redirects=True,
        )
        assert login_response.status_code == 200

        yield client

        # Cleanup
        with app.app_context():
            db.session.remove()
            db.drop_all()


class TestUploadSingleMMDFile:
    """Test cases for uploading single .mmd files"""

    def test_upload_valid_mmd_file(self, authenticated_client):
        """Test uploading a valid .mmd file succeeds"""
        mmd_content = b"flowchart TD\n    A-->B"
        data = {"file": (BytesIO(mmd_content), "diagram.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data
        assert json_data["filename"].endswith(".mmd")

    def test_upload_mmd_without_diagram(self, authenticated_client):
        """Test uploading .mmd file without diagram returns 400"""
        mmd_content = b"This is just plain text"
        data = {"file": (BytesIO(mmd_content), "invalid.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "No Mermaid diagram detected" in json_data["message"]

    def test_upload_mmd_with_multiple_diagrams(self, authenticated_client):
        """Test uploading .mmd file with multiple diagrams returns 400"""
        mmd_content = b"flowchart TD\n    A-->B\n\nsequenceDiagram\n    Alice->>Bob: Hello"
        data = {"file": (BytesIO(mmd_content), "multiple.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "Multiple Mermaid diagrams detected" in json_data["message"]


class TestUploadZipFile:
    """Test cases for uploading ZIP files with multiple diagrams"""

    def test_upload_zip_with_valid_files(self, authenticated_client):
        """Test uploading ZIP with valid .mmd files succeeds"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("diagram1.mmd", "flowchart TD\n    A-->B")
            zf.writestr("diagram2.mmd", "sequenceDiagram\n    Alice->>Bob: Hi")
            zf.writestr("subfolder/diagram3.mmd", "classDiagram\n    Animal <|-- Duck")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "diagrams.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filenames" in json_data
        assert len(json_data["filenames"]) == 3
        assert all(f.endswith(".mmd") for f in json_data["filenames"])

    def test_upload_zip_with_mixed_files(self, authenticated_client):
        """Test uploading ZIP with valid and invalid .mmd files"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("valid.mmd", "flowchart TD\n    A-->B")
            zf.writestr("invalid.mmd", "Just plain text")
            zf.writestr("readme.txt", "This is not a diagram")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "mixed.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filenames" in json_data
        assert len(json_data["filenames"]) == 1  # Only valid file
        assert "rejected" in json_data
        assert len(json_data["rejected"]) >= 1

    def test_upload_zip_without_valid_files(self, authenticated_client):
        """Test uploading ZIP with no valid files returns 400"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("invalid1.mmd", "No diagram here")
            zf.writestr("invalid2.mmd", "Still no diagram")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "empty.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "No valid Mermaid files in ZIP" in json_data["message"]
        assert "rejected" in json_data

    def test_upload_zip_with_path_traversal(self, authenticated_client):
        """Test ZIP security: path traversal attempts are rejected"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("../../../etc/passwd.mmd", "flowchart TD\n    A-->B")
            zf.writestr("valid.mmd", "flowchart TD\n    X-->Y")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "malicious.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        # Should accept only the valid file
        assert len(json_data["filenames"]) == 1
        assert "valid" in json_data["filenames"][0]
        assert json_data["filenames"][0].endswith(".mmd")
        # Should reject the malicious path
        assert any("Security" in r.get("reason", "") for r in json_data["rejected"])

    def test_upload_zip_duplicate_filenames(self, authenticated_client):
        """Test ZIP upload handles duplicate filenames with renaming"""
        # First upload
        zip_buffer1 = BytesIO()
        with ZipFile(zip_buffer1, "w") as zf:
            zf.writestr("diagram.mmd", "flowchart TD\n    A-->B")
        zip_buffer1.seek(0)

        response1 = authenticated_client.post(
            "/dataset/file/upload",
            data={"file": (zip_buffer1, "first.zip")},
            content_type="multipart/form-data",
        )
        assert response1.status_code == 200

        # Second upload with same filename
        zip_buffer2 = BytesIO()
        with ZipFile(zip_buffer2, "w") as zf:
            zf.writestr("diagram.mmd", "sequenceDiagram\n    Alice->>Bob: Hi")
        zip_buffer2.seek(0)

        response2 = authenticated_client.post(
            "/dataset/file/upload",
            data={"file": (zip_buffer2, "second.zip")},
            content_type="multipart/form-data",
        )

        assert response2.status_code == 200
        json_data = response2.get_json()
        assert len(json_data["filenames"]) == 1
        filename = json_data["filenames"][0]
        assert "diagram" in filename
        assert filename.endswith(".mmd")


class TestUploadFileValidation:
    """Test cases for file type and extension validation"""

    def test_upload_invalid_extension(self, authenticated_client):
        """Test uploading file with invalid extension returns 400"""
        data = {"file": (BytesIO(b"content"), "file.txt")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "must be .mmd or .zip" in json_data["message"]

    def test_upload_no_file(self, authenticated_client):
        """Test uploading with no file returns 400"""
        data = {}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
