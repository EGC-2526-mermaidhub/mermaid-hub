"""
Integration tests for ZIP upload feature.

These tests validate the complete workflow of uploading ZIP files containing
Mermaid diagrams, from file submission to database storage and retrieval.
"""

import io
import json
import os
import tempfile
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from app import db
from app.modules.auth.models import User


@pytest.fixture
def authenticated_user(test_client):
    """Create and authenticate a test user."""
    user = User.query.filter_by(email="test@example.com").first()
    if not user:
        user = User(email="test@example.com", password="test1234")
        db.session.add(user)
        db.session.commit()
    return user


@pytest.fixture
def temp_upload_dir():
    """Create a temporary directory for uploads."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


class TestZipUploadIntegration:
    """Integration tests for ZIP upload workflow."""

    def test_upload_and_retrieve_zip_files(self, test_client, authenticated_user, temp_upload_dir):
        """
        Test complete workflow: upload ZIP file, verify it's processed,
        and retrieve the extracted files.
        """
        response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
        assert response.status_code == 200

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("diagram1.mmd", "graph TD\n  A-->B\n  B-->C")
            zip_file.writestr("diagram2.mmd", "sequenceDiagram\n  A->>B: Hello")
            zip_file.writestr("readme.txt", "This is a readme")

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "diagrams.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "filenames" in data
        assert len(data["filenames"]) == 2
        assert "diagram1.mmd" in data["filenames"]
        assert "diagram2.mmd" in data["filenames"]
        assert os.path.exists(os.path.join(temp_upload_dir, "diagram1.mmd"))
        assert os.path.exists(os.path.join(temp_upload_dir, "diagram2.mmd"))
        assert not os.path.exists(os.path.join(temp_upload_dir, "readme.txt"))

    def test_upload_zip_with_nested_directories(self, test_client, authenticated_user, temp_upload_dir):
        """
        Test uploading a ZIP file with nested directories.
        Files in subdirectories should be extracted at root level.
        """
        # Login
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        # Create a ZIP file with nested structure
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("diagrams/flowchart.mmd", "graph LR\n  A[Start]-->B[End]")
            zip_file.writestr("other/sequence.mmd", "sequenceDiagram\n  Alice->>John: Hello")
            zip_file.writestr("diagram.mmd", "pie title Browser usage\n  Chrome: 45")

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "nested_diagrams.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["filenames"]) >= 3
        assert any("flowchart" in f for f in data["filenames"])
        assert any("sequence" in f for f in data["filenames"])
        assert any("diagram" in f for f in data["filenames"])

    def test_upload_large_zip_file(self, test_client, authenticated_user, temp_upload_dir):
        """Test uploading a larger ZIP file with multiple diagrams."""
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        zip_buffer = io.BytesIO()
        expected_files = []

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for i in range(10):
                filename = f"diagram_{i:02d}.mmd"
                content = f"graph TD\n  A[Diagram {i}]-->B[End {i}]"
                zip_file.writestr(filename, content)
                expected_files.append(filename)

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "many_diagrams.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["filenames"]) == 10
        for filename in expected_files:
            assert filename in data["filenames"]

    def test_upload_zip_then_create_dataset(self, test_client, authenticated_user, temp_upload_dir):
        """
        Test the full workflow: upload ZIP, extract files, then create a dataset
        using the extracted files.
        """
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("workflow.mmd", "graph LR\n  A[Start]-->B[Process]-->C[End]")
            zip_file.writestr("timeline.mmd", "timeline\n  title My Timeline\n  2024 : Event A")

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "workflow_diagrams.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["filenames"]) == 2

        extracted_files = data["filenames"]
        for filename in extracted_files:
            file_path = os.path.join(temp_upload_dir, filename)
            assert os.path.exists(file_path)
            with open(file_path, "r") as f:
                content = f.read()
                assert len(content) > 0
                assert "graph" in content or "timeline" in content

    def test_mixed_upload_mmd_and_zip_in_sequence(self, test_client, authenticated_user, temp_upload_dir):
        """
        Test uploading both .mmd files and ZIP files in sequence to verify
        they work together properly.
        """
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response1 = test_client.post(
                "/dataset/file/upload",
                data={"file": (io.BytesIO(b"graph TD\n  A-->B"), "first.mmd")},
                content_type="multipart/form-data",
            )

        assert response1.status_code == 200
        data1 = json.loads(response1.data)
        assert "first.mmd" in data1.get("filenames", data1.get("filename", ""))

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("second.mmd", "graph LR\n  X-->Y")
            zip_file.writestr("third.mmd", "pie\n  Chrome: 80")

        zip_buffer.seek(0)

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response2 = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "bundle.zip")}, content_type="multipart/form-data"
            )

        assert response2.status_code == 200
        data2 = json.loads(response2.data)
        assert len(data2["filenames"]) == 2
        assert os.path.exists(os.path.join(temp_upload_dir, "first.mmd"))
        assert os.path.exists(os.path.join(temp_upload_dir, "second.mmd"))
        assert os.path.exists(os.path.join(temp_upload_dir, "third.mmd"))

    def test_upload_zip_with_duplicate_handling(self, test_client, authenticated_user, temp_upload_dir):
        """
        Test that uploading a ZIP file with files that already exist
        results in proper duplicate handling (renaming).
        """
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response1 = test_client.post(
                "/dataset/file/upload",
                data={"file": (io.BytesIO(b"graph TD\n  A-->B"), "diagram.mmd")},
                content_type="multipart/form-data",
            )

        assert response1.status_code == 200

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("diagram.mmd", "graph LR\n  X-->Y")

        zip_buffer.seek(0)

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response2 = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "bundle.zip")}, content_type="multipart/form-data"
            )

        assert response2.status_code == 200
        data2 = json.loads(response2.data)
        assert len(data2["filenames"]) == 1
        returned_filename = data2["filenames"][0]
        assert os.path.exists(os.path.join(temp_upload_dir, returned_filename))
        assert os.path.exists(os.path.join(temp_upload_dir, "diagram.mmd"))

    def test_upload_empty_zip_file(self, test_client, authenticated_user, temp_upload_dir):
        """Test uploading an empty ZIP file."""
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED):
            pass

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "empty.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "No valid Mermaid files" in data.get("message", "")

    def test_upload_zip_preserves_file_content(self, test_client, authenticated_user, temp_upload_dir):
        """
        Test that extracted files from ZIP preserve their original content.
        """
        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        original_content = "graph TD\n  A[Start]-->B[Middle]-->C[End]\n  B-->D[Alternative]"

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("content_test.mmd", original_content)

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "content_test.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "content_test.mmd" in data["filenames"]
        extracted_file = os.path.join(temp_upload_dir, "content_test.mmd")
        with open(extracted_file, "r") as f:
            extracted_content = f.read()
        assert extracted_content == original_content
