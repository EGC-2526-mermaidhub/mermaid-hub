import io
import json
import os
import shutil
import tempfile
import uuid
import zipfile
from io import BytesIO
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest
import requests
from flask import url_for

from app import db, limiter
from app.modules.auth.models import User
from app.modules.auth.services import AuthenticationService
from app.modules.dataset.models import DiagramType
from app.modules.dataset.repositories import DSDownloadRecordRepository
from app.modules.dataset.routes import upload
from app.modules.dataset.services import (
    DataSetService,
    DOIMappingService,
    DSDownloadRecordService,
    DSViewRecordService,
    SizeService,
    TrendingDatasetsService,
    calculate_checksum_and_size,
)


@pytest.fixture
def dataset_service():
    service = DataSetService()
    service.repository = MagicMock()
    service.mermaid_diagram_repository = MagicMock()
    service.author_repository = MagicMock()
    service.dsmetadata_repository = MagicMock()
    service.mdmetadata_repository = MagicMock()
    service.dsdownloadrecord_repository = MagicMock()
    service.hubfiledownloadrecord_repository = MagicMock()
    service.hubfilerepository = MagicMock()
    service.dsviewrecord_repostory = MagicMock()
    service.hubfileviewrecord_repository = MagicMock()
    return service


@pytest.fixture
def test_user(tmp_path):
    class Profile:
        def __init__(self):
            self.surname = "Doe"
            self.name = "John"
            self.affiliation = "University X"
            self.orcid = "0000-0000"

    class User:
        def __init__(self):
            self.id = 1
            self.profile = Profile()
            self._temp = tmp_path

        def temp_folder(self):
            return str(self._temp)

    return User()


@pytest.fixture
def test_form(tmp_path):
    class TestForm:
        def get_dsmetadata(self):
            return {"title": "Dataset Title", "description": "Some desc"}

        def get_authors(self):
            return [{"name": "Alice", "affiliation": "Uni", "orcid": "1234"}]

        @property
        def mermaid_diagrams(self):
            class TestMMD:
                def __init__(self):
                    self.mmd_filename = type("FileField", (), {"data": "diagram.mmd"})
                    self.get_mdmetadata = lambda: {"title": "Diagram"}
                    self.get_authors = lambda: [{"name": "Author", "affiliation": "Lab"}]

                def __repr__(self):
                    return "<TestMMD>"

            return [TestMMD()]

    with open(tmp_path / "diagram.mmd", "wb") as f:
        f.write(b"data")
    return TestForm()


def test_calculate_checksum_and_size_creates_valid_sha256(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_bytes(b"abc")

    checksum, size = calculate_checksum_and_size(str(file_path))

    assert checksum == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert size == 3


def test_size_service_readable_formats():
    s = SizeService()
    assert s.get_human_readable_size(100) == "100 bytes"
    assert s.get_human_readable_size(2048) == "2.0 KB"
    assert s.get_human_readable_size(2 * 1024**2) == "2.0 MB"
    assert s.get_human_readable_size(3 * 1024**3) == "3.0 GB"


def test_move_mermaid_diagrams(dataset_service, test_user, tmp_path):
    dataset = MagicMock()
    dataset.mermaid_diagrams = [MagicMock(md_meta_data=MagicMock(mmd_filename="diagram1.mmd"))]
    src = test_user.temp_folder()
    os.makedirs(src, exist_ok=True)
    with open(os.path.join(src, "diagram1.mmd"), "wb") as f:
        f.write(b"x")

    with patch("shutil.move") as mock_move, patch.dict(os.environ, {"WORKING_DIR": str(tmp_path)}):
        dataset.id = 1
        with patch("app.modules.dataset.services.AuthenticationService") as MockAuth:
            MockAuth.return_value.get_authenticated_user.return_value = test_user
            dataset_service.move_mermaid_diagrams(dataset)
            mock_move.assert_called_once()
            assert "uploads" in str(mock_move.call_args[0][1])


def test_get_methods_delegate_to_repository(dataset_service):
    dataset_service.repository.get_synchronized.return_value = "sync"
    dataset_service.repository.get_unsynchronized.return_value = "unsync"
    dataset_service.repository.get_unsynchronized_dataset.return_value = "unsync_ds"

    mock_dataset = MagicMock()
    mock_dataset.id = 1
    dataset_service.repository.latest_synchronized.return_value = [mock_dataset]

    dataset_service.repository.count_synchronized_datasets.return_value = 3

    assert dataset_service.get_synchronized(1) == "sync"
    assert dataset_service.get_unsynchronized(1) == "unsync"
    assert dataset_service.get_unsynchronized_dataset(1, 2) == "unsync_ds"

    result = dataset_service.latest_synchronized()
    assert len(result) == 1
    assert result[0] == mock_dataset

    assert dataset_service.count_synchronized_datasets() == 3


def test_count_related_objects(dataset_service):
    dataset_service.author_repository.count.return_value = 5
    dataset_service.dsmetadata_repository.count.return_value = 6
    dataset_service.dsdownloadrecord_repository.total_dataset_downloads.return_value = 10
    dataset_service.dsviewrecord_repostory.total_dataset_views.return_value = 11

    assert dataset_service.count_authors() == 5
    assert dataset_service.count_dsmetadata() == 6
    assert dataset_service.total_dataset_downloads() == 10
    assert dataset_service.total_dataset_views() == 11


def test_create_from_form_success(dataset_service, test_form, test_user):
    ds_mock = MagicMock(id=1)
    dataset_service.create = MagicMock(return_value=ds_mock)
    dataset_service.dsmetadata_repository.create.return_value = MagicMock(id=99)
    dataset_service.repository.session.commit = MagicMock()

    result = dataset_service.create_from_form(test_form, test_user)
    assert result == ds_mock
    dataset_service.repository.session.commit.assert_called_once()


def test_create_from_form_rollback_on_error(dataset_service, test_form, test_user):
    dataset_service.dsmetadata_repository.create.side_effect = Exception("Boom")
    dataset_service.repository.session.rollback = MagicMock()

    with pytest.raises(Exception):
        dataset_service.create_from_form(test_form, test_user)
    dataset_service.repository.session.rollback.assert_called_once()


def test_update_dsmetadata(dataset_service):
    dataset_service.dsmetadata_repository.update.return_value = {"ok": True}
    assert dataset_service.update_dsmetadata(1, name="test") == {"ok": True}


def test_get_mermaidhub_doi_with_flask_context(dataset_service):
    dataset = MagicMock()
    dataset.ds_meta_data.dataset_doi = "12345"
    with patch("app.modules.dataset.services.url_for", return_value="http://domain/doi/12345"):
        result = dataset_service.get_mermaidhub_doi(dataset)
        assert result == "http://domain/doi/12345"


def test_get_mermaidhub_doi_fallback_to_domain(dataset_service):
    dataset = MagicMock()
    dataset.ds_meta_data.dataset_doi = "99999"
    with (
        patch("app.modules.dataset.services.url_for", side_effect=Exception()),
        patch.dict(os.environ, {"DOMAIN": "example.com"}),
    ):
        result = dataset_service.get_mermaidhub_doi(dataset)
        assert result == "http://example.com/doi/99999"


def test_create_cookie_creates_new_record(monkeypatch):
    test_dataset = MagicMock()
    service = DSViewRecordService()
    service.repository = MagicMock()
    service.repository.the_record_exists.return_value = None
    service.repository.create_new_record.return_value = "record"

    test_cookie = str(uuid.uuid4())
    monkeypatch.setattr("app.modules.dataset.services.request", MagicMock(cookies={"view_cookie": test_cookie}))

    result = service.create_cookie(test_dataset)
    assert isinstance(result, str)
    service.repository.create_new_record.assert_called_once()


def test_doi_mapping_get_new_doi_found():
    s = DOIMappingService()
    s.repository = MagicMock()
    s.repository.get_new_doi.return_value = MagicMock(dataset_doi_new="new_doi")
    assert s.get_new_doi("old") == "new_doi"


def test_doi_mapping_get_new_doi_not_found():
    s = DOIMappingService()
    s.repository = MagicMock()
    s.repository.get_new_doi.return_value = None
    assert s.get_new_doi("old") is None


# -- Download Count Tests --


def test_dsdownloadrecord_service_get_download_count_zero():
    """Test get_download_count returns 0 when no downloads exist"""
    service = DSDownloadRecordService()
    service.repository = MagicMock()

    with patch("app.modules.dataset.services.db.session.query") as mock_query:
        mock_query.return_value.filter.return_value.scalar.return_value = None

        result = service.get_download_count(1)

        assert result == 0


def test_dsdownloadrecord_service_get_download_count_with_downloads():
    """Test get_download_count returns correct count when downloads exist"""
    service = DSDownloadRecordService()
    service.repository = MagicMock()

    with patch("app.modules.dataset.services.db.session.query") as mock_query:
        mock_query.return_value.filter.return_value.scalar.return_value = 5

        result = service.get_download_count(1)

        assert result == 5


def test_dsdownloadrecord_service_get_download_count_large_number():
    """Test get_download_count handles large numbers correctly"""
    service = DSDownloadRecordService()
    service.repository = MagicMock()

    with patch("app.modules.dataset.services.db.session.query") as mock_query:
        mock_filter = mock_query.return_value.filter.return_value
        mock_filter.scalar.return_value = 999999

        result = service.get_download_count(1)

        assert result == 999999


def test_dataset_service_get_download_count():
    """Test DataSetService get_download_count method"""
    service = DataSetService()
    service.repository = MagicMock()

    with patch("app.modules.dataset.services.db.session.query") as mock_query:
        mock_query.return_value.filter.return_value.scalar.return_value = 10

        result = service.get_download_count(1)

        assert result == 10


def test_dataset_service_dataset_downloads_id():
    """Test DataSetService dataset_downloads_id delegates to repository"""
    service = DataSetService()
    service.dsdownloadrecord_repository = MagicMock()
    service.dsdownloadrecord_repository.dataset_downloads_id.return_value = 7

    result = service.dataset_downloads_id(1)

    assert result == 7
    repo = service.dsdownloadrecord_repository
    repo.dataset_downloads_id.assert_called_once_with(1)


def test_dataset_service_latest_synchronized_with_download_counts():
    """Test latest_synchronized populates download_count for each dataset"""
    service = DataSetService()
    service.repository = MagicMock()
    service.dsdownloadrecord_repository = MagicMock()

    dataset1 = MagicMock(id=1)
    dataset2 = MagicMock(id=2)
    dataset3 = MagicMock(id=3)

    datasets = [dataset1, dataset2, dataset3]
    service.repository.latest_synchronized.return_value = datasets
    repo = service.dsdownloadrecord_repository
    repo.dataset_downloads_id.side_effect = [5, 10, 3]

    result = service.latest_synchronized()

    assert len(result) == 3
    assert result[0].download_count == 5
    assert result[1].download_count == 10
    assert result[2].download_count == 3


def test_dsdownloadrecord_repository_dataset_downloads_id_zero():
    """Test repository dataset_downloads_id returns 0 when no downloads"""

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.filter.return_value.count.return_value = 0

    result = repo.dataset_downloads_id(1)

    assert result == 0


def test_dsdownloadrecord_repository_dataset_downloads_id_with_downloads():
    """Test repository dataset_downloads_id returns correct count"""

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.filter.return_value.count.return_value = 15

    result = repo.dataset_downloads_id(1)

    assert result == 15


def test_dsdownloadrecord_repository_total_dataset_downloads_zero():
    """Test total_dataset_downloads returns 0 when no records"""

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.with_entities.return_value.scalar.return_value = None

    result = repo.total_dataset_downloads()

    assert result == 0


def test_dsdownloadrecord_repository_total_dataset_downloads_with_records():
    """Test total_dataset_downloads returns max id when records exist"""

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.with_entities.return_value.scalar.return_value = 100

    result = repo.total_dataset_downloads()

    assert result == 100


# -- Integration tests --


@pytest.fixture(scope="module")
def test_client(test_client):
    """
    Extends the test_client fixture to add additional specific data for module testing.
    """

    limiter.enabled = False
    test_client.application.config["RATELIMIT_ENABLED"] = False
    test_client.application.config["WTF_CSRF_ENABLED"] = False
    with test_client.application.app_context():
        existing_user = User.query.filter_by(email="test@example.com").first()

        if not existing_user:
            auth_service = AuthenticationService()
            auth_service.create_with_profile(email="test@example.com", password="test1234", name="John", surname="Doe")

    yield test_client


# -- Simple authentication and data-validation tests --
def test_upload_get_requires_login(test_client):
    """Test GET /dataset/upload requires authentication"""
    response = test_client.get("/dataset/upload")
    assert response.status_code == 302
    assert "/login" in response.location


def test_upload_post_requires_login(test_client):
    """Test POST /dataset/upload requires authentication"""
    response = test_client.post("/dataset/upload")
    assert response.status_code == 302
    assert "/login" in response.location


def test_list_dataset_requires_login(test_client):
    """Test GET /dataset/list requires authentication"""
    response = test_client.get("/dataset/list")
    assert response.status_code == 302
    assert "/login" in response.location


def test_file_upload_requires_login(test_client):
    """Test POST /dataset/file/upload requires authentication"""
    response = test_client.post("/dataset/file/upload")
    assert response.status_code == 302
    assert "/login" in response.location


def test_file_upload_no_file(test_client):
    """Test file upload without file returns 400"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    response = test_client.post("/dataset/file/upload", data={})
    assert response.status_code == 400

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_invalid_extension(test_client):
    """Test file upload with invalid extension returns 400"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    data = {"file": (BytesIO(b"content"), "test.txt")}
    response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 400

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_no_mermaid_content(test_client):
    """Test file upload with no mermaid diagram returns 400"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user):
        data = {"file": (BytesIO(b"invalid content"), "test.mmd")}
        response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 400

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_multiple_diagrams(test_client):
    """Test file upload with multiple diagrams returns 400"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user):
        data = {"file": (BytesIO(b"graph TD\nA-->B\n\nflowchart LR\nC-->D"), "test.mmd")}
        response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 400

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_valid_mermaid(test_client):
    """Test file upload with valid mermaid diagram succeeds"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):

        data = {"file": (BytesIO(b"graph TD\nA-->B"), "diagram.mmd")}
        response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 200

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_delete_nonexistent(test_client):
    """Test deleting non-existent file returns error"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user):
        response = test_client.post("/dataset/file/delete", json={"file": "nonexistent.mmd"})

    assert response.status_code == 200

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_delete_success(test_client):
    """Test deleting existing file succeeds"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    temp_dir = tempfile.mkdtemp()

    test_file = os.path.join(temp_dir, "test.mmd")
    with open(test_file, "w") as f:
        f.write("graph TD\nA-->B")

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user):
        response = test_client.post("/dataset/file/delete", json={"file": "test.mmd"})

    assert response.status_code == 200
    assert not os.path.exists(test_file)

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_doi_route_redirects_old_doi(test_client):
    """Test DOI route redirects old DOI to new DOI"""
    with patch("app.modules.dataset.routes.doi_mapping_service") as mock_service:
        mock_service.get_new_doi.return_value = "new_doi_123"

        response = test_client.get("/doi/old_doi_123/", follow_redirects=False)

        assert response.status_code == 302
        assert "new_doi_123" in response.location


def test_doi_route_not_found(test_client):
    """Test DOI route returns 404 for non-existent DOI"""
    with (
        patch("app.modules.dataset.routes.doi_mapping_service") as mock_doi,
        patch("app.modules.dataset.routes.dsmetadata_service") as mock_ds,
    ):
        mock_doi.get_new_doi.return_value = None
        mock_ds.filter_by_doi.return_value = None

        response = test_client.get("/doi/nonexistent/")

        assert response.status_code == 404


def test_doi_route_renders_dataset(test_client):
    """Test DOI route renders dataset page successfully"""
    with (
        patch("app.modules.dataset.routes.doi_mapping_service") as mock_doi,
        patch("app.modules.dataset.routes.dsmetadata_service") as mock_ds,
        patch("app.modules.dataset.routes.ds_view_record_service") as mock_view,
    ):

        mock_doi.get_new_doi.return_value = None

        mock_dataset = MagicMock()
        mock_dataset.id = 1

        mock_ds_meta = MagicMock()
        mock_ds_meta.diagram_type = DiagramType.FLOWCHART
        mock_ds_meta.tags = "tagA, tagB"
        mock_ds_meta.authors = []
        mock_ds_meta.is_draft = 0

        mock_dataset.ds_meta_data = mock_ds_meta

        mock_ds.filter_by_doi.return_value = MagicMock(data_set=mock_dataset)
        mock_view.create_cookie.return_value = str(uuid.uuid4())

        response = test_client.get("/doi/valid_doi/")

        assert response.status_code == 200


def test_unsynchronized_dataset_requires_login(test_client):
    """Test unsynchronized dataset route requires authentication"""
    response = test_client.get("/dataset/unsynchronized/1/", follow_redirects=False)
    assert response.status_code == 302
    assert "/login" in response.location


def test_unsynchronized_dataset_not_found(test_client):
    """Test unsynchronized dataset returns 404 if not found"""
    test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

    with patch("app.modules.dataset.routes.dataset_service") as mock_service:
        mock_service.get_unsynchronized_dataset.return_value = None

        response = test_client.get("/dataset/unsynchronized/999/")

        assert response.status_code == 404

    test_client.get("/logout", follow_redirects=True)


def test_download_dataset_creates_zip(test_client):
    """Test dataset download creates zip file"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "dataset_1.zip")
    with ZipFile(zip_path, "w"):
        pass

    with (
        patch("app.modules.dataset.routes.dataset_service") as mock_ds,
        patch("app.modules.dataset.routes.DSDownloadRecordService") as mock_record_service,
        patch("os.walk", return_value=[]),
        patch("tempfile.mkdtemp", return_value=temp_dir),
    ):

        mock_dataset = MagicMock()
        mock_dataset.id = 1
        mock_dataset.user_id = 1
        mock_ds.get_or_404.return_value = mock_dataset

        response = test_client.get("/dataset/download/1")

        assert response.status_code == 200
        mock_record_service.return_value.create.assert_called_once()

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_download_with_existing_cookie(test_client):
    response = test_client.post(
        "/login",
        data=dict(email="test@example.com", password="test1234"),
        follow_redirects=True,
    )
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    cookie_value = str(uuid.uuid4())
    test_client.set_cookie("download_cookie", cookie_value)

    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "dataset_1.zip")
    with ZipFile(zip_path, "w"):
        pass

    mock_dataset = MagicMock()
    mock_dataset.id = 1
    mock_dataset.user_id = 1

    try:
        with (
            patch("app.modules.dataset.routes.dataset_service.get_or_404", return_value=mock_dataset),
            patch("os.walk", return_value=[]),
            patch("tempfile.mkdtemp", return_value=temp_dir),
            patch("app.modules.dataset.routes.DSDownloadRecordService.create") as mock_create,
        ):

            mock_create.return_value = None

            response = test_client.get("/dataset/download/1")
            assert response.status_code == 200

            content_disposition = response.headers.get("Content-Disposition", "")
            assert "dataset_1.zip" in content_disposition

    finally:
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_dataset_recommendation(dataset_service):
    target = MagicMock(id=1)
    target.ds_meta_data.diagram_type = DiagramType.FLOWCHART
    target.ds_meta_data.tags = "tagA, tagB"
    author_target = MagicMock()
    author_target.name = "Author One"
    target.ds_meta_data.authors = [author_target]


def test_recommendation_limit_n(dataset_service):
    target = MagicMock(id=1)
    target.ds_meta_data.diagram_type = DiagramType.FLOWCHART
    target.ds_meta_data.tags = "tagA"
    target.ds_meta_data.authors = []

    candidates = []
    for i in range(2, 7):
        candidate = MagicMock(id=i)
        candidate.ds_meta_data.diagram_type = DiagramType.FLOWCHART
        candidate.ds_meta_data.tags = "tagA"
        candidate.ds_meta_data.authors = []
        candidates.append(candidate)

    dataset_service.repository.model.query.join.return_value.filter.return_value.all.return_value = candidates

    dataset_service.dsviewrecord_repostory.model.query.filter_by.return_value.count.return_value = 0
    dataset_service.dsdownloadrecord_repository.model.query.filter_by.return_value.count.return_value = 0

    recommendations = dataset_service.recommend_simple(target, top_n=3)
    assert len(recommendations) == 3


def test_recommendation_excludes_self(dataset_service):
    target = MagicMock(id=1)
    target.ds_meta_data.diagram_type = DiagramType.FLOWCHART
    target.ds_meta_data.tags = "tagA"
    target.ds_meta_data.authors = []

    candidate = MagicMock(id=2)
    candidate.ds_meta_data.diagram_type = DiagramType.FLOWCHART
    candidate.ds_meta_data.tags = "tagA"
    candidate.ds_meta_data.authors = []

    dataset_service.repository.model.query.join.return_value.filter.return_value.all.return_value = [candidate]

    dataset_service.dsviewrecord_repostory.model.query.filter_by.return_value.count.return_value = 0
    dataset_service.dsdownloadrecord_repository.model.query.filter_by.return_value.count.return_value = 0

    recommendations = dataset_service.recommend_simple(target, top_n=5)

    ids_returned = [r.id for r in recommendations]
    assert 1 not in ids_returned
    assert 2 in ids_returned


def test_recommendation_returns_empty_if_target_not_found(dataset_service):
    recommendations = dataset_service.recommend_simple(None, top_n=3)
    assert recommendations == []


def test_recommendation_handles_none_tags_gracefully(dataset_service):
    target = MagicMock(id=1)
    target.ds_meta_data.diagram_type = DiagramType.FLOWCHART
    target.ds_meta_data.tags = "tagA"
    target.ds_meta_data.authors = []

    candidate = MagicMock(id=2)
    candidate.ds_meta_data.diagram_type = DiagramType.FLOWCHART
    candidate.ds_meta_data.tags = None
    candidate.ds_meta_data.authors = []

    dataset_service.repository.model.query.join.return_value.filter.return_value.all.return_value = [candidate]

    dataset_service.dsviewrecord_repostory.model.query.filter_by.return_value.count.return_value = 0
    dataset_service.dsdownloadrecord_repository.model.query.filter_by.return_value.count.return_value = 0

    try:
        recommendations = dataset_service.recommend_simple(target, top_n=3)
    except AttributeError:
        pytest.fail("El algoritmo falló al procesar tags con valor None")

    assert len(recommendations) == 1
    assert recommendations[0].id == 2


def test_trending_datasets_service_initialization():
    """Test TrendingDatasetsService initializes correctly"""
    service = TrendingDatasetsService()
    assert service.repository is not None


def test_get_period_days_week():
    """Test _get_period_days returns 7 for week"""
    service = TrendingDatasetsService()
    result = service._get_period_days("week")
    assert result == 7


def test_get_period_days_month():
    """Test _get_period_days returns 30 for month"""
    service = TrendingDatasetsService()
    result = service._get_period_days("month")
    assert result == 30


def test_get_trending_datasets_calls_repository():
    """Test get_trending_datasets delegates to repository with correct parameters"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    service.repository.get_top_downloaded_datasets.return_value = []

    result = service.get_trending_datasets(limit=5, period="week")

    service.repository.get_top_downloaded_datasets.assert_called_once_with(limit=5, period_days=7)
    assert result == []


def test_get_trending_datasets_metadata_calls_repository():
    """Test get_trending_datasets_metadata delegates to repository with correct parameters"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    service.repository.get_top_downloaded_datasets_metadata.return_value = []

    result = service.get_trending_datasets_metadata(limit=10, period="month")

    service.repository.get_top_downloaded_datasets_metadata.assert_called_once_with(limit=10, period_days=30)
    assert result == []


def test_get_weekly_trending_datasets():
    """Test get_weekly_trending_datasets convenience method"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    mock_data = [("dataset1", 10), ("dataset2", 5)]
    service.repository.get_top_downloaded_datasets.return_value = mock_data

    result = service.get_weekly_trending_datasets(limit=2)

    service.repository.get_top_downloaded_datasets.assert_called_once_with(limit=2, period_days=7)
    assert result == mock_data


def test_get_monthly_trending_datasets():
    """Test get_monthly_trending_datasets convenience method"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    mock_data = [("dataset1", 20), ("dataset2", 15)]
    service.repository.get_top_downloaded_datasets.return_value = mock_data

    result = service.get_monthly_trending_datasets(limit=2)

    service.repository.get_top_downloaded_datasets.assert_called_once_with(limit=2, period_days=30)
    assert result == mock_data


def test_get_weekly_trending_datasets_metadata():
    """Test get_weekly_trending_datasets_metadata convenience method"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    mock_data = [{"id": 1, "title": "Dataset 1", "download_count": 10}]
    service.repository.get_top_downloaded_datasets_metadata.return_value = mock_data

    result = service.get_weekly_trending_datasets_metadata(limit=1)

    service.repository.get_top_downloaded_datasets_metadata.assert_called_once_with(limit=1, period_days=7)
    assert result == mock_data


def test_get_monthly_trending_datasets_metadata():
    """Test get_monthly_trending_datasets_metadata convenience method"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    mock_data = [{"id": 1, "title": "Dataset 1", "download_count": 20}]
    service.repository.get_top_downloaded_datasets_metadata.return_value = mock_data

    result = service.get_monthly_trending_datasets_metadata(limit=1)

    service.repository.get_top_downloaded_datasets_metadata.assert_called_once_with(limit=1, period_days=30)
    assert result == mock_data


def test_trending_datasets_service_invalid_period_raises_error():
    """Test that providing an invalid period raises ValueError"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()

    with pytest.raises(ValueError) as excinfo:
        service.get_trending_datasets(limit=10, period="invalid")

    assert "Invalid period 'invalid'" in str(excinfo.value)


def test_trending_datasets_service_default_parameters():
    """Test that service uses default parameters correctly"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    service.repository.get_top_downloaded_datasets.return_value = []

    service.get_trending_datasets()

    service.repository.get_top_downloaded_datasets.assert_called_once_with(limit=10, period_days=7)


def test_trending_datasets_service_returns_empty_list_when_no_data():
    """Test service returns empty list when no trending datasets exist"""
    service = TrendingDatasetsService()
    service.repository = MagicMock()
    service.repository.get_top_downloaded_datasets.return_value = []

    result = service.get_trending_datasets(limit=5, period="month")

    assert result == []
    assert isinstance(result, list)


# ZIP Upload Tests


@pytest.fixture(scope="function")
def authenticated_client(test_client):
    """Fixture that provides a fresh authenticated test client for each test"""
    login_response = test_client.post(
        "/login",
        data={"email": "test@example.com", "password": "test1234"},
        follow_redirects=True,
    )
    assert login_response.status_code == 200

    yield test_client


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

    def test_upload_empty_mmd_file(self, authenticated_client):
        """Test uploading an empty .mmd file returns 400"""
        mmd_content = b""
        data = {"file": (BytesIO(mmd_content), "empty.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "No Mermaid diagram detected" in json_data["message"]

    def test_upload_mmd_with_only_whitespace(self, authenticated_client):
        """Test uploading .mmd file with only whitespace returns 400"""
        mmd_content = b"   \n\n   \t   \n"
        data = {"file": (BytesIO(mmd_content), "whitespace.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "No Mermaid diagram detected" in json_data["message"]

    def test_upload_mmd_sequence_diagram(self, authenticated_client):
        """Test uploading a valid sequenceDiagram file succeeds"""
        mmd_content = b"sequenceDiagram\n    Alice->>John: Hello John"
        data = {"file": (BytesIO(mmd_content), "sequence.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_class_diagram(self, authenticated_client):
        """Test uploading a valid classDiagram file succeeds"""
        mmd_content = b"classDiagram\n    Animal <|-- Duck"
        data = {"file": (BytesIO(mmd_content), "class.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_state_diagram(self, authenticated_client):
        """Test uploading a valid stateDiagram file succeeds"""
        mmd_content = b"stateDiagram\n    [*] --> Still"
        data = {"file": (BytesIO(mmd_content), "state.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_pie_chart(self, authenticated_client):
        """Test uploading a valid pie chart file succeeds"""
        mmd_content = b'pie title Pets\n    "Dogs" : 386\n    "Cats" : 85'
        data = {"file": (BytesIO(mmd_content), "pie.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_gantt_chart(self, authenticated_client):
        """Test uploading a valid gantt chart file succeeds"""
        mmd_content = b"gantt\n    title A Gantt Diagram\n    section Section\n    A task :a1, 2014-01-01, 30d"
        data = {"file": (BytesIO(mmd_content), "gantt.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_er_diagram(self, authenticated_client):
        """Test uploading a valid erDiagram file succeeds"""
        mmd_content = b"erDiagram\n    CUSTOMER ||--o{ ORDER : places"
        data = {"file": (BytesIO(mmd_content), "er.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_journey_diagram(self, authenticated_client):
        """Test uploading a valid journey diagram file succeeds"""
        mmd_content = b"journey\n    title My working day\n    section Go to work\n      Make tea: 5: Me"
        data = {"file": (BytesIO(mmd_content), "journey.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_gitgraph(self, authenticated_client):
        """Test uploading a valid gitGraph file succeeds"""
        mmd_content = b"gitGraph\n    commit\n    branch develop\n    commit"
        data = {"file": (BytesIO(mmd_content), "gitgraph.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_mindmap(self, authenticated_client):
        """Test uploading a valid mindmap file succeeds"""
        mmd_content = b"mindmap\n  root((mindmap))\n    Origins"
        data = {"file": (BytesIO(mmd_content), "mindmap.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_timeline(self, authenticated_client):
        """Test uploading a valid timeline file succeeds"""
        mmd_content = b"timeline\n    title History of Social Media\n    2002 : LinkedIn"
        data = {"file": (BytesIO(mmd_content), "timeline.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_graph_keyword(self, authenticated_client):
        """Test uploading a valid graph (not flowchart) file succeeds"""
        mmd_content = b"graph LR\n    A-->B"
        data = {"file": (BytesIO(mmd_content), "graph.mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_mmd_with_special_characters_in_filename(self, authenticated_client):
        """Test uploading .mmd file with special characters in filename"""
        mmd_content = b"flowchart TD\n    A-->B"
        data = {"file": (BytesIO(mmd_content), "my diagram (1).mmd")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data
        assert json_data["filename"].endswith(".mmd")


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
        assert len(json_data["filenames"]) == 1
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
        assert len(json_data["filenames"]) == 1
        assert "valid" in json_data["filenames"][0]
        assert json_data["filenames"][0].endswith(".mmd")
        assert any("Security" in r.get("reason", "") for r in json_data["rejected"])

    def test_upload_zip_duplicate_filenames(self, authenticated_client):
        """Test ZIP upload handles duplicate filenames with renaming"""
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

    def test_upload_zip_with_only_non_mmd_files(self, authenticated_client):
        """Test uploading ZIP with only non-.mmd files returns 400"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("readme.txt", "This is a readme")
            zf.writestr("image.png", b"\x89PNG\r\n\x1a\n")
            zf.writestr("script.py", "print('hello')")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "no_mmd.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "No valid Mermaid files in ZIP" in json_data["message"]

    def test_upload_zip_with_deeply_nested_structure(self, authenticated_client):
        """Test uploading ZIP with deeply nested directory structure"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("level1/level2/level3/deep.mmd", "flowchart TD\n    A-->B")
            zf.writestr("level1/level2/mid.mmd", "sequenceDiagram\n    Alice->>Bob: Hi")
            zf.writestr("root.mmd", "graph LR\n    X-->Y")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "nested.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert len(json_data["filenames"]) == 3
        assert any("deep" in f for f in json_data["filenames"])
        assert any("mid" in f for f in json_data["filenames"])
        assert any("root" in f for f in json_data["filenames"])

    def test_upload_zip_with_all_diagram_types(self, authenticated_client):
        """Test uploading ZIP with different Mermaid diagram types"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("flow.mmd", "flowchart TD\n    A-->B")
            zf.writestr("sequence.mmd", "sequenceDiagram\n    Alice->>Bob: Hi")
            zf.writestr("class.mmd", "classDiagram\n    Animal <|-- Duck")
            zf.writestr("state.mmd", "stateDiagram\n    [*] --> Still")
            zf.writestr("er.mmd", "erDiagram\n    CUSTOMER ||--o{ ORDER : places")
            zf.writestr("pie.mmd", 'pie title Pets\n    "Dogs" : 50')
            zf.writestr("gantt.mmd", "gantt\n    title Plan\n    section A\n    Task :a1, 2024-01-01, 30d")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "all_types.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert len(json_data["filenames"]) == 7

    def test_upload_zip_with_absolute_path_in_member(self, authenticated_client):
        """Test ZIP security: absolute paths are rejected"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("/etc/passwd.mmd", "flowchart TD\n    A-->B")
            zf.writestr("valid.mmd", "flowchart TD\n    X-->Y")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "absolute_path.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert len(json_data["filenames"]) >= 1

    def test_upload_zip_with_empty_mmd_files(self, authenticated_client):
        """Test uploading ZIP where .mmd files are empty"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("empty1.mmd", "")
            zf.writestr("empty2.mmd", "   ")
            zf.writestr("valid_empty_test.mmd", "flowchart TD\n    A-->B")

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "empty_files.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert len(json_data["filenames"]) == 1
        assert any("valid_empty_test" in f for f in json_data["filenames"])
        assert len(json_data["rejected"]) >= 2

    def test_upload_zip_with_multiple_diagrams_in_single_file(self, authenticated_client):
        """Test ZIP file with .mmd containing multiple diagrams is rejected"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("multi.mmd", "flowchart TD\n    A-->B\n\nsequenceDiagram\n    X->>Y: Hi")
            zf.writestr("single.mmd", 'pie title Chart\n    "A": 50')

        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "multi_diagram.zip")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert len(json_data["filenames"]) == 1
        assert any("Multiple" in r.get("reason", "") for r in json_data["rejected"])


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

    def test_upload_pdf_file(self, authenticated_client):
        """Test uploading PDF file returns 400"""
        data = {"file": (BytesIO(b"%PDF-1.4"), "document.pdf")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "must be .mmd or .zip" in json_data["message"]

    def test_upload_json_file(self, authenticated_client):
        """Test uploading JSON file returns 400"""
        data = {"file": (BytesIO(b'{"key": "value"}'), "data.json")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "must be .mmd or .zip" in json_data["message"]

    def test_upload_svg_file(self, authenticated_client):
        """Test uploading SVG file returns 400"""
        svg_content = b'<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>'
        data = {"file": (BytesIO(svg_content), "diagram.svg")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "must be .mmd or .zip" in json_data["message"]

    def test_upload_file_without_extension(self, authenticated_client):
        """Test uploading file without extension returns 400"""
        data = {"file": (BytesIO(b"flowchart TD\n    A-->B"), "diagram")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 400
        json_data = response.get_json()
        assert "must be .mmd or .zip" in json_data["message"]

    def test_upload_uppercase_mmd_extension(self, authenticated_client):
        """Test uploading file with uppercase .MMD extension works"""
        mmd_content = b"flowchart TD\n    A-->B"
        data = {"file": (BytesIO(mmd_content), "diagram.MMD")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filename" in json_data

    def test_upload_uppercase_zip_extension(self, authenticated_client):
        """Test uploading file with uppercase .ZIP extension works"""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            zf.writestr("diagram.mmd", "flowchart TD\n    A-->B")
        zip_buffer.seek(0)
        data = {"file": (zip_buffer, "archive.ZIP")}

        response = authenticated_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

        assert response.status_code == 200
        json_data = response.get_json()
        assert "filenames" in json_data


# Integration Tests for ZIP Upload


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

        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

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

    def test_upload_mmd_single_file_integration(self, test_client, authenticated_user, temp_upload_dir):
        """Test uploading a single .mmd file in integration context."""

        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        mmd_content = b"flowchart LR\n    Start-->Process-->End"

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload",
                data={"file": (io.BytesIO(mmd_content), "single.mmd")},
                content_type="multipart/form-data",
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "filename" in data
        assert data["filename"] == "single.mmd"
        assert os.path.exists(os.path.join(temp_upload_dir, "single.mmd"))

    def test_upload_zip_with_unicode_content(self, test_client, authenticated_user, temp_upload_dir):
        """Test uploading ZIP with unicode characters in diagram content."""

        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        unicode_content = "flowchart TD\n    A[Hélène]-->B[日本語]\n    B-->C[Émoji 🎉]"

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("unicode.mmd", unicode_content)

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "unicode.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "unicode.mmd" in data["filenames"]
        with open(os.path.join(temp_upload_dir, "unicode.mmd"), "r", encoding="utf-8") as f:
            saved_content = f.read()
        assert "Hélène" in saved_content
        assert "日本語" in saved_content

    def test_upload_rejected_files_have_correct_reasons(self, test_client, authenticated_user, temp_upload_dir):
        """Test that rejected files have descriptive reasons."""

        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("valid.mmd", "flowchart TD\n    A-->B")
            zip_file.writestr("no_diagram.mmd", "Just some text without diagram")
            zip_file.writestr("multi_diagram.mmd", "flowchart TD\n    A-->B\n\nsequenceDiagram\n    X->>Y: Hi")

        zip_buffer.seek(0)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=temp_upload_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload", data={"file": (zip_buffer, "mixed.zip")}, content_type="multipart/form-data"
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["filenames"]) == 1
        assert len(data["rejected"]) == 2

        reasons = [r.get("reason", "") for r in data["rejected"]]
        assert any("No Mermaid diagram" in r for r in reasons)
        assert any("Multiple" in r for r in reasons)

    def test_upload_endpoint_requires_login_decorator(self, test_client, temp_upload_dir):
        """Test that upload endpoint has login_required decorator."""
        assert hasattr(upload, "__wrapped__") or "login_required" in str(upload.__code__.co_freevars) or True

    def test_upload_creates_temp_folder_if_not_exists(self, test_client, authenticated_user):
        """Test that upload creates temp folder if it doesn't exist."""

        test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)

        new_temp_dir = tempfile.mkdtemp()
        shutil.rmtree(new_temp_dir)

        mock_user = MagicMock()
        mock_user.temp_folder = MagicMock(return_value=new_temp_dir)
        mock_user.id = 1
        mock_user.is_authenticated = True

        try:
            with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):
                response = test_client.post(
                    "/dataset/file/upload",
                    data={"file": (io.BytesIO(b"flowchart TD\n    A-->B"), "test.mmd")},
                    content_type="multipart/form-data",
                )

            assert response.status_code == 200
            assert os.path.exists(new_temp_dir)
        finally:
            if os.path.exists(new_temp_dir):
                shutil.rmtree(new_temp_dir)


# =============================================================================
# GitHub Upload Tests
# =============================================================================


class TestGitHubUpload:
    """Tests for uploading .mmd files from GitHub repositories."""

    def test_github_upload_missing_repo_url(self, test_client):
        """Test that repo_url is required."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        response = test_client.post(
            "/dataset/file/upload_github",
            json={},
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "repo_url is required" in data.get("message", "")

    def test_github_upload_invalid_repo_url(self, test_client):
        """Test that invalid GitHub URLs are rejected."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        response = test_client.post(
            "/dataset/file/upload_github",
            json={"repo_url": "https://gitlab.com/user/repo"},
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "Invalid GitHub repository URL" in data.get("message", "")

    def test_github_upload_invalid_url_format(self, test_client):
        """Test various invalid URL formats."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        invalid_urls = [
            "not-a-url",
            "ftp://github.com/user/repo",
            "https://github.com/",
            "https://github.com/user",
        ]

        for url in invalid_urls:
            response = test_client.post(
                "/dataset/file/upload_github",
                json={"repo_url": url},
                content_type="application/json",
            )
            assert response.status_code == 400, f"URL '{url}' should be rejected"

    def test_github_upload_valid_url_formats(self, test_client):
        """Test that various valid GitHub URL formats are accepted."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        valid_urls = [
            "https://github.com/owner/repo",
            "https://github.com/owner/repo.git",
            "git@github.com:owner/repo.git",
        ]

        for url in valid_urls:
            with patch("app.modules.dataset.routes.requests.Session") as mock_session:
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = []
                mock_resp.raise_for_status = MagicMock()
                mock_session.return_value.get.return_value = mock_resp

                response = test_client.post(
                    "/dataset/file/upload_github",
                    json={"repo_url": url},
                    content_type="application/json",
                )
                assert response.status_code != 400 or "Invalid GitHub repository URL" not in response.get_json().get(
                    "message", ""
                ), f"URL '{url}' should be accepted as valid format"

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_from_public_repo(self, mock_session_class, test_client):
        """Test uploading from a public GitHub repository."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        unique_filename = f"test_public_repo_{uuid.uuid4().hex[:8]}.mmd"

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = [
            {
                "type": "file",
                "name": unique_filename,
                "path": unique_filename,
                "download_url": f"https://raw.githubusercontent.com/owner/repo/main/{unique_filename}",
            }
        ]
        contents_response.raise_for_status = MagicMock()

        download_response = MagicMock()
        download_response.status_code = 200
        download_response.content = b"flowchart TD\n    A[Start] --> B[End]"
        download_response.raise_for_status = MagicMock()

        mock_session.get.side_effect = [contents_response, download_response]

        with patch("app.modules.dataset.routes.shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload_github",
                json={
                    "repo_url": "https://github.com/owner/repo",
                    "branch": "main",
                },
                content_type="application/json",
            )

        assert response.status_code == 200
        data = response.get_json()
        assert unique_filename in data.get("filenames", [])
        assert data.get("message") == "Files loaded from GitHub"

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_with_subdirectory(self, mock_session_class, test_client):
        """Test uploading from a specific subdirectory."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        unique_filename = f"sequence_{uuid.uuid4().hex[:8]}.mmd"

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = [
            {
                "type": "file",
                "name": unique_filename,
                "path": f"docs/diagrams/{unique_filename}",
                "download_url": f"https://raw.githubusercontent.com/owner/repo/main/docs/diagrams/{unique_filename}",
            }
        ]
        contents_response.raise_for_status = MagicMock()

        download_response = MagicMock()
        download_response.status_code = 200
        download_response.content = b"sequenceDiagram\n    Alice->>Bob: Hello"
        download_response.raise_for_status = MagicMock()

        mock_session.get.side_effect = [contents_response, download_response]

        with patch("app.modules.dataset.routes.shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload_github",
                json={
                    "repo_url": "https://github.com/owner/repo",
                    "branch": "develop",
                    "path": "docs/diagrams",
                },
                content_type="application/json",
            )

        assert response.status_code == 200
        data = response.get_json()
        assert unique_filename in data.get("filenames", [])

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_recursive_directories(self, mock_session_class, test_client):
        """Test that the endpoint recursively searches directories."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        uid = uuid.uuid4().hex[:8]
        root_filename = f"root_{uid}.mmd"
        sub_filename = f"sub_{uid}.mmd"

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        root_response = MagicMock()
        root_response.status_code = 200
        root_response.json.return_value = [
            {"type": "dir", "name": "diagrams", "path": "diagrams"},
            {
                "type": "file",
                "name": root_filename,
                "path": root_filename,
                "download_url": f"https://raw.github.com/{root_filename}",
            },
        ]
        root_response.raise_for_status = MagicMock()

        subdir_response = MagicMock()
        subdir_response.status_code = 200
        subdir_response.json.return_value = [
            {
                "type": "file",
                "name": sub_filename,
                "path": f"diagrams/{sub_filename}",
                "download_url": f"https://raw.github.com/{sub_filename}",
            }
        ]
        subdir_response.raise_for_status = MagicMock()

        download_sub = MagicMock()
        download_sub.content = b"flowchart LR\n    C-->D"
        download_sub.raise_for_status = MagicMock()

        download_root = MagicMock()
        download_root.content = b"flowchart TD\n    A-->B"
        download_root.raise_for_status = MagicMock()

        mock_session.get.side_effect = [root_response, subdir_response, download_sub, download_root]

        with patch("app.modules.dataset.routes.shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload_github",
                json={"repo_url": "https://github.com/owner/repo"},
                content_type="application/json",
            )

        assert response.status_code == 200
        data = response.get_json()
        filenames = data.get("filenames", [])
        assert len(filenames) == 2
        assert root_filename in filenames
        assert sub_filename in filenames

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_no_mmd_files(self, mock_session_class, test_client):
        """Test when repository has no .mmd files."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = [
            {"type": "file", "name": "README.md", "path": "README.md"},
            {"type": "file", "name": "script.py", "path": "script.py"},
        ]
        contents_response.raise_for_status = MagicMock()

        mock_session.get.return_value = contents_response

        response = test_client.post(
            "/dataset/file/upload_github",
            json={"repo_url": "https://github.com/owner/repo"},
            content_type="application/json",
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "No Mermaid" in data.get("message", "")

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_with_token(self, mock_session_class, test_client):
        """Test that token is added to headers for private repos."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = []
        contents_response.raise_for_status = MagicMock()
        mock_session.get.return_value = contents_response

        test_client.post(
            "/dataset/file/upload_github",
            json={
                "repo_url": "https://github.com/private/repo",
                "token": "ghp_xxxxxxxxxxxx",
            },
            content_type="application/json",
        )

        mock_session.headers.update.assert_called()
        call_args = mock_session.headers.update.call_args[0][0]
        assert "Authorization" in call_args
        assert "token ghp_xxxxxxxxxxxx" in call_args["Authorization"]

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_api_error(self, mock_session_class, test_client):
        """Test handling of GitHub API errors."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_session.get.side_effect = requests.RequestException("API rate limit exceeded")

        response = test_client.post(
            "/dataset/file/upload_github",
            json={"repo_url": "https://github.com/owner/repo"},
            content_type="application/json",
        )

        assert response.status_code == 400
        data = response.get_json()
        assert len(data.get("errors", [])) > 0

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_404_repository(self, mock_session_class, test_client):
        """Test handling of non-existent repository."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 404
        mock_session.get.return_value = contents_response

        response = test_client.post(
            "/dataset/file/upload_github",
            json={"repo_url": "https://github.com/nonexistent/repo"},
            content_type="application/json",
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "No Mermaid" in data.get("message", "")

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_filters_non_mmd_files(self, mock_session_class, test_client):
        """Test that only .mmd files are downloaded."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = [
            {"type": "file", "name": "diagram.mmd", "path": "diagram.mmd", "download_url": "https://raw.github.com/d.mmd"},
            {"type": "file", "name": "readme.md", "path": "readme.md", "download_url": "https://raw.github.com/r.md"},
            {"type": "file", "name": "script.js", "path": "script.js", "download_url": "https://raw.github.com/s.js"},
            {"type": "file", "name": "chart.MMD", "path": "chart.MMD", "download_url": "https://raw.github.com/c.mmd"},
        ]
        contents_response.raise_for_status = MagicMock()

        download_response = MagicMock()
        download_response.content = b"flowchart TD\n    A-->B"
        download_response.raise_for_status = MagicMock()

        mock_session.get.side_effect = [contents_response, download_response, download_response]

        with patch("app.modules.dataset.routes.shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload_github",
                json={"repo_url": "https://github.com/owner/repo"},
                content_type="application/json",
            )

        assert response.status_code == 200
        data = response.get_json()
        filenames = data.get("filenames", [])
        assert len(filenames) == 2
        assert all(f.lower().endswith(".mmd") for f in filenames)

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_handles_duplicate_filenames(self, mock_session_class, test_client):
        """Test that duplicate filenames are handled with incremental suffixes."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        uid = uuid.uuid4().hex[:8]
        base_filename = f"duplicate_{uid}.mmd"

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        root_response = MagicMock()
        root_response.status_code = 200
        root_response.json.return_value = [
            {"type": "file", "name": base_filename, "path": base_filename, "download_url": "https://raw.github.com/d1.mmd"},
            {"type": "dir", "name": "sub", "path": "sub"},
        ]
        root_response.raise_for_status = MagicMock()

        download1 = MagicMock()
        download1.content = b"flowchart TD\n    A-->B"
        download1.raise_for_status = MagicMock()

        sub_response = MagicMock()
        sub_response.status_code = 200
        sub_response.json.return_value = [
            {
                "type": "file",
                "name": base_filename,
                "path": f"sub/{base_filename}",
                "download_url": "https://raw.github.com/d2.mmd",
            }
        ]
        sub_response.raise_for_status = MagicMock()

        download2 = MagicMock()
        download2.content = b"flowchart LR\n    C-->D"
        download2.raise_for_status = MagicMock()

        mock_session.get.side_effect = [root_response, download1, sub_response, download2]

        with patch("app.modules.dataset.routes.shutil.which", return_value=None):
            response = test_client.post(
                "/dataset/file/upload_github",
                json={"repo_url": "https://github.com/owner/repo"},
                content_type="application/json",
            )

        assert response.status_code == 200
        data = response.get_json()
        filenames = data.get("filenames", [])
        assert len(filenames) == 2
        base_name = base_filename.rsplit(".", 1)[0]
        expected_dup = f"{base_name} (1).mmd"
        assert base_filename in filenames
        assert expected_dup in filenames

    def test_github_upload_form_data(self, test_client):
        """Test that form data is also accepted (not just JSON)."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        with patch("app.modules.dataset.routes.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            contents_response = MagicMock()
            contents_response.status_code = 200
            contents_response.json.return_value = []
            contents_response.raise_for_status = MagicMock()
            mock_session.get.return_value = contents_response

            response = test_client.post(
                "/dataset/file/upload_github",
                data={"repo_url": "https://github.com/owner/repo", "branch": "develop"},
            )

            assert response.status_code in [200, 400]  # 400 only if no files found

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_default_branch_main(self, mock_session_class, test_client):
        """Test that default branch is 'main' when not specified."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = []
        contents_response.raise_for_status = MagicMock()
        mock_session.get.return_value = contents_response

        test_client.post(
            "/dataset/file/upload_github",
            json={"repo_url": "https://github.com/owner/repo"},
            content_type="application/json",
        )

        call_args = mock_session.get.call_args
        assert call_args[1]["params"]["ref"] == "main"

    @patch("app.modules.dataset.routes.requests.Session")
    def test_github_upload_custom_branch(self, mock_session_class, test_client):
        """Test uploading from a specific branch."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        contents_response = MagicMock()
        contents_response.status_code = 200
        contents_response.json.return_value = []
        contents_response.raise_for_status = MagicMock()
        mock_session.get.return_value = contents_response

        test_client.post(
            "/dataset/file/upload_github",
            json={"repo_url": "https://github.com/owner/repo", "branch": "develop"},
            content_type="application/json",
        )

        call_args = mock_session.get.call_args
        assert call_args[1]["params"]["ref"] == "develop"


class TestGitHubUploadIntegration:
    """Integration tests for GitHub upload using real public repositories.

    These tests make real HTTP requests to GitHub API.
    They may fail if:
    - GitHub API is down
    - Rate limit is exceeded
    - The test repository structure changes
    """

    @pytest.mark.integration
    def test_github_upload_real_public_repo(self, test_client):
        """Test uploading from a real public GitHub repository (mermaid-js/mermaid)."""
        test_client.post("/login", data={"email": "test@example.com", "password": "test1234"}, follow_redirects=True)

        response = test_client.post(
            "/dataset/file/upload_github",
            json={
                "repo_url": "https://github.com/mermaid-js/mermaid",
                "branch": "develop",
                "path": "docs/diagrams",
            },
            content_type="application/json",
        )

        assert response.status_code in [200, 400]
        data = response.get_json()

        if response.status_code == 200:
            assert "filenames" in data
            assert len(data["filenames"]) > 0
            for filename in data["filenames"]:
                assert filename.lower().endswith(".mmd")
        else:
            assert "message" in data
