import os
import uuid
from unittest.mock import MagicMock, patch

import pytest
from flask import url_for

from app.modules.dataset.services import (
    DataSetService,
    DOIMappingService,
    DSDownloadRecordService,
    DSViewRecordService,
    SizeService,
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

    # Create mock datasets
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
    from app.modules.dataset.repositories import DSDownloadRecordRepository

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.filter.return_value.count.return_value = 0

    result = repo.dataset_downloads_id(1)

    assert result == 0


def test_dsdownloadrecord_repository_dataset_downloads_id_with_downloads():
    """Test repository dataset_downloads_id returns correct count"""
    from app.modules.dataset.repositories import DSDownloadRecordRepository

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.filter.return_value.count.return_value = 15

    result = repo.dataset_downloads_id(1)

    assert result == 15


def test_dsdownloadrecord_repository_total_dataset_downloads_zero():
    """Test total_dataset_downloads returns 0 when no records"""
    from app.modules.dataset.repositories import DSDownloadRecordRepository

    repo = DSDownloadRecordRepository()
    repo.model = MagicMock()
    repo.model.query.with_entities.return_value.scalar.return_value = None

    result = repo.total_dataset_downloads()

    assert result == 0


def test_dsdownloadrecord_repository_total_dataset_downloads_with_records():
    """Test total_dataset_downloads returns max id when records exist"""
    from app.modules.dataset.repositories import DSDownloadRecordRepository

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
    with test_client.application.app_context():
        # Add HERE new elements to the database that you want to exist in the test context.
        # DO NOT FORGET to use db.session.add(<element>) and db.session.commit() to save the data.
        pass

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

    # Need to create a BytesIO object to simulate a .txt upload
    from io import BytesIO

    data = {"file": (BytesIO(b"content"), "test.txt")}
    response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 400

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_no_mermaid_content(test_client):
    """Test file upload with no mermaid diagram returns 400"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    # Need to create a BytesIO object to simulate a .mmd upload
    import tempfile
    from io import BytesIO

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user):
        data = {"file": (BytesIO(b"invalid content"), "test.mmd")}
        response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 400

    # We need to clean up the temporary directory created for this test
    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_multiple_diagrams(test_client):
    """Test file upload with multiple diagrams returns 400"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    import tempfile
    from io import BytesIO

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    # Simulate a file with multiple mermaid diagrams
    with patch("app.modules.dataset.routes.current_user", mock_user):
        data = {"file": (BytesIO(b"graph TD\nA-->B\n\nflowchart LR\nC-->D"), "test.mmd")}
        response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 400

    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_upload_valid_mermaid(test_client):
    """Test file upload with valid mermaid diagram succeeds"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    import tempfile
    from io import BytesIO

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    # Simulate a valid mermaid diagram file
    with patch("app.modules.dataset.routes.current_user", mock_user), patch("shutil.which", return_value=None):

        data = {"file": (BytesIO(b"graph TD\nA-->B"), "diagram.mmd")}
        response = test_client.post("/dataset/file/upload", data=data, content_type="multipart/form-data")

    assert response.status_code == 200

    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_delete_nonexistent(test_client):
    """Test deleting non-existent file returns error"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    import tempfile

    temp_dir = tempfile.mkdtemp()

    mock_user = MagicMock()
    mock_user.temp_folder = MagicMock(return_value=temp_dir)
    mock_user.id = 1
    mock_user.is_authenticated = True

    with patch("app.modules.dataset.routes.current_user", mock_user):
        response = test_client.post("/dataset/file/delete", json={"file": "nonexistent.mmd"})

    assert response.status_code == 200

    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_file_delete_success(test_client):
    """Test deleting existing file succeeds"""
    response = test_client.post("/login", data=dict(email="test@example.com", password="test1234"), follow_redirects=True)
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    import tempfile

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

    import shutil

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

    import tempfile
    from zipfile import ZipFile

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

    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)


def test_download_with_existing_cookie(test_client):
    import os
    import shutil
    import tempfile
    import uuid
    from unittest.mock import MagicMock, patch
    from zipfile import ZipFile

    # Loguearse
    response = test_client.post(
        "/login",
        data=dict(email="test@example.com", password="test1234"),
        follow_redirects=True,
    )
    assert response.request.path != url_for("auth.login"), "Login was unsuccessful"

    # Crear cookie
    cookie_value = str(uuid.uuid4())
    test_client.set_cookie("download_cookie", cookie_value)

    # Crear directorio temporal y zip dummy
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

            # Evitar que se cree registro real
            mock_create.return_value = None

            # Ejecutar endpoint
            response = test_client.get("/dataset/download/1")
            assert response.status_code == 200

            content_disposition = response.headers.get("Content-Disposition", "")
            assert "dataset_1.zip" in content_disposition

    finally:
        shutil.rmtree(temp_dir)

    test_client.get("/logout", follow_redirects=True)
