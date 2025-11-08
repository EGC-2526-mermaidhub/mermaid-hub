import os
import tempfile
import pytest
import uuid
from unittest.mock import MagicMock, patch

from app.modules.dataset.services import (
    calculate_checksum_and_size,
    DataSetService,
    SizeService,
    DSViewRecordService,
    DOIMappingService,
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


def test_calculate_checksum_and_size_creates_valid_md5(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_bytes(b"abc")
    checksum, size = calculate_checksum_and_size(str(file_path))
    assert checksum == "900150983cd24fb0d6963f7d28e17f72"
    assert size == 3


def test_size_service_readable_formats():
    s = SizeService()
    assert s.get_human_readable_size(100) == "100 bytes"
    assert s.get_human_readable_size(2048) == "2.0 KB"
    assert s.get_human_readable_size(2 * 1024**2) == "2.0 MB"
    assert s.get_human_readable_size(3 * 1024**3) == "3.0 GB"


def test_move_mermaid_diagrams(dataset_service, test_user, tmp_path):
    dataset = MagicMock()
    dataset.mermaid_diagrams = [
        MagicMock(md_meta_data=MagicMock(mmd_filename="diagram1.mmd"))
    ]
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
    dataset_service.repository.latest_synchronized.return_value = "latest"
    dataset_service.repository.count_synchronized_datasets.return_value = 3

    assert dataset_service.get_synchronized(1) == "sync"
    assert dataset_service.get_unsynchronized(1) == "unsync"
    assert dataset_service.get_unsynchronized_dataset(1, 2) == "unsync_ds"
    assert dataset_service.latest_synchronized() == "latest"
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
    with patch("app.modules.dataset.services.url_for", side_effect=Exception()), patch.dict(
        os.environ, {"DOMAIN": "example.com"}
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