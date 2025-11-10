import os
from unittest.mock import MagicMock, patch

import pytest

from app.modules.hubfile.services import HubfileDownloadRecordService, HubfileService


@pytest.fixture
def hubfile_service():
    service = HubfileService()
    service.repository = MagicMock()
    service.hubfile_view_record_repository = MagicMock()
    service.hubfile_download_record_repository = MagicMock()
    return service


@pytest.fixture
def test_hubfile():
    class TestHubfile:
        def __init__(self, name="diagram.mmd"):
            self.name = name

    return TestHubfile()


@pytest.fixture
def test_user():
    class TestUser:
        def __init__(self):
            self.id = 10

    return TestUser()


@pytest.fixture
def test_dataset():
    class TestDataset:
        def __init__(self):
            self.id = 20

    return TestDataset()


def test_get_owner_user_by_hubfile_calls_repository(hubfile_service, test_hubfile):
    hubfile_service.repository.get_owner_user_by_hubfile.return_value = "user"
    result = hubfile_service.get_owner_user_by_hubfile(test_hubfile)
    assert result == "user"
    hubfile_service.repository.get_owner_user_by_hubfile.assert_called_once_with(test_hubfile)


def test_get_dataset_by_hubfile_calls_repository(hubfile_service, test_hubfile):
    hubfile_service.repository.get_dataset_by_hubfile.return_value = "dataset"
    result = hubfile_service.get_dataset_by_hubfile(test_hubfile)
    assert result == "dataset"
    hubfile_service.repository.get_dataset_by_hubfile.assert_called_once_with(test_hubfile)


def test_get_path_by_hubfile_builds_correct_path(hubfile_service, test_hubfile, test_user, test_dataset):
    hubfile_service.get_owner_user_by_hubfile = MagicMock(return_value=test_user)
    hubfile_service.get_dataset_by_hubfile = MagicMock(return_value=test_dataset)

    with patch.dict(os.environ, {"WORKING_DIR": "/tmp"}):
        path = hubfile_service.get_path_by_hubfile(test_hubfile)
        assert path == "/tmp/uploads/user_10/dataset_20/diagram.mmd"


def test_total_hubfile_views_returns_count(hubfile_service):
    hubfile_service.hubfile_view_record_repository.total_hubfile_views.return_value = 42
    result = hubfile_service.total_hubfile_views()
    assert result == 42
    hubfile_service.hubfile_view_record_repository.total_hubfile_views.assert_called_once()


def test_total_hubfile_downloads_uses_repository_class(monkeypatch):
    mock_repo = MagicMock()
    mock_repo.total_hubfile_downloads.return_value = 99
    monkeypatch.setattr("app.modules.hubfile.services.HubfileDownloadRecordRepository", lambda: mock_repo)

    service = HubfileService()
    result = service.total_hubfile_downloads()
    assert result == 99
    mock_repo.total_hubfile_downloads.assert_called_once()


def test_hubfile_download_record_service_initialization():
    s = HubfileDownloadRecordService()
    assert isinstance(s.repository, object)
