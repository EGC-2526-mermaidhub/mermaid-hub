import pytest
from unittest.mock import MagicMock

from app.modules.mermaiddiagram.services import MermaidDiagramService


@pytest.fixture
def mermaid_service():
    service = MermaidDiagramService()
    service.repository = MagicMock()
    service.hubfile_service = MagicMock()
    return service


def test_total_mermaid_diagram_views_delegates_to_hubfile_service(mermaid_service):
    mermaid_service.hubfile_service.total_hubfile_views.return_value = 15
    result = mermaid_service.total_mermaid_diagram_views()
    assert result == 15
    mermaid_service.hubfile_service.total_hubfile_views.assert_called_once()


def test_total_mermaid_diagram_downloads_delegates_to_hubfile_service(mermaid_service):
    mermaid_service.hubfile_service.total_hubfile_downloads.return_value = 9
    result = mermaid_service.total_mermaid_diagram_downloads()
    assert result == 9
    mermaid_service.hubfile_service.total_hubfile_downloads.assert_called_once()


def test_count_mermaid_diagrams_delegates_to_repository(mermaid_service):
    mermaid_service.repository.count_mermaid_diagrams.return_value = 33
    result = mermaid_service.count_mermaid_diagrams()
    assert result == 33
    mermaid_service.repository.count_mermaid_diagrams.assert_called_once()


def test_mdmetadata_service_initialization():
    from app.modules.mermaiddiagram.repositories import MDMetaDataRepository

    inner = MermaidDiagramService.MDMetaDataService()
    assert isinstance(inner.repository, MDMetaDataRepository)
