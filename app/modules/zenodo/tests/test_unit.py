import pytest

from app.modules.zenodo.services import FakenodoService


class TestAuthor:
    def __init__(self, name, affiliation=None, orcid=None):
        self.name = name
        self.affiliation = affiliation
        self.orcid = orcid


class TestMetaData:
    def __init__(self, title, description, diagram_type, authors, tags=None):
        self.title = title
        self.description = description
        self.diagram_type = diagram_type
        self.authors = authors
        self.tags = tags


class TestDiagramType:
    def __init__(self, value):
        self.value = value


class TestDataset:
    def __init__(self, user_id, meta):
        self.user_id = user_id
        self.ds_meta_data = meta


class TestMermaidDiagram:
    def __init__(self, filename):
        self.md_meta_data = type("Meta", (), {"mmd_filename": filename})


@pytest.fixture
def service():
    return FakenodoService()


@pytest.fixture
def dataset():
    authors = [
        TestAuthor("Alice", "University X", "0000-0001"),
        TestAuthor("Bob"),
    ]
    meta = TestMetaData(
        title="Sample Dataset",
        description="A test dataset",
        diagram_type=TestDiagramType("none"),
        authors=authors,
        tags="science, data",
    )
    return TestDataset(user_id=42, meta=meta)


def test_test_full_connection(service):
    result = service.test_full_connection()
    assert result == {"success": True, "messages": "OK"}


def test_create_new_deposition(service, dataset):
    dep = service.create_new_deposition(dataset)

    assert dep["metadata"]["title"] == "Sample Dataset"
    assert dep["metadata"]["upload_type"] == "dataset"
    assert dep["owner"] == 42
    assert dep["version"] == 1
    assert dep["state"] == "unsubmitted"
    assert dep["files"] == []

    assert dep["id"] in service.depositions


def test_get_all_depositions(service, dataset):
    service.create_new_deposition(dataset)
    service.create_new_deposition(dataset)

    all_deps = service.get_all_depositions()
    assert len(all_deps) == 2
    assert all(isinstance(dep, dict) for dep in all_deps)


def test_upload_file_success(service, dataset):
    dep = service.create_new_deposition(dataset)
    mermaid = TestMermaidDiagram("diagram.mmd")

    result = service.upload_file(dataset, dep["id"], mermaid)

    assert "filename" in result
    assert result["filename"] == "diagram.mmd"
    assert len(service.depositions[dep["id"]]["files"]) == 1


def test_upload_file_not_found(service, dataset):
    mermaid = TestMermaidDiagram("diagram.mmd")
    result, status = service.upload_file(dataset, 999999, mermaid)
    assert status == 404
    assert result["error"] == "Deposition not found"


def test_publish_deposition_success(service, dataset):
    dep = service.create_new_deposition(dataset)
    published = service.publish_deposition(dep["id"])

    assert published["submitted"] is True
    assert published["state"] == "done"
    assert published["doi"].startswith("10.5281/fakenodo.")
    assert published["links"]["doi"] == published["doi_url"]


def test_publish_deposition_not_found(service):
    result, status = service.publish_deposition(9999)
    assert status == 404
    assert result["error"] == "Deposition not found"


def test_get_deposition_success(service, dataset):
    dep = service.create_new_deposition(dataset)
    result = service.get_deposition(dep["id"])
    assert result["id"] == dep["id"]


def test_get_deposition_not_found(service):
    result, status = service.get_deposition(99999)
    assert status == 404
    assert result["error"] == "Deposition not found"


def test_get_doi_returns_none_if_not_found(service):
    assert service.get_doi(99999) is None


def test_get_doi_returns_doi(service, dataset):
    dep = service.create_new_deposition(dataset)
    service.publish_deposition(dep["id"])
    doi = service.get_doi(dep["id"])
    assert doi.startswith("10.5281/fakenodo.")
