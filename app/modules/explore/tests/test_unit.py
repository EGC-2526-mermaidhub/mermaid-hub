import pytest
from unittest.mock import MagicMock

from app.modules.explore.services import ExploreService


@pytest.fixture
def explore_service():
    service = ExploreService()
    service.repository = MagicMock()
    service.repository.filter.return_value = {"results": []}
    return service


def test_filter_with_default_values(explore_service):
    result = explore_service.filter()

    explore_service.repository.filter.assert_called_once_with(
        "",
        "newest",
        "any",
        [],
    )
    assert result == {"results": []}


def test_filter_with_custom_values(explore_service):
    explore_service.repository.filter.return_value = {"results": ["diagram1", "diagram2"]}

    result = explore_service.filter(
        query="test", sorting="oldest", diagram_type="FLOWCHART", tags=["flow", "chart"], page=2, per_page=10
    )

    explore_service.repository.filter.assert_called_once_with(
        "test", "oldest", "FLOWCHART", ["flow", "chart"], page=2, per_page=10
    )
    assert result == {"results": ["diagram1", "diagram2"]}


def test_filter_handles_empty_tags_list(explore_service):
    explore_service.filter(tags=[])
    first_call_tags = explore_service.repository.filter.call_args[0][3]

    explore_service.filter(tags=[])
    second_call_tags = explore_service.repository.filter.call_args[0][3]

    assert first_call_tags == []
    assert second_call_tags == []
    assert first_call_tags is not second_call_tags


def test_filter_forwards_additional_kwargs(explore_service):
    explore_service.filter(extra_param="value", another_param=123)

    _, kwargs = explore_service.repository.filter.call_args
    assert kwargs["extra_param"] == "value"
    assert kwargs["another_param"] == 123
