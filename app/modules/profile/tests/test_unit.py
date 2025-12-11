import pytest

from app import db
from app.modules.auth.models import User
from app.modules.conftest import login, logout
from app.modules.profile.models import UserProfile


@pytest.fixture(scope="module")
def test_client(test_client):
    """
    Extends the test_client fixture to add additional specific data for module testing.
    for module testing (por example, new users)
    """
    with test_client.application.app_context():
        user_test = User(email="user@example.com", password="test1234")
        db.session.add(user_test)
        db.session.commit()

        profile = UserProfile(user_id=user_test.id, name="Name", surname="Surname")
        db.session.add(profile)
        db.session.commit()

    yield test_client


def test_edit_profile_page_get(test_client):
    """
    Tests access to the profile editing page via a GET request.
    """
    login_response = login(test_client, "user@example.com", "test1234")
    assert login_response.status_code == 200, "Login was unsuccessful."

    response = test_client.get("/profile/edit")
    assert response.status_code == 200, "The profile editing page could not be accessed."
    assert b"Edit profile" in response.data, "The expected content is not present on the page"

    logout(test_client)


def test_edit_profile_post_success(test_client):
    """
    Tests updating profile data via POST request.
    This test verifies the fix for the 500 error when updating profile.
    """
    # Disable CSRF for testing
    test_client.application.config["WTF_CSRF_ENABLED"] = False

    login_response = login(test_client, "user@example.com", "test1234")
    assert login_response.status_code == 200, "Login was unsuccessful."

    # Submit updated profile data
    response = test_client.post(
        "/profile/edit",
        data={
            "name": "UpdatedName",
            "surname": "UpdatedSurname",
            "orcid": "",
            "affiliation": "",
        },
        follow_redirects=True,
    )

    # Should redirect to profile edit page with success message (not 500 error)
    assert response.status_code == 200, f"Profile update failed with status {response.status_code}"
    assert b"Profile updated successfully" in response.data, "Success message not shown"

    # Verify the profile was actually updated in the database
    with test_client.application.app_context():
        user = User.query.filter_by(email="user@example.com").first()
        profile = UserProfile.query.filter_by(user_id=user.id).first()
        assert profile.name == "UpdatedName", "Name was not updated in database"
        assert profile.surname == "UpdatedSurname", "Surname was not updated in database"

    logout(test_client)
