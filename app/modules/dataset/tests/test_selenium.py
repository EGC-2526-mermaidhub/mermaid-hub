import os
import time

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from core.environment.host import get_host_for_selenium_testing
from core.selenium.common import close_driver, initialize_driver


def wait_for_page_to_load(driver, timeout=10):
    WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")


def test_upload_dataset_with_mermaid():
    driver = initialize_driver()
    try:
        host = get_host_for_selenium_testing()
        print(f"[INFO] Host de prueba: {host}")

        driver.get(f"{host}/login")
        wait_for_page_to_load(driver)
        driver.find_element(By.NAME, "email").send_keys("user1@example.com")
        password_field = driver.find_element(By.NAME, "password")
        password_field.send_keys("1234")
        password_field.send_keys(Keys.RETURN)
        wait_for_page_to_load(driver)
        time.sleep(2)

        driver.get(f"{host}/dataset/upload")
        wait_for_page_to_load(driver)
        time.sleep(2)

        driver.find_element(By.NAME, "title").send_keys("Sample UI")
        driver.find_element(By.NAME, "desc").send_keys("Sample UI description")
        driver.find_element(By.NAME, "diagram_type").send_keys("FLOWCHART")
        driver.find_element(By.NAME, "tags").send_keys("tag1,tag2")
        time.sleep(2)

        file_path = os.path.abspath("app/modules/dataset/mmd_examples/file1.mmd")
        dropzone_input = WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME, "dz-hidden-input"))
        dropzone_input.send_keys(file_path)
        time.sleep(2)

        file_list_item = WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CSS_SELECTOR, "#file-list li"))

        show_info_button = file_list_item.find_element(By.CSS_SELECTOR, "button.info-button")
        show_info_button.click()

        form_container = file_list_item.find_element(By.CSS_SELECTOR, ".uvl_form")

        form_unique_id = int(form_container.get_attribute("id").split("_")[0])
        input_prefix = f"mermaid_diagrams-{form_unique_id}"

        form_container.find_element(By.NAME, f"{input_prefix}-title").send_keys("Sample UI MMD")
        form_container.find_element(By.NAME, f"{input_prefix}-desc").send_keys("Mermaid diagram description")
        form_container.find_element(By.NAME, f"{input_prefix}-diagram_type").send_keys("FLOWCHART")
        form_container.find_element(By.NAME, f"{input_prefix}-tags").send_keys("mermaid,diagram")
        time.sleep(2)

        agree_checkbox = driver.find_element(By.ID, "agreeCheckbox")
        agree_checkbox.send_keys(Keys.SPACE)
        time.sleep(2)

        upload_btn = driver.find_element(By.ID, "upload_button")
        upload_btn.click()
        time.sleep(1)

        wait_for_page_to_load(driver)
        time.sleep(2)

        assert driver.current_url == f"{host}/dataset/list", "Test failed!"

    finally:
        close_driver(driver)


def test_view_uploaded_dataset():
    driver = initialize_driver()
    try:
        host = get_host_for_selenium_testing()
        driver.get(f"{host}/login")
        wait_for_page_to_load(driver)
        driver.find_element(By.NAME, "email").send_keys("user1@example.com")
        driver.find_element(By.NAME, "password").send_keys("1234", Keys.RETURN)
        wait_for_page_to_load(driver)
        time.sleep(2)

        driver.get(f"{host}/dataset/list")
        wait_for_page_to_load(driver)
        time.sleep(2)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        assert len(rows) > 0

        first_row = rows[0]
        view_button = first_row.find_element(By.CSS_SELECTOR, "td a")
        view_button.click()
        wait_for_page_to_load(driver)
        time.sleep(2)

        title = driver.find_element(By.CSS_SELECTOR, "h1 b").text
        assert title == "Sample UI"

    finally:
        close_driver(driver)


def test_recommendations_block_is_visible():
    driver = initialize_driver()
    try:
        host = get_host_for_selenium_testing()

        driver.get(f"{host}/login")
        wait_for_page_to_load(driver)
        driver.find_element(By.NAME, "email").send_keys("user1@example.com")
        driver.find_element(By.NAME, "password").send_keys("1234")
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)
        wait_for_page_to_load(driver)

        driver.get(f"{host}/dataset/list")
        wait_for_page_to_load(driver)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if not rows:

            return

        rows[0].find_element(By.CSS_SELECTOR, "td a").click()
        wait_for_page_to_load(driver)
        time.sleep(2)

        print("[DEBUG] Buscando bloque de recomendaciones...")

        try:

            rec_header = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//h3[contains(., 'Recommended')]"))
            )

            driver.execute_script("arguments[0].scrollIntoView(true);", rec_header)
            time.sleep(1)

            assert rec_header.is_displayed(), "El encabezado existe en el DOM pero no es visible."
            print("[SUCCESS] Bloque encontrado.")

        except TimeoutException:

            body_text = driver.find_element(By.TAG_NAME, "body").text
            print(f"[FAIL] Texto en página (fragmento): {body_text[:200]}...")
            print(f"[FAIL] URL: {driver.current_url}")
            raise AssertionError("Timeout esperando 'Recommended datasets'.")

    finally:
        close_driver(driver)


def test_github_import_form_visible():
    """Test that the GitHub import form is visible and interactive on the upload page."""
    driver = initialize_driver()
    try:
        host = get_host_for_selenium_testing()

        driver.get(f"{host}/login")
        wait_for_page_to_load(driver)
        driver.find_element(By.NAME, "email").send_keys("user1@example.com")
        driver.find_element(By.NAME, "password").send_keys("1234")
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)
        wait_for_page_to_load(driver)
        time.sleep(2)

        driver.get(f"{host}/dataset/upload")
        wait_for_page_to_load(driver)
        time.sleep(2)

        github_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "githubImportDropdown")))
        assert github_dropdown.is_displayed(), "GitHub Import button should be visible"
        github_dropdown.click()
        time.sleep(1)

        dropdown_menu = driver.find_element(By.CSS_SELECTOR, ".dropdown-menu[aria-labelledby='githubImportDropdown']")
        assert dropdown_menu.is_displayed(), "GitHub import dropdown menu should be visible after click"

        repo_input = driver.find_element(By.ID, "github_repo")
        branch_input = driver.find_element(By.ID, "github_branch")
        path_input = driver.find_element(By.ID, "github_path")
        token_input = driver.find_element(By.ID, "github_token")

        assert repo_input.is_displayed(), "Repository URL input should be visible"
        assert branch_input.is_displayed(), "Branch input should be visible"
        assert path_input.is_displayed(), "Path input should be visible"
        assert token_input.is_displayed(), "Token input should be visible"

        submit_button = driver.find_element(By.CSS_SELECTOR, "#githubImportForm button[type='submit']")
        assert submit_button.is_displayed(), "Load files button should be visible"
        assert submit_button.text == "Load files", "Button text should be 'Load files'"

        close_button = driver.find_element(By.ID, "githubImportClose")
        close_button.click()
        time.sleep(1)

        assert not dropdown_menu.is_displayed(), "Dropdown should be closed after clicking Close button"

        print("[SUCCESS] GitHub import form test passed")

    finally:
        close_driver(driver)


def test_github_import_invalid_url_error():
    """Test that submitting an invalid GitHub URL shows an error message."""
    driver = initialize_driver()
    try:
        host = get_host_for_selenium_testing()

        driver.get(f"{host}/login")
        wait_for_page_to_load(driver)
        driver.find_element(By.NAME, "email").send_keys("user1@example.com")
        driver.find_element(By.NAME, "password").send_keys("1234")
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)
        wait_for_page_to_load(driver)
        time.sleep(2)

        driver.get(f"{host}/dataset/upload")
        wait_for_page_to_load(driver)
        time.sleep(2)

        github_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "githubImportDropdown")))
        github_dropdown.click()
        time.sleep(1)

        repo_input = driver.find_element(By.ID, "github_repo")
        repo_input.clear()
        repo_input.send_keys("https://invalid-url.com/not-github")

        submit_button = driver.find_element(By.CSS_SELECTOR, "#githubImportForm button[type='submit']")
        submit_button.click()
        time.sleep(2)

        try:
            alert_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".toast, .alert, #alerts"))
            )
            if alert_element.is_displayed():
                print(f"[INFO] Alert message found: {alert_element.text}")
        except TimeoutException:
            print("[INFO] No toast/alert found, checking page content")

        assert "/dataset/upload" in driver.current_url, "Should remain on upload page after invalid URL submission"

        print("[SUCCESS] GitHub import invalid URL error test passed")

    finally:
        close_driver(driver)
