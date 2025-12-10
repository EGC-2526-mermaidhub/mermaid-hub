import time

import pytest
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from core.environment.host import get_host_for_selenium_testing
from core.selenium.common import close_driver, initialize_driver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


def test_login_and_check_element():

    driver = initialize_driver()

    try:
        host = get_host_for_selenium_testing()

        # Open the login page
        driver.get(f"{host}/login")

        # Wait a little while to make sure the page has loaded completely
        time.sleep(4)

        # Find the username and password field and enter the values
        email_field = driver.find_element(By.NAME, "email")
        password_field = driver.find_element(By.NAME, "password")

        email_field.send_keys("user1@example.com")
        password_field.send_keys("1234")

        # Send the form
        password_field.send_keys(Keys.RETURN)

        # Wait a little while to ensure that the action has been completed
        time.sleep(4)

        try:

            driver.find_element(By.XPATH, "//h1[contains(@class, 'h2 mb-3') and contains(., 'Latest datasets')]")
            print("Test passed!")

        except NoSuchElementException:
            raise AssertionError("Test failed!")

    finally:

        # Close the browser
        close_driver(driver)


INCORRECT_EMAIL = "fail_selenium@example.com"
INCORRECT_PASSWORD = "wrongpassword"
CORRECT_EMAIL = "user1@example.com"
CORRECT_PASSWORD = "1234"
ATTEMPT_LIMIT = 6


@pytest.mark.run(order=-1)
def test_rate_limit_functional_block():

    driver = initialize_driver()
    host = get_host_for_selenium_testing()
    driver.get(f"{host}/login")

    EXACT_BLOCK_MESSAGE = "You have exceeded the allowed login attempt limit."

    BLOCK_MESSAGE_XPATH = "//span[contains(text(), 'exceeded the allowed login')]"

    try:

        for attempt in range(1, 6):

            WebDriverWait(driver, 5).until(
               EC.url_contains("/login")
            )

            email_field = driver.find_element(By.NAME, "email")
            password_field = driver.find_element(By.NAME, "password")

            email_field.clear()
            email_field.send_keys(INCORRECT_EMAIL)
            password_field.clear()
            password_field.send_keys(INCORRECT_PASSWORD)

            driver.find_element(By.ID, "login-submit").click()

            assert "/login" in driver.current_url, f"Login succeeded unexpectedly on attempt {attempt}!"
            print(f"Intento {attempt}: Fallido (correctamente).")
            time.sleep(0.5)

        email_field = driver.find_element(By.NAME, "email")
        password_field = driver.find_element(By.NAME, "password")

        email_field.clear()
        email_field.send_keys(CORRECT_EMAIL)
        password_field.clear()
        password_field.send_keys(CORRECT_PASSWORD)

        driver.find_element(By.ID, "login-submit").click()

        try:
            WebDriverWait(driver, 10).until(
               EC.presence_of_element_located((By.XPATH, BLOCK_MESSAGE_XPATH))
            )

            assert EXACT_BLOCK_MESSAGE in driver.page_source, "FAILURE: La frase de bloqueo no coincide en el código fuente."

            print("SUCCESS: El mensaje de bloqueo fue detectado en el DOM.")

        except TimeoutException:
            pytest.fail("FAILURE: El servidor redirigió, pero el mensaje de bloqueo no se cargó en la UI.")

        assert "/index" not in driver.current_url, "FAILURE: El usuario inició sesión a pesar del bloqueo."

        print("SUCCESS: El 6º intento (con credenciales correctas) fue BLOQUEADO.")

    except TimeoutException:
        pytest.fail("FAILURE: Tiempo de espera agotado. El servidor no respondió como se esperaba.")

    finally:
        close_driver(driver)
