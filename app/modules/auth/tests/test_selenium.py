import os
import time

import pytest
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from core.environment.host import get_host_for_selenium_testing
from core.selenium.common import close_driver, initialize_driver

# Lógica para detectar si el Rate Limit está activo
# Si LOAD_TESTS es 'True', el Rate Limit está desactivado en el backend.
load_tests_env = os.getenv("LOAD_TESTS", "False").lower()
RATE_LIMITING_DISABLED = load_tests_env == "true"


def test_login_and_check_element():
    driver = initialize_driver()
    try:
        host = get_host_for_selenium_testing()
        driver.get(f"{host}/login")

        # Esperar a que el campo email sea visible (más robusto que sleep)
        email_field = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.NAME, "email")))
        password_field = driver.find_element(By.NAME, "password")

        email_field.send_keys("user1@example.com")
        password_field.send_keys("1234")
        password_field.send_keys(Keys.RETURN)

        # Esperar a que la URL cambie o aparezca el elemento dashboard
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h1[contains(., 'Latest datasets')]")))
            print("Test passed! Element found.")
        except TimeoutException:
            # Check rápido por si estamos bloqueados y por eso falló este test
            if "exceeded" in driver.page_source:
                pytest.fail("FAILURE: El test de login falló porque la IP está bloqueada por intentos anteriores.")
            raise AssertionError("Test failed! No se encontró el elemento 'Latest datasets' tras el login.")

    finally:
        close_driver(driver)


INCORRECT_EMAIL = "fail_selenium@example.com"
INCORRECT_PASSWORD = "wrongpassword"
CORRECT_EMAIL = "user1@example.com"
CORRECT_PASSWORD = "1234"


# ESTE TEST SE SALTARÁ SI LOAD_TESTS=TRUE
@pytest.mark.run(order=-1)
@pytest.mark.skipif(RATE_LIMITING_DISABLED, reason="Rate limiting desactivado por entorno (LOAD_TESTS=True)")
def test_rate_limit_functional_block():
    driver = initialize_driver()
    host = get_host_for_selenium_testing()
    driver.get(f"{host}/login")

    # Usamos un XPATH más genérico para encontrar el mensaje en cualquier etiqueta
    BLOCK_MESSAGE_XPATH = "//*[contains(text(), 'exceeded the allowed login')]"
    EXACT_BLOCK_MESSAGE = "You have exceeded the allowed login attempt limit."

    try:
        # Intentos fallidos
        for attempt in range(1, 6):
            WebDriverWait(driver, 5).until(EC.url_contains("/login"))

            email_field = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.NAME, "email")))
            password_field = driver.find_element(By.NAME, "password")

            email_field.clear()
            email_field.send_keys(INCORRECT_EMAIL)
            password_field.clear()
            password_field.send_keys(INCORRECT_PASSWORD)

            driver.find_element(By.ID, "login-submit").click()

            # Pequeña espera para asegurar que el backend procesa el fallo
            time.sleep(1)
            print(f"Intento {attempt}: Enviado incorrectamente.")

        # Intento 6: Credenciales correctas
        print("Realizando intento 6 con credenciales correctas (Esperando bloqueo)...")
        email_field = driver.find_element(By.NAME, "email")
        password_field = driver.find_element(By.NAME, "password")

        email_field.clear()
        email_field.send_keys(CORRECT_EMAIL)
        password_field.clear()
        password_field.send_keys(CORRECT_PASSWORD)

        driver.find_element(By.ID, "login-submit").click()

        try:
            # Esperamos que aparezca el mensaje de error
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, BLOCK_MESSAGE_XPATH)))
            assert EXACT_BLOCK_MESSAGE in driver.page_source
            print("SUCCESS: El mensaje de bloqueo fue detectado.")

        except TimeoutException:
            # Si entramos aquí, es que NO apareció el mensaje de bloqueo
            if "/login" not in driver.current_url:
                pytest.fail(f"FAILURE: El bloqueo falló. El usuario entró a: {driver.current_url}")
            else:
                pytest.fail("FAILURE: Seguimos en login pero no se encontró el mensaje de error específico.")

        assert "/index" not in driver.current_url, "FAILURE: El usuario inició sesión a pesar del bloqueo."

    finally:
        close_driver(driver)
