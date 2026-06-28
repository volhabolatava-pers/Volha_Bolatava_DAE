import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Chrome Options modify the browser's fingerprint to hide automated execution.
# This deactivates the 'navigator.webdriver' flag to prevent Google from detecting
# automated test software and triggering a reCAPTCHA block.
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(options=options)

# Implicit Wait is set to 10 seconds as a global baseline backup.
# If an element is not immediately present in the DOM during any find_element() call,
# Selenium polls the page continuously for up to 10 seconds before throwing an exception.
driver.implicitly_wait(10)

try:
    # Navigates to the main Google homepage.
    driver.get("https://www.google.com")

    # Implicit Wait automatically monitors the DOM until the search textarea is loaded.
    # The element is located by its standard 'NAME' attribute ("q").
    search_box = driver.find_element(By.NAME, "q")

    # Simulates organic keyboard interaction by typing the query and submitting the form.
    search_box.send_keys("Selenium")
    search_box.send_keys(Keys.RETURN)

    # A precise scoped XPath ("//div[@id='search']//h3") targets only the first <h3> header
    # inside the main organic search results container (#search).
    # This approach isolates the target from Google AI Overviews, sidecards, and ads.
    first_real_link = WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable((By.XPATH, "//div[@id='search']//h3")))

    # Click action triggers navigation to the official website
    first_real_link.click()
    print("[SUCCESS] The script completed steps 1-3 sequentially and opened the first organic link!")

except Exception as error:
    # Exception block prevents immediate script crash and logs the error details
    print(f"[ERROR] Automation execution failed: {error}")

finally:
    # A hard pause allows visual verification of the final state and screenshot capture.
    time.sleep(5)
    # Safe termination closes all active windows and kills the driver process
    driver.quit()