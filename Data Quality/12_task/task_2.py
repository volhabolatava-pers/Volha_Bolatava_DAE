import os # Standard library module to interact with the Operating System (e.g., managing paths or variables)
from selenium import webdriver # Master module containing all browser engine blueprints and constructors
from selenium.webdriver.edge.service import Service as EdgeService # Service class to manually control the background driver process

# 1. AUTOMATIC APPROACH (Chrome)
print("--- Launching Chrome (Automatic Approach) ---")

# Calling webdriver.Chrome() with empty parentheses triggers Selenium Manager.
# It automatically scans the system registry for chrome.exe, identifies its version,
# downloads the matching chromedriver binary into a hidden cache directory, and links it.
chrome_driver = webdriver.Chrome()

try:
    # Sends a JSON command via HTTP request instructing the browser to load the specific URL
    chrome_driver.get("https://www.google.com")
    print(f"Chrome Title: {chrome_driver.title}")
finally:
    # block run, ensuring the browser window closes and
    # the background chromedriver process is killed.
    chrome_driver.quit()


# 2. MANUAL APPROACH (Microsoft Edge)

print("\n--- Launching Microsoft Edge (Manual Approach / Hard Coded) ---")

# Allocate an absolute file path pointing directly to  local driver executable.
PATH_TO_EDGEDRIVER = "C:\Drivers\msedgedriver.exe"

# Instantiate EdgeService and manually inject the hard-coded driver path.
# This forces Selenium to bypass automated discovery and rely strictly on this location.
edge_service = EdgeService(executable_path=PATH_TO_EDGEDRIVER)

# Create the Edge session by explicitly passing manually configured service instance.
#  32-bit driver binary was used to match 32-bit Python runtime environment.
edge_driver = webdriver.Edge(service=edge_service)

try:
    # Instructs the manually initialized Edge instance to navigate to the Google landing page
    edge_driver.get("https://www.google.com")
    print(f"Edge Title: {edge_driver.title}")
finally:
    # Guarantees the teardown process: safely closes all open Edge tabs and shuts down the driver process
    edge_driver.quit()