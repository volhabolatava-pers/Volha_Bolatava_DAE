from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class MicrosoftEarningsPage:
    # Page URL — opened by the driver in open()

    URL = "https://www.microsoft.com/en-us/Investor/earnings/FY-2023-Q3/income-statements"

    # Locators — define how Selenium finds each element on the page.
    # Stored here so tests never contain raw XPath/selectors directly
    PAGE_TITLE = (By.XPATH, "//h1[contains(., 'Earnings Release')]")
    TOTAL_REVENUE_LINK = (By.XPATH, "//a[contains(text(), 'Total revenue')]")
    BALANCE_SHEETS_TAB = (By.LINK_TEXT, "Balance Sheets")

    def __init__(self, driver):
        # Store the WebDriver instance and set a 20s explicit wait for all checks
        self.driver = driver
        self.wait = WebDriverWait(driver, 20)

    def open(self):
        # Navigate the browser to the report page
        self.driver.get(self.URL)

    def is_displayed(self, locator):
        # Wait until the element is visible (not just present in DOM), then return its state.
        # Uses visibility_of_element_located so JS-rendered content has time to appear
        element = self.wait.until(EC.visibility_of_element_located(locator))
        return element.is_displayed()