import pytest
import allure
from Pages.MicrosoftEarningsPage import MicrosoftEarningsPage

# Test 1: checks that the page title renders correctly after opening the URL
@allure.epic("Microsoft Investor Relations")
@allure.feature("Income Statements Report")
@pytest.mark.ui
@allure.title("Verify page title is displayed")
def test_page_title_is_displayed(driver):
    # Initialize the page object pattern by passing the live browser fixture
    page = MicrosoftEarningsPage(driver)
    # Navigate to the target URL defined in the page class
    page.open()

    # Wrap the assertion in an Allure step for detailed, granular reporting
    with allure.step("Verify 'Earnings Release' title is present on the page"):
        # Assert element visibility; returns a custom error message if the assertion fails
        assert page.is_displayed(page.PAGE_TITLE), "Page title is not displayed"


# Test 2: checks that key report elements are visible — a data row and a navigation tab
@allure.epic("Microsoft Investor Relations")
@allure.feature("Income Statements Report")
@pytest.mark.ui
@allure.title("Verify Total Revenue link and Balance Sheets tab are displayed")
def test_total_revenue_and_balance_sheets_tab_displayed(driver):
    # Initialize the page object pattern
    page = MicrosoftEarningsPage(driver)
    page.open()

    # Step 1: Check the visibility of the financial data row/link
    with allure.step("Verify 'Total revenue' link is present"):
        assert page.is_displayed(page.TOTAL_REVENUE_LINK), "Total revenue link is not displayed"

    # Step 2: Check the visibility of the navigation tab
    with allure.step("Verify 'Balance Sheets' navigation tab is present"):
        assert page.is_displayed(page.BALANCE_SHEETS_TAB), "Balance Sheets tab is not displayed"