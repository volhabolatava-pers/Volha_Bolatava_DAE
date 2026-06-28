import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.relative_locator import locate_with
from webdriver_manager.chrome import ChromeDriverManager

#'detach' was used to keep the Chrome window open after the script ends.
# This allows manual visual inspection of the final test state.
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)


# Function that draw a RED border
# # around the discovered element. It gives immediate visual proof that locator
# # targeted the correct, unique UI element on the screen.
def find_and_highlight(by_type, locator_string=None):
    # Handles advanced Relative Locators (which pass a single object)
    if locator_string is None and not isinstance(by_type, str):
        element = driver.find_element(by_type)
    else:
        # Handles standard locators (ID, Class Name, Name, CSS, XPath)
        element = driver.find_element(by_type, locator_string)

    # Injecting CSS border style directly into the live HTML DOM element
    driver.execute_script("arguments[0].style.border='4px solid #FF0000'; arguments[0].style.display='block';", element)
    time.sleep(1.2)  # Short pause to track the red highlight
    return element


try:
    # ----- https://phptravels.com/demo/ -----------
    driver.get("https://phptravels.com/demo/")
    time.sleep(4)  # Dynamic script loading delay

    # 1. CLASS_NAME: Good for finding elements structured via common style classes.
    first_name_class = find_and_highlight(By.CLASS_NAME, "first_name")
    last_name_class = find_and_highlight(By.CLASS_NAME, "last_name")

    # 2. By ID: IDs must be unique on the page.
    captcha_num1_id = find_and_highlight(By.ID, "numb1")
    captcha_num2_id = find_and_highlight(By.ID, "numb2")

    # ----- https://phptravels.org/register.php -----------
    driver.get("https://phptravels.org/register.php")
    time.sleep(4)

    # 3. By NAME
    first_name_name = find_and_highlight(By.NAME, "firstname")
    email_name = find_and_highlight(By.NAME, "email")

    # 4. By CSS_SELECTOR:
    last_name_css = find_and_highlight(By.CSS_SELECTOR, "#inputLastName")
    city_css = find_and_highlight(By.CSS_SELECTOR, "#inputCity")

    # ------https://phptravels.com/blog/ --------

    driver.get("https://phptravels.com/blog/")
    time.sleep(4)

    # 5. XPATH: Used when elements lack clean IDs or Names. It allows searching
    # directly for strict textual content inside specific HTML text headers.
    trends_header_xpath = find_and_highlight(By.XPATH, "//h2[contains(text(), '10 Travel Industry Trends')]")
    trends_desc_xpath = find_and_highlight(By.XPATH,"//p[contains(text(), 'By 2030, the travel industry will be shaped')]")


    # 6 RELATIVE LOCATORS

    #Returning to the first link ----- https://phptravels.com/demo/ -----------
    driver.get("https://phptravels.com/demo/")
    time.sleep(3)

    # fields initialised and used as geographic markers to find other elements nearby
    anchor_first_name = driver.find_element(By.CLASS_NAME, "first_name")
    anchor_last_name = driver.find_element(By.CLASS_NAME, "last_name")

    # looking for the input located to the right of the "First Name" field (this is "Last Name").
    last_name_relative = find_and_highlight(locate_with(By.TAG_NAME, "input").to_right_of(anchor_first_name))

    # looking for the input located to the left of the "Last Name" field (this is "First Name").
    first_name_relative = find_and_highlight(locate_with(By.TAG_NAME, "input").to_left_of(anchor_last_name))


except Exception as error:
    # If any locator fails the error will be catched and printed a clean log message.
    print(f"\n[ОШИБКА] Произошел сбой: {error}")

finally:
    #closes the browser
    driver.quit()

