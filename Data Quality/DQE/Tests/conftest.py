import os
import pytest
import yaml
import psycopg2
import allure
from dotenv import load_dotenv
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Load environment variables from .env file (DB credentials, etc.)
# This keeps sensitive data out of the source code
load_dotenv()

# Build an absolute path to config_queries.yaml regardless of where pytest is launched from.
# BASE_DIR goes two levels up from this file: Tests/ -> DQE/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_QUERIES_PATH = os.path.join(BASE_DIR, 'Configs', 'config_queries.yaml')

# =============================================================================
# DATABASE FIXTURES AND CONFIG
# =============================================================================
# These fixtures are used by Tests/test_database.py.
# pytest automatically injects them into any test function that declares
# a parameter with the same name

#Load SQL test cases configuration from YAML file
def load_sql_config():
    with open(CONFIG_QUERIES_PATH, "r") as f:
        return yaml.safe_load(f)

#Database connection fixture (provides a DB cursor to test functions)
@pytest.fixture(scope="function")
def db_cursor():
    # Setup: Open database connection before the test execution
    with allure.step("Establishing connection to PostgreSQL Database"):

        conn = psycopg2.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()

    yield cursor

    #Safely close cursor and connection after the test finishes
    with allure.step("Closing connection to PostgreSQL Database (Teardown)"):
        cursor.close()
        conn.close()

#Prepare datasets and IDs for test parametrization

config_data = load_sql_config()

smoke_params = [case for case in config_data["smoke_tests"]]
smoke_ids = [case["case_name"] for case in config_data["smoke_tests"]]

critical_params = [case for case in config_data["critical_tests"]]
critical_ids = [case["case_name"] for case in config_data["critical_tests"]]

# =============================================================================
# GCP / INTEGRATION FIXTURES
# =============================================================================

# These fixtures are used by Tests/test_integration.py.
# They are chained: list_gcs_blobs and list_staging_blobs both depend on
# provide_config, so pytest resolves and runs provide_config first automatically.
@pytest.fixture(scope='function')
def provide_config():
    """Returns a configuration dictionary containing bucket names,
    path prefixes, and an anonymous GCS client."""
    config = {
        # Source bucket — Contains the original NEXRAD radar weather data files
        'source_bucket_name': 'gcp-public-data-nexrad-l2',
        'source_prefix': '2024/01/01/KTLX/',

        # Staging bucket — Contains Landsat data
        'staging_bucket_name': 'gcp-public-data-landsat',
        'staging_prefix': 'LC08/01/001/002/',

        'gcp_storage_anon_client': storage.Client.create_anonymous_client()
    }
    return config


@pytest.fixture(scope='function')
def list_gcs_blobs(provide_config):
    """Connects to the source GCS bucket and returns a list of blob names."""
    config = provide_config
    blobs = config['gcp_storage_anon_client'].list_blobs(config['source_bucket_name'], prefix=config['source_prefix'])
    objects = [blob.name for blob in blobs]
    return objects


@pytest.fixture(scope='function')
def list_staging_blobs(provide_config):
    """Connects to the staging GCS bucket and returns a list of blob names."""
    config = provide_config
    blobs = config['gcp_storage_anon_client'].list_blobs(
        config['staging_bucket_name'],
        prefix=config['staging_prefix']
    )
    objects = [blob.name for blob in blobs]
    return objects


@pytest.fixture(scope='function')
def provide_posts_data():
    """Generates a list of 10 mock post payloads for API testing."""
    posts = []
    for i in range(1, 11):
        posts.append({
            "title": f"Stub Post {i}",
            "body": f"This is the body content for stub post number {i}.",
            "userId": 3
        })
    return posts

# =============================================================================
# SELENIUM / UI FIXTURES
# =============================================================================
# This fixture is used by Tests/test_power_bi.py.
# It manages the full Chrome browser lifecycle for each UI test.

@pytest.fixture(scope="function")
def driver():
    """Fixture that launches a Chrome browser instance for each UI test and
        closes it afterwards, regardless of whether the test passed or failed."""
    with allure.step("Initializing Chrome WebDriver"):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        service = Service(ChromeDriverManager().install())
        chrome_driver = webdriver.Chrome(service=service, options=options)

    yield chrome_driver

    with allure.step("Closing Chrome WebDriver (Teardown)"):
        chrome_driver.quit()

