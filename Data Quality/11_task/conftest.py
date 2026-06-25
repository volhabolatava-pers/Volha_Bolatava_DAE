import os
import pytest
import yaml
import psycopg2
import allure
from dotenv import load_dotenv

load_dotenv()

#Load SQL test cases configuration from YAML file
def load_sql_config():
    with open("config_queries.yaml", "r") as f:
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