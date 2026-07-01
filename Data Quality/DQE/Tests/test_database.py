import pytest
import allure
from conftest import smoke_params, smoke_ids, critical_params, critical_ids


@allure.epic("DWH Automation Framework")
@allure.feature("Dwh_clients table Checks")
@allure.suite("Smoke: Structural Presence Validation")
@pytest.mark.smoke
@pytest.mark.parametrize("case", smoke_params, ids=smoke_ids)
def test_db_smoke_structure(db_cursor, case):

    allure.dynamic.title(f"Smoke: {case['case_name']}")

    with allure.step("Executing SQL metadata validation query"):
        allure.attach(case['query'], name="SQL Query Executed", attachment_type=allure.attachment_type.TEXT)

        db_cursor.execute(case['query'])
        result = db_cursor.fetchone()[0]

    with allure.step(f"Validation: Expected {case['expected']}, Got {result}"):
        assert result == case['expected'], f"Failed! Schema structure is broken for case: {case['case_name']}"

@allure.epic("DWH Automation Framework")
@allure.feature("Dwh_clients table Checks")
@allure.suite("Critical Path: Data Integrity Checks")
@pytest.mark.critical
@pytest.mark.parametrize("case", critical_params, ids=critical_ids)
def test_db_critical_data_quality(db_cursor, case):

    allure.dynamic.title(f"Critical Path: {case['case_name']}")

    with allure.step("Executing Data Quality validation query"):
        allure.attach(case['query'], name="SQL Query Executed", attachment_type=allure.attachment_type.TEXT)

        db_cursor.execute(case['query'])
        result = db_cursor.fetchone()[0]

    with allure.step(f"Validation: Expected {case['expected']}, Got {result}"):
        assert result == case['expected'], f"Failed! Schema structure is broken for case: {case['case_name']}"