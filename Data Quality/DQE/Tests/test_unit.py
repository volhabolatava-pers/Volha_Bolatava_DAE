import os
import pytest
import yaml

# Absolute path to config.yaml in Configs/ folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'Configs', 'config.yaml')

# Reads test cases from config.yaml and returns the 'cases' list
def get_numbers_data(config_name):
    with open(config_name, 'r') as stream:
        config = yaml.safe_load(stream)
    return config['cases']

# Business logic under test — raises a clear TypeError for non-numeric input
def add_numbers(a, b, c):
    try:
        return a + b + c
    except TypeError:
        raise TypeError('Please check the parameters. All of them must be numeric')

# Load test data from config at collection time so parametrize can use it
cases_data = get_numbers_data(CONFIG_PATH)

params = [case['input'] + [case['expected']] for case in cases_data]
case_ids = [case['case_name'] for case in cases_data]

# Smoke: runs once per case from config.yaml, checks that the sum is correct
@pytest.mark.smoke
@pytest.mark.parametrize("a, b, c, expected", params, ids=case_ids)
def test_add_numbers(a, b, c, expected):
    result = add_numbers(a, b, c)
    assert result == expected

# Critical: verifies that a non-numeric argument triggers the correct TypeError
@pytest.mark.critical
def test_add_floats():
    a, b, c = 'a', 2, 1
    with pytest.raises(TypeError) as exc_info:
        add_numbers(a, b, c)

    assert 'Please check the parameters' in str(exc_info.value)
