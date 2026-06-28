import pytest
import yaml


def get_numbers_data(config_name):
    with open(config_name, 'r') as stream:
        config = yaml.safe_load(stream)
    return config['cases']


def add_numbers(a, b, c):
    try:
        return a + b + c
    except TypeError:
        raise TypeError('Please check the parameters. All of them must be numeric')

cases_data = get_numbers_data('config.yaml')

params = [case['input'] + [case['expected']] for case in cases_data]
case_ids = [case['case_name'] for case in cases_data]

@pytest.mark.smoke
@pytest.mark.parametrize("a, b, c, expected", params, ids=case_ids)
def test_add_numbers(a, b, c, expected):
    result = add_numbers(a, b, c)
    assert result == expected


@pytest.mark.critical
def test_add_floats():
    a, b, c = 'a', 2, 1
    with pytest.raises(TypeError) as exc_info:
        add_numbers(a, b, c)

    assert 'Please check the parameters' in str(exc_info.value)