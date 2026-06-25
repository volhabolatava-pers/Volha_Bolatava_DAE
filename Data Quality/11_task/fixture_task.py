import time
import pytest


@pytest.fixture(scope="session", autouse=True)
def track_suite_time():
    start_time = time.time()
    print(f"\n--- [SUITE START] Starting test suite execution ---")

    yield

    end_time = time.time()
    total_time = end_time - start_time
    print(f"\n--- [SUITE END] Total suite execution time: {total_time:.2f} seconds ---")

@pytest.fixture()
def track_test_time():
    start_time = time.time()

    yield

    end_time = time.time()
    total_time = end_time - start_time
    print(f"\n[TEST TIME] Test execution took: {total_time:.2f} seconds")

def add_numbers(a, b):
    return a + b


def test_add_two_positive_numbers(track_test_time):
    a, b = 3, 5
    result = add_numbers(a, b)
    time.sleep(2)
    assert result == 8
    assert result == 8


def test_add_two_negative_numbers(track_test_time):
    a, b = -3, -5
    result = add_numbers(a, b)
    time.sleep(3)
    assert result == -8


def test_add_negative_and_positive_numbers():
    a, b = -3, 5
    result = add_numbers(a, b)
    time.sleep(10)
    assert result == 2
