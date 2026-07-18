import pytest
import requests
import allure


# Sends 10 POST requests to a public REST API and verifies all were created for the target user
@pytest.mark.integration
@allure.epic('API Integration Tests')
@allure.feature('Posts Management')
@allure.story('Create Mock Posts')
@allure.title('Verify bulk creation of stub posts for specific user')
@allure.description(
    'This test simulates sending 10 sequential POST requests to a public staging REST API and asserts that all 10 are successfully processed.'
)
def test_user_with_posts(provide_posts_data):
    # Setup API variables
    base_url = "https://jsonplaceholder.typicode.com/posts"
    created_posts_count = 0
    target_user_id = 3

    # Send POST requests for each mock item and verify success
    with allure.step('Iterate and send POST requests for 10 stub posts'):
        for post_payload in provide_posts_data:
            response = requests.post(base_url, json=post_payload)
            assert response.status_code == 201, f"Failed to create post: {response.text}"

            # Count successfully created posts matching the target user
            resp_json = response.json()
            if resp_json.get("userId") == target_user_id:
                created_posts_count += 1

    # Validate that all 10 posts were created successfully
    assert created_posts_count == 10, f"Expected 10 posts, but counted {created_posts_count}"

    with allure.step('Validate total count of successfully created posts'):
        assert (
                created_posts_count == 10
        ), f'Expected 10 posts, but counted {created_posts_count}'

# Verifies that both source and staging GCP buckets contain data for the given prefixes
@pytest.mark.integration
@allure.epic('Cloud Infrastructure Smoke Tests')
@allure.feature('Data Availability Verification')
@allure.story('Cross-Bucket Presence Check')
@allure.title(
    'Verify non-empty data presence between Source and Staging buckets'
)
@allure.description(
    'Connects anonymously to GCP Public Storage to ensure data exists for specified date prefixes in both raw radar and staging satellite layers.'
)
def test_data_is_presented_between_staging_raw(list_gcs_blobs, list_staging_blobs):
    # Smoke test: Verify that both cloud buckets are not empty
    # Assert source bucket is not empty
    with allure.step('Verify source GCP bucket file count'):
        assert len(list_gcs_blobs) > 0, "Source GCP bucket is empty for the specified date prefix!"

    # Assert staging bucket is not empty
    with allure.step('Verify target staging bucket file count'):
        assert len(list_staging_blobs) > 0, "Target AWS staging bucket is empty for the specified date prefix!"

    # Attach blob counts to Allure report for visibility
    with allure.step('Log discovery results'):
        print(f"\n[PASSED] Found {len(list_gcs_blobs)} items in source GCP and {len(list_staging_blobs)} items in staging GCP.")

        allure.attach(
            f'Source Blobs: {len(list_gcs_blobs)}\nStaging Blobs: {len(list_staging_blobs)}',
            name='Execution Statistics',
            attachment_type=allure.attachment_type.TEXT,
        )