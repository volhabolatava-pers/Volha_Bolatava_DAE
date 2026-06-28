import pytest
import requests
from google.cloud import storage
import allure

# =============================================================================
# TASK DESCRIPTION
# =============================================================================
# Original task:
#   The client used GCP Cloud for storing forecast data (source bucket).
#   Now the client wants to build analytics in AWS (staging bucket).
#   We need to verify that data for a specific date exists in both
#   the source (GCP) and the target staging bucket (AWS) as a smoke test.
#
# Modified task:
# The original task used AWS S3 bucket noaa-nexrad-level2 as the staging target.
# AWS closed anonymous access to this bucket, returning AccessDenied
# for all unauthenticated requests via boto3 with UNSIGNED config.
#
# Solution: The AWS staging bucket was replaced with gcp-public-data-landsat,
# a publicly accessible GCP bucket containing NASA/USGS Landsat satellite data.
# Both source and staging buckets are now on GCP and accessible anonymously,
# preserving the original test logic (verify both buckets are not empty).
#
# Updated task:
#   Source bucket:  gcp-public-data-nexrad-l2  (NEXRAD radar data, original source)
#   Staging bucket: gcp-public-data-landsat     (Landsat satellite data, simulates staging)
#   Both buckets are publicly accessible anonymously via GCP Storage API.
#   The test verifies that both buckets contain data for the given prefix,
#   which serves as a basic smoke test before running any analytics pipeline.
# =============================================================================

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

    with allure.step('Verify source GCP bucket file count'):
        assert len(list_gcs_blobs) > 0, "Source GCP bucket is empty for the specified date prefix!"

    with allure.step('Verify target staging bucket file count'):
        assert len(list_staging_blobs) > 0, "Target AWS staging bucket is empty for the specified date prefix!"

    with allure.step('Log discovery results'):
        print(f"\n[PASSED] Found {len(list_gcs_blobs)} items in source GCP and {len(list_staging_blobs)} items in staging GCP.")

        allure.attach(
            f'Source Blobs: {len(list_gcs_blobs)}\nStaging Blobs: {len(list_staging_blobs)}',
            name='Execution Statistics',
            attachment_type=allure.attachment_type.TEXT,
        )