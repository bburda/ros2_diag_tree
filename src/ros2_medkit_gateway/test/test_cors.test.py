#!/usr/bin/env python3
# Copyright 2025 bburda
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
CORS (Cross-Origin Resource Sharing) integration tests for ROS 2 Medkit Gateway.

Tests verify:
1. CORS headers are set correctly for allowed origins
2. CORS headers are NOT set for disallowed origins
3. OPTIONS preflight requests return 204 with appropriate headers
4. Max-Age header is only set for allowed origins (security)
5. Credentials header behavior (enabled vs disabled)
6. Multiple allowed origins work correctly
"""

import os
import time
import unittest

from launch import LaunchDescription
from launch.actions import TimerAction
import launch_ros.actions
import launch_testing.actions
import requests


def get_coverage_env():
    """
    Get environment variables for gcov coverage data collection.

    When running with coverage enabled (ENABLE_COVERAGE=ON), subprocess nodes
    need GCOV_PREFIX set to write coverage data to the correct build directory.
    This allows integration test coverage to be captured alongside unit tests.

    Returns
    -------
    dict
        Environment variables dict with GCOV_PREFIX and GCOV_PREFIX_STRIP,
        or empty dict if coverage path cannot be determined.

    """
    try:
        from ament_index_python.packages import get_package_prefix
        pkg_prefix = get_package_prefix('ros2_medkit_gateway')
        # pkg_prefix is like /path/to/workspace/install/ros2_medkit_gateway
        # workspace is 2 levels up from install/package_name
        workspace = os.path.dirname(os.path.dirname(pkg_prefix))
        build_dir = os.path.join(workspace, 'build', 'ros2_medkit_gateway')

        if os.path.exists(build_dir):
            # GCOV_PREFIX_STRIP removes leading path components from compiled-in paths
            # GCOV_PREFIX prepends the new path for .gcda file output
            return {
                'GCOV_PREFIX': build_dir,
                'GCOV_PREFIX_STRIP': str(workspace.count(os.sep) + 1),
            }
    except Exception:
        # Ignore: if coverage environment cannot be determined,
        # return empty dict so tests proceed without coverage data.
        pass
    return {}


# Test configuration - two gateway instances with different CORS settings
GATEWAY_HOST = '127.0.0.1'
GATEWAY_PORT_NO_CREDS = 8085  # CORS without credentials
GATEWAY_PORT_WITH_CREDS = 8086  # CORS with credentials enabled
ALLOWED_ORIGIN = 'http://localhost:5173'
ALLOWED_ORIGIN_2 = 'http://localhost:3000'
DISALLOWED_ORIGIN = 'http://evil.com'
# API version prefix - must match rest_server.cpp
API_BASE_PATH = '/api/v1'


def generate_test_description():
    """Generate launch description with two gateway nodes for CORS testing."""
    # Get coverage environment variables for subprocess coverage collection
    coverage_env = get_coverage_env()

    # Gateway 1: CORS enabled, credentials disabled
    gateway_no_creds = launch_ros.actions.Node(
        package='ros2_medkit_gateway',
        executable='gateway_node',
        name='gateway_cors_no_creds',
        output='screen',
        parameters=[{
            'server.host': GATEWAY_HOST,
            'server.port': GATEWAY_PORT_NO_CREDS,
            'cors.allowed_origins': [ALLOWED_ORIGIN, ALLOWED_ORIGIN_2],
            'cors.allowed_methods': ['GET', 'PUT', 'OPTIONS'],
            'cors.allowed_headers': ['Content-Type', 'Accept', 'Authorization'],
            'cors.allow_credentials': False,
            'cors.max_age_seconds': 3600,
        }],
        additional_env=coverage_env,
    )

    # Gateway 2: CORS enabled, credentials enabled
    gateway_with_creds = launch_ros.actions.Node(
        package='ros2_medkit_gateway',
        executable='gateway_node',
        name='gateway_cors_with_creds',
        output='screen',
        parameters=[{
            'server.host': GATEWAY_HOST,
            'server.port': GATEWAY_PORT_WITH_CREDS,
            'cors.allowed_origins': [ALLOWED_ORIGIN],
            'cors.allowed_methods': ['GET', 'PUT', 'OPTIONS'],
            'cors.allowed_headers': ['Content-Type', 'Accept', 'Authorization'],
            'cors.allow_credentials': True,
            'cors.max_age_seconds': 3600,
        }],
        additional_env=coverage_env,
    )

    # Delay before running tests to allow gateways to start
    delayed_test = TimerAction(
        period=2.0,
        actions=[launch_testing.actions.ReadyToTest()],
    )

    return LaunchDescription([
        gateway_no_creds,
        gateway_with_creds,
        delayed_test,
    ])


class TestCorsIntegration(unittest.TestCase):
    """CORS integration tests."""

    BASE_URL_NO_CREDS = f'http://{GATEWAY_HOST}:{GATEWAY_PORT_NO_CREDS}{API_BASE_PATH}'
    BASE_URL_WITH_CREDS = f'http://{GATEWAY_HOST}:{GATEWAY_PORT_WITH_CREDS}{API_BASE_PATH}'

    @classmethod
    def setUpClass(cls):
        """Wait for both gateways to be ready."""
        for base_url, name in [
            (cls.BASE_URL_NO_CREDS, 'no-creds'),
            (cls.BASE_URL_WITH_CREDS, 'with-creds'),
        ]:
            max_retries = 30
            for i in range(max_retries):
                try:
                    response = requests.get(f'{base_url}/health', timeout=1)
                    if response.status_code == 200:
                        print(f'Gateway ({name}) ready after {i + 1} attempts')
                        break
                except requests.exceptions.ConnectionError:
                    # Ignore connection errors while waiting for gateway to start
                    pass
                time.sleep(0.5)
            else:
                raise RuntimeError(f'Gateway ({name}) failed to start within timeout')

    def test_01_cors_headers_for_allowed_origin(self):
        """Test CORS headers are set for allowed origins."""
        response = requests.get(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={'Origin': ALLOWED_ORIGIN},
            timeout=5
        )
        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            response.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN,
            'Access-Control-Allow-Origin should match the request origin'
        )
        self.assertIn(
            'GET',
            response.headers.get('Access-Control-Allow-Methods', ''),
            'Access-Control-Allow-Methods should include GET'
        )
        self.assertIn(
            'Content-Type',
            response.headers.get('Access-Control-Allow-Headers', ''),
            'Access-Control-Allow-Headers should include Content-Type'
        )

    def test_02_cors_headers_not_set_for_disallowed_origin(self):
        """Test CORS headers are NOT set for disallowed origins."""
        response = requests.get(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={'Origin': DISALLOWED_ORIGIN},
            timeout=5
        )
        # Request succeeds (CORS is browser-enforced), but no CORS headers
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Origin'),
            'Access-Control-Allow-Origin should NOT be set for disallowed origin'
        )

    def test_03_preflight_options_request(self):
        """Test OPTIONS preflight returns 204 with CORS headers."""
        response = requests.options(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={
                'Origin': ALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'Content-Type',
            },
            timeout=5
        )
        self.assertEqual(response.status_code, 204, 'OPTIONS preflight should return 204')

        self.assertEqual(
            response.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN
        )
        self.assertIn('GET', response.headers.get('Access-Control-Allow-Methods', ''))
        self.assertIn('Content-Type', response.headers.get('Access-Control-Allow-Headers', ''))
        self.assertEqual(
            response.headers.get('Access-Control-Max-Age'),
            '3600',
            'Access-Control-Max-Age should be set for preflight'
        )

    def test_04_preflight_disallowed_origin_no_cors_headers(self):
        """Test preflight from disallowed origin returns 403 Forbidden."""
        response = requests.options(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={
                'Origin': DISALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'GET',
            },
            timeout=5
        )
        # Returns 403 Forbidden for disallowed origins (prevents endpoint discovery)
        self.assertEqual(response.status_code, 403)
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Origin'),
            'CORS headers should NOT be set for disallowed origin'
        )
        self.assertIsNone(
            response.headers.get('Access-Control-Max-Age'),
            'Max-Age should NOT be set for disallowed origin'
        )

    def test_05_cors_on_put_endpoint(self):
        """Test that CORS preflight works for PUT requests."""
        response = requests.options(
            f'{self.BASE_URL_NO_CREDS}/components/test/data/test_topic',
            headers={
                'Origin': ALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'PUT',
                'Access-Control-Request-Headers': 'Content-Type',
            },
            timeout=5
        )
        self.assertEqual(response.status_code, 204)
        self.assertIn(
            'PUT',
            response.headers.get('Access-Control-Allow-Methods', ''),
            'PUT should be in allowed methods'
        )

    def test_06_no_cors_headers_without_origin(self):
        """Test that requests without Origin header don't get CORS headers."""
        response = requests.get(f'{self.BASE_URL_NO_CREDS}/health', timeout=5)
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Origin'),
            'CORS headers should NOT be set when Origin is not provided'
        )

    def test_07_multiple_allowed_origins(self):
        """Test that multiple configured origins are all allowed."""
        # First origin
        response1 = requests.get(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={'Origin': ALLOWED_ORIGIN},
            timeout=5
        )
        self.assertEqual(
            response1.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN
        )

        # Second origin
        response2 = requests.get(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={'Origin': ALLOWED_ORIGIN_2},
            timeout=5
        )
        self.assertEqual(
            response2.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN_2
        )

    def test_08_credentials_header_not_set_when_disabled(self):
        """Test that credentials header is NOT set when disabled."""
        response = requests.get(
            f'{self.BASE_URL_NO_CREDS}/health',
            headers={'Origin': ALLOWED_ORIGIN},
            timeout=5
        )
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Credentials'),
            'Credentials header should NOT be set when disabled'
        )

    def test_09_credentials_header_set_when_enabled(self):
        """Test that credentials header is 'true' when enabled."""
        response = requests.get(
            f'{self.BASE_URL_WITH_CREDS}/health',
            headers={'Origin': ALLOWED_ORIGIN},
            timeout=5
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN
        )
        self.assertEqual(
            response.headers.get('Access-Control-Allow-Credentials'),
            'true',
            'Credentials header should be "true" when enabled'
        )

    def test_10_credentials_in_preflight(self):
        """Test that credentials header is in preflight when enabled."""
        response = requests.options(
            f'{self.BASE_URL_WITH_CREDS}/health',
            headers={
                'Origin': ALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'Content-Type',
            },
            timeout=5
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(
            response.headers.get('Access-Control-Allow-Credentials'),
            'true',
            'Preflight should include Access-Control-Allow-Credentials: true'
        )
