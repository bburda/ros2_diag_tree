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
1. CORS headers are set correctly when enabled
2. OPTIONS preflight requests return 204
3. Requests from allowed origins receive CORS headers
4. Requests from disallowed origins don't receive CORS headers
5. CORS is disabled when not configured
"""

import time
import unittest

from launch import LaunchDescription
from launch.actions import TimerAction
import launch_ros.actions
import launch_testing.actions
import requests


# Test configuration
GATEWAY_HOST = '127.0.0.1'
GATEWAY_PORT = 8085  # Different port to avoid conflicts
ALLOWED_ORIGIN = 'http://localhost:5173'
DISALLOWED_ORIGIN = 'http://evil.com'


def generate_test_description():
    """Generate launch description with gateway node configured for CORS testing."""
    # Launch the ROS 2 Medkit Gateway node with CORS enabled
    gateway_node = launch_ros.actions.Node(
        package='ros2_medkit_gateway',
        executable='gateway_node',
        name='ros2_medkit_gateway_cors_test',
        output='screen',
        parameters=[{
            'server.host': GATEWAY_HOST,
            'server.port': GATEWAY_PORT,
            'cors.allowed_origins': [ALLOWED_ORIGIN, 'http://localhost:3000'],
            'cors.allowed_methods': ['GET', 'PUT', 'OPTIONS'],
            'cors.allowed_headers': ['Content-Type', 'Accept', 'Authorization'],
            'cors.allow_credentials': False,
            'cors.max_age_seconds': 3600,
        }],
    )

    # Delay before running tests to allow gateway to start
    delayed_test = TimerAction(
        period=2.0,
        actions=[launch_testing.actions.ReadyToTest()],
    )

    return LaunchDescription([
        gateway_node,
        delayed_test,
    ])


class TestCorsIntegration(unittest.TestCase):
    """CORS integration tests."""

    BASE_URL = f'http://{GATEWAY_HOST}:{GATEWAY_PORT}'

    @classmethod
    def setUpClass(cls):
        """Wait for gateway to be ready."""
        max_retries = 30
        for i in range(max_retries):
            try:
                response = requests.get(f'{cls.BASE_URL}/health', timeout=1)
                if response.status_code == 200:
                    print(f'Gateway ready after {i + 1} attempts')
                    return
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(0.5)
        raise RuntimeError('Gateway failed to start within timeout')

    def test_01_cors_headers_for_allowed_origin(self):
        """
        Test that CORS headers are set for requests from allowed origins.
        """
        response = requests.get(
            f'{self.BASE_URL}/health',
            headers={'Origin': ALLOWED_ORIGIN},
            timeout=5
        )
        self.assertEqual(response.status_code, 200)

        # Verify CORS headers
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

        print('✓ CORS headers for allowed origin test passed')

    def test_02_cors_headers_not_set_for_disallowed_origin(self):
        """
        Test that CORS headers are NOT set for requests from disallowed origins.
        """
        response = requests.get(
            f'{self.BASE_URL}/health',
            headers={'Origin': DISALLOWED_ORIGIN},
            timeout=5
        )
        # Request should still succeed (CORS is browser-enforced)
        self.assertEqual(response.status_code, 200)

        # But CORS headers should NOT be present
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Origin'),
            'Access-Control-Allow-Origin should NOT be set for disallowed origin'
        )

        print('✓ CORS headers not set for disallowed origin test passed')

    def test_03_preflight_options_request(self):
        """
        Test that OPTIONS preflight requests return 204 with CORS headers.
        """
        response = requests.options(
            f'{self.BASE_URL}/health',
            headers={
                'Origin': ALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'GET',
                'Access-Control-Request-Headers': 'Content-Type',
            },
            timeout=5
        )
        self.assertEqual(
            response.status_code,
            204,
            'OPTIONS preflight should return 204 No Content'
        )

        # Verify CORS headers
        self.assertEqual(
            response.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN
        )
        self.assertIn(
            'GET',
            response.headers.get('Access-Control-Allow-Methods', '')
        )
        self.assertIn(
            'Content-Type',
            response.headers.get('Access-Control-Allow-Headers', '')
        )

        # Max-Age should be set for preflight
        self.assertEqual(
            response.headers.get('Access-Control-Max-Age'),
            '3600',
            'Access-Control-Max-Age should be set for preflight'
        )

        print('✓ OPTIONS preflight request test passed')

    def test_04_preflight_options_disallowed_origin(self):
        """
        Test that OPTIONS preflight from disallowed origin doesn't get CORS headers.
        """
        response = requests.options(
            f'{self.BASE_URL}/health',
            headers={
                'Origin': DISALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'GET',
            },
            timeout=5
        )
        # Should still return 204 but without CORS headers
        self.assertEqual(response.status_code, 204)
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Origin'),
            'CORS headers should NOT be set for disallowed origin in preflight'
        )

        print('✓ OPTIONS preflight disallowed origin test passed')

    def test_05_cors_on_put_endpoint(self):
        """
        Test that CORS works on PUT endpoints.
        """
        # First do a preflight
        preflight = requests.options(
            f'{self.BASE_URL}/components/test/data/test_topic',
            headers={
                'Origin': ALLOWED_ORIGIN,
                'Access-Control-Request-Method': 'PUT',
                'Access-Control-Request-Headers': 'Content-Type',
            },
            timeout=5
        )
        self.assertEqual(preflight.status_code, 204)
        self.assertIn(
            'PUT',
            preflight.headers.get('Access-Control-Allow-Methods', ''),
            'PUT should be in allowed methods'
        )

        print('✓ CORS on PUT endpoint test passed')

    def test_06_no_cors_headers_without_origin(self):
        """
        Test that requests without Origin header don't get CORS headers.
        """
        response = requests.get(
            f'{self.BASE_URL}/health',
            timeout=5
        )
        self.assertEqual(response.status_code, 200)

        # No Origin header = no CORS headers in response
        self.assertIsNone(
            response.headers.get('Access-Control-Allow-Origin'),
            'CORS headers should NOT be set when Origin is not provided'
        )

        print('✓ No CORS headers without Origin test passed')

    def test_07_multiple_allowed_origins(self):
        """
        Test that multiple origins can be allowed.
        """
        # Test first allowed origin
        response1 = requests.get(
            f'{self.BASE_URL}/health',
            headers={'Origin': ALLOWED_ORIGIN},
            timeout=5
        )
        self.assertEqual(
            response1.headers.get('Access-Control-Allow-Origin'),
            ALLOWED_ORIGIN
        )

        # Test second allowed origin
        response2 = requests.get(
            f'{self.BASE_URL}/health',
            headers={'Origin': 'http://localhost:3000'},
            timeout=5
        )
        self.assertEqual(
            response2.headers.get('Access-Control-Allow-Origin'),
            'http://localhost:3000'
        )

        print('✓ Multiple allowed origins test passed')
