import hashlib
import json
import logging
from unittest.mock import patch

import requests
from core.management.utils.xsr_client import (extract_source,
                                              get_source_metadata_key_value,
                                              get_xsr_api_endpoint,
                                              get_xsr_api_response)
from core.models import XSRConfiguration
from ddt import data, ddt, unpack
from django.test import tag

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class UtilsTests(TestSetUp):
    """Unit Test cases for utils """

    # Test cases for XSR_CLIENT

    def test_get_xsr_api_endpoint(self):
        """Test setting API endpoint to connect to XSR"""
        test_data = 'test'
        test_json = {"key": "value"}
        test_url = test_data+"?key=value"

        xsr_config = XSRConfiguration(
            Transcript_API=test_data,
            Subscription_key=test_data,
            Parameter=test_json)
        transcript_api, subscription_key = \
            get_xsr_api_endpoint(xsr_config)
        self.assertEqual(transcript_api, test_url)
        self.assertEqual(subscription_key, test_data)

    def test_get_xsr_api_response(self):
        """ Test Function to get api response
        from xsr endpoint"""
        with patch('core.management.utils.xsr_client'
                   '.get_xsr_api_endpoint') as mock_api, \
            patch('core.management.utils.xsr_client'
                  '.requests.get'):
            mock_api.return_value = "url", "token"
            resp = get_xsr_api_response(mock_api)

            self.assertTrue(resp)

    @patch('core.management.utils.xsr_client.logger')
    def test_get_xsr_api_response_exception(self, mock_logger):
        """ Test Function to get api response
        from xsr endpoint"""
        with patch('core.management.utils.xsr_client'
                   '.get_xsr_api_endpoint') as mock_api, \
            patch('core.management.utils.xsr_client'
                  '.requests.get') as mock_response:
            mock_api.return_value = "url", "token"
            mock_response.return_value =\
                requests.exceptions.RequestException
            get_xsr_api_response(mock_api)
            self.assertRaises(SystemExit)

    @data(('key_field1', 'key_field2'), ('key_field11', 'key_field22'))
    @unpack
    def test_get_source_metadata_key_value(self, first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'Key_val': first_value,
            'VerNum': first_value,
            'CourseNumber': second_value,
            'SOURCESYSTEM': second_value
        }

        expected_key = (first_value + '_' + second_value)
        expected_key_hash = hashlib.sha512(expected_key.encode('utf-8')). \
            hexdigest()

        result_key_dict = get_source_metadata_key_value(test_dict)
        self.assertEqual(result_key_dict['key_value'], expected_key)
        self.assertEqual(result_key_dict['key_value_hash'], expected_key_hash)

    @data(('key_field1', ''))
    @unpack
    def test_get_source_metadata_key_value_fail(self,
                                                first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'Key_val': first_value,
            'VerNum': first_value,
            'CourseNumber': second_value,
            'SOURCESYSTEM': second_value
        }

        result_key_dict = get_source_metadata_key_value(test_dict)

        self.assertEqual(result_key_dict, None)

    def test_extract_source(self):
        """Test function to parse xsr data and
        convert to dictionary"""
        test_data = 'test'
        test_json = {"key": "value"}

        xsr_config = XSRConfiguration(
            Transcript_API=test_data,
            Subscription_key=test_data,
            Parameter=test_json)
        xsr_config.save()

        with patch('core.management.utils.xsr_client'
                   '.get_xsr_api_response') as mock_resp:
            mock_resp.return_value.text = json.dumps(self.xsr_data)
            resp = extract_source()
            self.assertIn(self.xsr_data["update"]['versions']['ACEID'],
                          resp[0]['ACEID'].values)
            self.assertIsInstance(resp, list)
