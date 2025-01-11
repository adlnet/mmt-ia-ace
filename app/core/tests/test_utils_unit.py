import hashlib
import logging
import xml.etree.ElementTree as ET
from unittest.mock import patch

import pandas as pd
from core.management.utils.xsr_client import (get_source_metadata_key_value,
                                              read_source_file)
from ddt import data, ddt, unpack
from django.test import tag

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class UtilsTests(TestSetUp):
    """Unit Test cases for utils """

    # Test cases for XSR_CLIENT
    def test_read_source_file(self):
        """Test to check the extraction of source data from XSR for EVTVL"""
        with patch('core.management.utils.xsr_client'
                   '.XSRConfiguration.objects') as xsrCfg, \
                patch('core.management.utils.xsr_client.'
                      'ET.parse')as ext_data1, \
                patch('core.management.utils.xsr_client.'
                      'pd.DataFrame')as ext_data:
            # xml_string = """
            #     <version>
            #         <name>John</name>
            #         <age>30</age>
            #     </version>
            #     """
            # Parse the XML string
            root = ET.fromstring(self.xml_string)
            xsrCfg.first.source_file.return_value = 'Source_file'
            ext_data1.return_value = ext_data1
            ext_data1.tree.getroot.return_value = root
            ext_data.return_value = pd.DataFrame. \
                from_dict(self.source_metadata, orient='index')
            return_from_function = read_source_file()
            self.assertIsInstance(return_from_function, list)

    @data(('key_field1', 'key_field2'), ('key_field11', 'key_field22'))
    @unpack
    def test_get_source_metadata_key_value(self, first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'AceID': first_value,
            'SOURCESYSTEM': second_value
        }

        expected_key = first_value + '_' + second_value
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
            'AceID': first_value,
            'SOURCESYSTEM': second_value
        }

        result_key_dict = get_source_metadata_key_value(test_dict)

        self.assertEqual(result_key_dict, None)
