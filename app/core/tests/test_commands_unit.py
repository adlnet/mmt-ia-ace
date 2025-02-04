import logging
from unittest.mock import patch

import pandas as pd
from core.management.commands.copy_target_metadata import copy_target_metadata
from core.management.commands.extract_source_metadata import (
    add_publisher_to_source, extract_metadata_using_key, get_source_metadata,
    store_source_metadata)
from ddt import ddt
from django.core.management import call_command
from django.db.utils import OperationalError
from django.test import tag
from openlxp_xia.models import MetadataLedger, XIAConfiguration

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class CommandTests(TestSetUp):

    # Test cases for waitdb
    def test_wait_for_db_ready(self):
        """Test that waiting for db when db is available"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.return_value = True
            call_command('waitdb')
            self.assertEqual(gi.call_count, 1)

    @patch('time.sleep', return_value=True)
    def test_wait_for_db(self, ts):
        """Test waiting for db"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.side_effect = [OperationalError] * 5 + [True]
            call_command('waitdb')
            self.assertEqual(gi.ensure_connection.call_count, 6)

    # Test case with extract_source_metadata

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        with patch('openlxp_xia.management.utils.xia_internal'
                   '.get_publisher_detail'), \
                patch('openlxp_xia.management.utils.xia_internal'
                      '.XIAConfiguration.objects') as xisCfg:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xisCfg.first.return_value = xiaConfig
            test_df = pd.DataFrame.from_dict(self.test_data)
            result = add_publisher_to_source(test_df)
            key_exist = 'SOURCESYSTEM' in result.columns
            self.assertTrue(key_exist)

    def test_extract_metadata_using_key(self):
        """Test to creating key, hash of key & hash of metadata"""

        data = {1: self.source_metadata}
        data_df = pd.DataFrame.from_dict(data, orient='index')
        with patch(
                'core.management.commands.extract_source_metadata'
                '.add_publisher_to_source',
                return_value=data_df), \
                patch(
                    'core.management.commands.extract_source_metadata'
                    '.get_source_metadata_key_value',
                    return_value=None) as mock_get_source, \
                patch(
                    'core.management.commands.extract_source_metadata'
                    '.store_source_metadata',
                    return_value=None) as mock_store_source:
            mock_get_source.return_value = mock_get_source
            mock_get_source.exclude.return_value = mock_get_source
            mock_get_source.filter.side_effect = [
                mock_get_source, mock_get_source]

            extract_metadata_using_key(data_df)
            self.assertEqual(mock_get_source.call_count, 1)
            self.assertEqual(mock_store_source.call_count, 1)

    def test_store_source_metadata(self):
        """Test to check saving of source metadata"""

        store_source_metadata(self.key_value,
                              self.key_value_hash,
                              self.hash_value,
                              self.source_metadata)
        m_obj = MetadataLedger. \
            objects.get(source_metadata_key=self.key_value)
        self.assertIsNotNone(m_obj)

    def test_get_source_metadata(self):
        """Test to check extraction of source metadata as dataframe"""

        with patch('core.management.commands.extract_source_metadata.'
                   'read_source_file') as mock_read_source_file, \
                patch('core.management.commands.extract_source_metadata.'
                      'read_source_file') as mock_extract_metadata_using_key:
            d = {'col1': [0, 1, 2, 3], 'col2': pd.Series([2, 3], index=[2, 3])}
            df = pd.DataFrame(data=d, index=[0, 1, 2, 3])
            mock_read_source_file.return_value = mock_read_source_file
            mock_read_source_file.return_value = [df]
            get_source_metadata()
            self.assertEqual(mock_extract_metadata_using_key.call_count, 1)

    # Test case with copy_target_metadata

    def test_copy_target_metadata(self):
        """Test for Copy source data to target
        metadata fields for loading"""
        metadata = MetadataLedger(source_metadata=self.source_metadata,
                                  source_metadata_key=self.key_value,
                                  source_metadata_key_hash=self.key_value_hash,
                                  source_metadata_hash=self.hash_value)
        metadata.save()
        copy_target_metadata()

        m_obj = MetadataLedger.objects.get(source_metadata_key=self.key_value)

        self.assertEqual(m_obj.target_metadata, self.source_metadata)
        self.assertEqual(m_obj.target_metadata_key, self.key_value)
        self.assertEqual(m_obj.target_metadata_key_hash, self.key_value_hash)
        self.assertEqual(m_obj.target_metadata_hash, self.hash_value)
