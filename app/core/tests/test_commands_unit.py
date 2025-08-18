import logging
from unittest.mock import patch

import pandas as pd
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
    @patch('core.management.commands.extract_source_metadata.logger')
    def test_get_source_metadata(self, mock_logger):
        """Test Retrieving source metadata"""
        with patch('core.management.commands.extract_source_metadata'
                   '.extract_source') as sample_df:
            sample_df.return_value = [pd.DataFrame()]
            get_source_metadata()
            mock_logger.error.assert_called_with("Source metadata is empty!")

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        with patch('openlxp_xia.management.utils.xia_internal'
                   '.get_publisher_detail'), \
                patch('openlxp_xia.management.utils.xia_internal'
                      '.XIAConfiguration.objects') as xis_cfg:
            xia_config = XIAConfiguration(publisher='JKO')
            xis_cfg.first.return_value = xia_config
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

    def test_store_source_metadata_active_inactive(self):
        """Test to check saving of source metadata
        active and inactive"""

        store_source_metadata(self.key_value,
                              self.key_value_hash,
                              self.hash_value,
                              self.source_metadata)
        metadata_new = self.source_metadata
        metadata_new["LastUpdatedOn"] = "2018-03-28T00:00:00-04:00"

        store_source_metadata(self.key_value,
                              self.key_value_hash,
                              self.hash_value1,
                              metadata_new)
        m_obj_active = MetadataLedger. \
            objects.get(source_metadata_key=self.key_value,
                        record_lifecycle_status="Active")
        m_obj_inactive = MetadataLedger. \
            objects.get(source_metadata_key=self.key_value,
                        record_lifecycle_status="Inactive")
        self.assertIsNotNone(m_obj_active)
        self.assertIsNotNone(m_obj_inactive)
