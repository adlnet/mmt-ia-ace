from unittest.mock import patch

import pandas as pd
from api.views import get_status
from core.tests.test_setup import TestSetUp
from ddt import ddt
from django.test import tag
from django.urls import reverse
from rest_framework import status


@tag('unit')
@ddt
class ViewTests(TestSetUp):

    def test_xia_workflow(self):
        """Test that the /api/xia-workflow/"""

        url = reverse('api:xia_workflow')
        with patch('api.views.'
                   'execute_xia_automated_workflow') as mock_workflow:
            class Test:
                id = 1

            mock_workflow.delay.return_value = Test
            response = self.client.get(url)

            self.assertEqual(mock_workflow.delay.call_count, 1)
            self.assertEqual(response.status_code,
                             status.HTTP_202_ACCEPTED)

    def test_get_status(self):
        "Test for get_status"
        with patch('api.views.'
                   'AsyncResult') as mock_AsyncResult:
            mock_AsyncResult.return_value.status = 201
            mock_AsyncResult.return_value.result = "success"
            response = get_status(None, 1)
            self.assertEqual(response.status_code,
                             status.HTTP_200_OK)

    # Test cases for CreditDataView
    def test_CreditDataView(self):
        """Test to check HTTP requests for Credential data for XIA"""
        with patch('api.views.element_Tree.fromstring'), \
                patch('api.views.PlainTextParser'), \
                patch('api.views.pd.DataFrame')as ext_data, \
                patch('api.views.get_source_metadata'):
            xml_data = """
                <data>
                    <name>John</name>
                    <age>30</age>
                </data>
                """
            url = reverse('api:credit-data')
            ext_data.return_value = pd.DataFrame. \
                from_dict(self.source_metadata, orient='index')

            response = self.client.post(url, xml_data,
                                        content_type='application/xml')
            self.assertEqual(response.status_code,
                             status.HTTP_200_OK)
