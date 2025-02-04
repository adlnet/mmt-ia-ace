from unittest.mock import patch

import pandas as pd
from django.test import TestCase


class TestSetUp(TestCase):
    """Class with setup and teardown for tests in XIS"""

    def setUp(self):
        """Function to set up necessary data for testing"""

        # globally accessible data sets
        self.patcher = patch('core.tasks.conformance_alerts_Command')
        self.mock_alert = self.patcher.start()

        # globally accessible data sets

        self.source_metadata = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "test name",
            "Start_date": "2017-03-28T00:00:00-04:00",
            "AceID": "TestData 123",
            "SOURCESYSTEM": "ACE",
            "test_description": "test description",
            "supplemental_data": "sample1"
        }

        self.key_value = "TestData 123_ACE"
        self.key_value_hash = \
            "348a3c0ceae1888ea586252c6f66c9010917935237688771c638" \
            "e46dfc50efb473a9d7ceded9f27b4c41f83a4d949d4382b5ace3" \
            "912f5f7af59bcfc99c9f2d7f"
        self.hash_value = \
            "513a7f00940220c7839f5a0969afbdb6dff4a5d93b5af813287db6" \
            "01885349670875f27fcedbee8aa7b2bbbae9617853c8f9b14098faa1" \
            "92b2f1816a147ebd88"

        self.test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}
        self.metadata_df = pd.DataFrame.from_dict({1: self.source_metadata},
                                                  orient='index')

        self.xml_string = """
        <update Database="Courses">
            <versions Chapter="Navy" Count="6">
                <version AceID="NV-1511-0022">
                <titles>
                    <title Precedence="prime">Naval</title>
                </titles>
                </version>
            </versions>
        </update>
        """

        return super().setUp()

    def tearDown(self):
        return super().tearDown()
