from core.models import XSRConfiguration
from django.test import TestCase, tag


@tag('unit')
class ModelTests(TestCase):

    def test_create_xsr_configuration(self):
        """Test that creating a new XSR Configuration entry is successful
        with defaults """
        test_data = 'test'
        test_json = {"key": "value"}

        xia_config = XSRConfiguration(
            Transcript_API=test_data,
            Subscription_key=test_data,
            Parameter=test_json)

        self.assertEqual(xia_config.Transcript_API,
                         test_data)
        self.assertEqual(xia_config.Subscription_key,
                         test_data)
        self.assertEqual(xia_config.Parameter,
                         test_json)
