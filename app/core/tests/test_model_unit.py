from core.models import XSRConfiguration
from django.core.exceptions import ValidationError
from django.test import TestCase, tag


@tag('unit')
class ModelTests(TestCase):

    def test_create_xsr_configuration(self):
        """Test that creating a new XSR Configuration entry is successful
        with defaults """
        source_file = 'test_file.json'

        xiaConfig = XSRConfiguration(
            source_file=source_file)

        self.assertEqual(xiaConfig.source_file,
                         source_file)

    def test_create_two_xsr_configuration(self):
        """Test that trying to create more than one XSR Configuration throws
        ValidationError """
        with self.assertRaises(ValidationError):
            xsrConfig = \
                XSRConfiguration(source_file="example1.json")
            xsrConfig2 = \
                XSRConfiguration(source_file="example2.json")

            xsrConfig.save()
            xsrConfig2.save()
