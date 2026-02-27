from django.db import models
from model_utils import Choices
from model_utils.models import StatusField


class XSRConfiguration(models.Model):
    """Model for XSR Configuration """
    API_CHOICES = Choices('Course', 'Occupation')
    API_type = StatusField(choices_name='API_CHOICES')
    Transcript_API = models.CharField(
        help_text='Enter the ACE '
        'Transcript API endpoint',
        max_length=200, blank=True
    )
    s3_source_file = models.CharField(
        help_text='Enter the S3 source file path',
        max_length=500, blank=True
    )
    Subscription_key = models.CharField(
        help_text='Enter the XSR Token',
        max_length=200, blank=True
    )
    Parameter = models.JSONField('Enter parameters ',
                                 null=True, blank=True)
