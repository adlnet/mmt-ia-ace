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
        max_length=200
    )
    Subscription_key = models.CharField(
        help_text='Enter the XSR Token',
        max_length=200, null=True, blank=True
    )
    Parameter = models.JSONField('Enter parameters ',
                                 null=True, blank=True)
