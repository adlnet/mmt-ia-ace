from django import forms
from django.contrib import admin

from .models import XSRConfiguration

# Register your models here.


class MyForm(forms.ModelForm):
    API_type = forms.ChoiceField(
        choices=XSRConfiguration.API_CHOICES,
        widget=forms.RadioSelect
    )

    class Meta:
        model = XSRConfiguration
        fields = ['Transcript_API',
                  'Subscription_key',
                  'Parameter', 'API_type']


@admin.register(XSRConfiguration)
class XSRConfigurationAdmin(admin.ModelAdmin):
    list_display = ('Transcript_API', 's3_source_file', 'API_type')
    fields = ['Transcript_API',
              'Subscription_key',
              'Parameter', 'API_type', 's3_source_file']
    form = MyForm
