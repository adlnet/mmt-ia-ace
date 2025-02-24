from django.contrib import admin

from .models import XSRConfiguration

# Register your models here.


@admin.register(XSRConfiguration)
class XSRConfigurationAdmin(admin.ModelAdmin):
    list_display = ('Transcript_API',)
    fields = ['Transcript_API',
              'Subscription_key',
              'Parameter']
