import logging

from django.core.management.base import BaseCommand
from django.utils import timezone
from openlxp_xia.models import MetadataLedger

logger = logging.getLogger('dict_config_logger')


def copy_target_metadata():
    """Function to Copy source data to target
    metadata fields for loading """

    metadata_records = MetadataLedger.objects.all()

    for metadata in metadata_records:
        # copying over hash value of metadata
        logger.info('Copying over record source to target fields')
        metadata.target_metadata = metadata.source_metadata
        metadata.target_metadata_hash = metadata.source_metadata_hash
        metadata.target_metadata_key = metadata.source_metadata_key
        metadata.target_metadata_key_hash = metadata.source_metadata_key_hash
        metadata.source_metadata_transformation_date = timezone.now()
        metadata.save()


class Command(BaseCommand):
    """Django command to Copy source data to target
    metadata fields for loading """

    def handle(self, *args, **options):
        """
            Metadata is extracted from XSR and stored in Metadata Ledger
        """
        copy_target_metadata()

        logger.info('MetadataLedger updated with target data from source')
