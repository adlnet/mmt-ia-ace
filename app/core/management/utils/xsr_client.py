import hashlib
import logging
import xml.etree.ElementTree as ET

import pandas as pd
from core.models import XSRConfiguration
from openlxp_xia.management.utils.xia_internal import get_key_dict

logger = logging.getLogger('dict_config_logger')


def read_source_file():
    """setting file path from s3 bucket"""
    xsr_data = XSRConfiguration.objects.first()
    file_name = xsr_data.source_file

    # Parse the XML file
    tree = ET.parse(file_name)

    # Get the root element
    xsr_root = tree.getroot()

    xsr_items = []

    for s_item in xsr_root.findall('.//version'):
        xsr_items.append(s_item.attrib)

        for count, item in enumerate(s_item):

            xsr_items[-1][item.tag] = {}
            xsr_items[-1][item.tag] = item.attrib

            for count1, c_item in enumerate(item):

                if c_item.tag not in xsr_items[-1][item.tag]:
                    xsr_items[-1][item.tag][c_item.tag] = list()

                if c_item.text:
                    xsr_items[-1][item.tag][c_item.tag].append(
                        {c_item.tag: c_item.text})
                    (xsr_items[-1][item.tag][c_item.tag][-1]).update(
                        c_item.attrib)
                else:
                    xsr_items[-1][item.tag][c_item.tag].append(c_item.attrib)

                for count1_1, item1_1 in enumerate(c_item):

                    xsr_items[-1][item.tag][c_item.tag][-1][item1_1.tag] = (
                        item1_1.attrib)

                    for count2, cs_item in enumerate(item1_1):

                        if cs_item.tag not in (xsr_items[-1][item.tag]
                                               [c_item.tag][-1][item1_1.tag]):
                            (xsr_items[-1][item.tag][c_item.tag][-1]
                             [item1_1.tag][cs_item.tag]) = list()

                        if cs_item.text:
                            (xsr_items[-1][item.tag][c_item.tag]
                             [-1][item1_1.tag][cs_item.tag]). \
                                append({cs_item.tag: cs_item.text})
                            (xsr_items[-1][item.tag][c_item.tag]
                             [-1][item1_1.tag][cs_item.tag][-1]). \
                                update(cs_item.attrib)
                        else:
                            (xsr_items[-1][item.tag][c_item.tag]
                             [-1][item1_1.tag][cs_item.tag]).append(
                                cs_item.attrib)

    source_df = pd.DataFrame(xsr_items)
    std_source_df = source_df.where(pd.notnull(source_df),
                                    None)

    #  Creating list of dataframes of sources
    source_list = [std_source_df]

    logger.debug("Sending source data in dataframe format for EVTVL")
    # file_name.delete()
    return source_list


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['AceID', 'SOURCESYSTEM']
    field_values = []

    for item in field:
        if not data_dict.get(item):
            logger.info('Field name ' + item + ' is missing for '
                                               'key creation')
            return None
        field_values.append(data_dict.get(item))

    # Key value creation for source metadata
    key_value = '_'.join(field_values)

    # Key value hash creation for source metadata
    key_value_hash = hashlib.sha512(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key
