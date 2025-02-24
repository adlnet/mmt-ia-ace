import hashlib
import json
import logging

import pandas as pd
import requests
from core.models import XSRConfiguration
from openlxp_xia.management.utils.xia_internal import get_key_dict

logger = logging.getLogger('dict_config_logger')


def get_xsr_api_endpoint(xsr_obj):
    """Setting API endpoint to connect to XSR """
    logger.debug("Retrieve xsr_api_endpoint from XSR configuration")
    # add parameters to API

    xsr_url = xsr_obj.Transcript_API
    if xsr_obj.Parameter:
        # Iterate over key-value pairs
        for key, value in xsr_obj.Parameter.items():
            xsr_url += '?'+str(key)+'='+str(value)
    return (xsr_url, xsr_obj.Subscription_key)


def get_xsr_api_response(xsr_obj):
    """Function to get api response from xsr endpoint"""
    # url of rss feed

    xsr_url, token = get_xsr_api_endpoint(xsr_obj)

    headers = {'Cache-Control': 'no-cache',
               'Ocp-Apim-Subscription-Key': token}

    # creating HTTP response object from given url
    try:
        resp = requests.get(url=xsr_url, headers=headers, verify=False)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        raise SystemExit('Exiting! Can not make connection with XSR.')

    return resp


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['ACEID', 'VerNum', 'CourseNumber', 'SOURCESYSTEM']
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


def extract_source():
    """function to parse xsr data and
    convert to dictionary"""

    ace_fields = ['ACEID',
                  'VerNum',
                  'Chapter',
                  'ModDate',
                  'StartDateYYYYMM',
                  'EndDateYYYYMM',
                  'LastUpdatedOn',
                  'objective',
                  'instruction',
                  'titles',
                  'locations',
                  'groups']
    std_source_df = pd.DataFrame()

    for xsr_obj in XSRConfiguration.objects.all():

        resp = get_xsr_api_response(xsr_obj)
        source_data_dict = json.loads(resp.text)
        source_data_dict = source_data_dict["update"]['versions']

        logger.info("Retrieving data from source page ")
        source_df_list = [pd.json_normalize(source_data_dict,
                                            record_path=['courses'],
                                            meta=ace_fields,
                                            errors='ignore')]

        source_df = pd.concat(source_df_list).reset_index(drop=True)
        logger.info("Changing null values to None for source dataframe")
        std_source_df = source_df.where(pd.notnull(source_df),
                                        None)
    logger.info("Completed retrieving data from source")
    return [std_source_df]
