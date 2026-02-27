import hashlib
import json
import logging
from datetime import datetime
import os

import boto3
import pandas as pd
import requests
from core.models import XSRConfiguration
from openlxp_xia.management.utils.xia_internal import convert_date_to_isoformat
from openlxp_xia.management.utils.xia_internal import get_key_dict

logger = logging.getLogger('dict_config_logger')


def convert_yyyymm_to_date(yyyymm_string):
    """Converts a yyyymm string to a date object,
    defaulting to the first day of the month."""
    try:
        date_val = datetime.strptime(yyyymm_string + '01', '%Y%m%d').date()
        return str(date_val)
    except ValueError:
        return None


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
        resp = requests.get(url=xsr_url, headers=headers)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        raise SystemExit('Exiting! Can not make connection with XSR.')

    return resp


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['Key_val', 'SOURCESYSTEM']
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


def _extract_area_fields(metadata):
    """Extracts version, start_date, end_date, 
    last_updated_on from metadata."""
    titles = _process_titles(metadata.get('titles', []))
    skill_level = metadata.get('SkillLevel', "")
    version = metadata.get('VerNum', "")
    start_date = convert_yyyymm_to_date(metadata.get(
        'StartDateYYYYMM', "")) if metadata.get('StartDateYYYYMM') else ""
    end_date = convert_yyyymm_to_date(metadata.get(
        'EndDateYYYYMM', "")) if metadata.get('EndDateYYYYMM') else ""
    last_updated_on = convert_date_to_isoformat(metadata.get(
        'LastUpdatedOn', "")) if metadata.get('LastUpdatedOn') else ""
    return version, start_date, end_date, last_updated_on, skill_level, titles


def _build_area_dict(subject, level, version, start_date,
                     end_date, last_updated_on, metadata, skill_level,
                     titles):
    return {
        "hours": subject['Min'],
        "level": level['AcadLevel'],
        "version": version,
        "start_date": start_date,
        "end_date": end_date,
        "last_updated_on": last_updated_on,
        "academic_course_area": {
            "course_area": subject['Subject']
        },
        "military_course": metadata["experience_id"],
        "military_name": titles,
        "skill_level": skill_level,
        "ace_identifier": metadata['ACEID']
    }


def _process_titles(titles):
    if not titles:
        return ""
    # Ensure titles is a list
    if not isinstance(titles, list):
        titles = [titles] if titles else []
    title_list = [None] * len(titles)
    num = len(titles) - 1
    for item in titles:
        if item.get("Precedence") == "prime":
            title_list[0] = item.get("Title")
        else:
            title_list[num] = item.get("Title")
            num -= 1
    return ", ".join(filter(None, title_list))


def custom_ace_setting_areas(metadata):
    """Reduce complexity by splitting logic into helpers."""
    version, start_date, end_date, last_updated_on, skill_level, titles = \
        _extract_area_fields(
            metadata)
    areasandhours_list = []

    groups = metadata.get('groups', [])
    if not isinstance(groups, list):
        groups = [groups] if groups else []
    for level in groups:
        for subject in level.get('subjects', []):
            area_dict = _build_area_dict(
                subject, level, version, start_date,
                end_date, last_updated_on, metadata, skill_level, titles)
            areasandhours_list.append(area_dict)

    metadata['groups'] = areasandhours_list
    metadata['titles'] = _process_titles(metadata.get('titles', []))
    return metadata


def custom_ace_data(metadata):
    """Custom manipulation to ACE metadata"""

    if ('description' in metadata and
        type(metadata["description"]) is dict and
            'Objective' in metadata['description']):
        metadata['description'] = (metadata['description']
                                   ['Objective'])
    if ('requirements' in metadata and
        type(metadata["requirements"]) is dict and
            'Instruction' in metadata['requirements']):
        metadata['requirements'] = (metadata['requirements']
                                    ['Instruction'])
    return metadata


def extract_ace_course_data(source_data_dict):
    ace_fields = ['ACEID',
                  'VerNum',
                  'Chapter',
                  'ModDate',
                  'StartDateYYYYMM',
                  'EndDateYYYYMM',
                  'LastUpdatedOn',
                  'objective',
                  ['instruction'],
                  'titles',
                  'locations',
                  'groups']
    source_data_dict = source_data_dict["update"]['versions']

    logger.info("Retrieving data from source page ")
    source_df = pd.json_normalize(source_data_dict,
                                  record_path=['courses'],
                                  meta=ace_fields,
                                  errors='ignore', sep='.')
    source_df["requirements"] = source_df["instruction"]
    source_df["description"] = (source_df["objective"])
    source_df["experience_id"] = source_df["CourseNumber"]
    source_df["Key_val"] = (
        source_df["ACEID"] + source_df["CourseNumber"] +
        source_df["VerNum"])
    source_df_list = source_df
    return source_df_list


def extract_ace_occupation_data(source_data_dict):
    ace_fields = ['ACEID',
                  'Type',
                  'Chapter',
                  'ModDate',
                  'StartDateYYYYMM',
                  'EndDateYYYYMM',
                  'LastUpdatedOn',
                  'Pattern',
                  'Summary',
                  'titles',
                  'field']
    source_data_dict = source_data_dict["update"]['exhibits']

    logger.info("Retrieving data from source page ")
    source_df = pd.json_normalize(source_data_dict,
                                  record_path=['levels'],
                                  meta=ace_fields,
                                  errors='ignore')

    source_df["requirements"] = source_df["Pattern"]
    source_df["description"] = source_df["Summary"]
    source_df["experience_id"] = (
        source_df["ACEID"] + "-" + source_df["SkillLevel"])
    source_df["Key_val"] = (
        source_df["ACEID"] + source_df["SkillLevel"])

    source_df_list = source_df
    return source_df_list


def _read_from_s3(s3_source_file):
    """Read JSON data from S3 bucket."""
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        logger.warning("S3 bucket name not found")
        return None

    try:
        s3 = boto3.resource('s3')
        logger.info("Reading source file from S3")
        obj = s3.Object(bucket_name, s3_source_file)
        return json.loads(obj.get()['Body'].read().decode('utf-8'))
    except s3.meta.client.exceptions.NoSuchKey:
        logger.error("File %s not found in bucket %s",
                     s3_source_file, bucket_name)
        return None
    except Exception as e:
        logger.error("Error reading from S3: %s", e)
        return None


def _get_source_data(xsr_obj):
    """Get source data from either S3 or API."""
    if xsr_obj.s3_source_file:
        return _read_from_s3(xsr_obj.s3_source_file)
    else:
        logger.info("Retrieving data from XSR API")
        resp = get_xsr_api_response(xsr_obj)
        return json.loads(resp.text)


def _process_xsr_configuration(xsr_obj):
    """Process a single XSR configuration and return dataframe."""
    source_data_dict = _get_source_data(xsr_obj)
    if source_data_dict is None:
        return None

    if xsr_obj.API_type == "Course":
        return extract_ace_course_data(source_data_dict)
    else:
        return extract_ace_occupation_data(source_data_dict)


def extract_source():
    """Function to parse xsr data and convert to dictionary"""
    source_df_list = []

    for xsr_obj in XSRConfiguration.objects.all():
        source_df = _process_xsr_configuration(xsr_obj)
        if source_df is not None:
            source_df_list.append(source_df)

    if not source_df_list:
        logger.warning("No data retrieved from any XSR configuration")
        return [pd.DataFrame()]

    source_df = pd.concat(source_df_list).reset_index(drop=True)
    logger.info("Changing null values to None for source dataframe")
    std_source_df = source_df.where(pd.notnull(source_df), None)
    logger.info("Completed retrieving data from source")
    return [std_source_df]
