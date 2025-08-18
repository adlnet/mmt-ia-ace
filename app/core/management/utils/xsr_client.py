import hashlib
import json
import logging
from datetime import datetime

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
        resp = requests.get(url=xsr_url, headers=headers, verify=False)
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


def custom_ACE_setting_areas(metadata):

    areasandhours_list = []

    area_dict = {}
    version = ""
    start_date = ""
    end_date = ""
    last_updated_on = ""

    # if 'ACEID' in metadata and metadata['ACEID']:

    #     ace_identifier = metadata['ACEID']

    # area_dict.update({
    #     "ace_identifier": {"ace_identifier": metadata['ACEID']}})

    if 'VerNum' in metadata and metadata['VerNum']:

        version = metadata['VerNum']
        area_dict.update({
            "version": metadata['VerNum']})

    if 'StartDateYYYYMM' in metadata and metadata['StartDateYYYYMM']:

        start_date = convert_yyyymm_to_date(metadata['StartDateYYYYMM'])

        # area_dict.update({
        #     "start_date": start_date})
    if 'EndDateYYYYMM' in metadata and metadata['EndDateYYYYMM']:

        end_date = convert_yyyymm_to_date(metadata['EndDateYYYYMM'])

        # area_dict.update({
        #     "end_date": end_date})
    if 'LastUpdatedOn' in metadata and metadata['LastUpdatedOn']:

        last_updated_on = convert_date_to_isoformat(metadata['LastUpdatedOn'])

        # area_dict.update({
        #     "last_updated_on": last_updated_on})

    if 'groups' in metadata and metadata['groups']:
        for level in metadata['groups']:
            if 'subject' in level:
                for subject in level['subjects']:
                    area_dict = {
                        "hours": subject['Min'],
                        "level": level['AcadLevel'],
                        "version": version,
                        "start_date": start_date,
                        "end_date": end_date,
                        "last_updated_on": last_updated_on,
                        "academic_course_area": {
                            "course_area": subject['Subject']
                        }
                    }

                areasandhours_list.append(area_dict)

    metadata['groups'] = areasandhours_list

    if 'titles' in metadata and metadata['titles']:
        title_json = metadata['titles']
        title_len = len(title_json)

        title_list = [None]*title_len
        num = title_len-1

        for item in title_json:
            if item["Precedence"] == "prime":
                title_list[-title_len] = item["Title"]
            else:
                title_list[num] = item["Title"]
                num -= 1
        title = ", ".join(title_list)
        metadata['titles'] = title

    return metadata


def custom_ACE_data(metadata):
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


def extract_ACE_course_data(source_data_dict):
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
    source_df.to_excel("course.xlsx")
    source_df_list = source_df
    return source_df_list


def extract_ACE_occupation_data(source_data_dict):
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


def extract_source():
    """function to parse xsr data and
    convert to dictionary"""

    std_source_df = pd.DataFrame()

    source_df_list = []

    for xsr_obj in XSRConfiguration.objects.all():

        resp = get_xsr_api_response(xsr_obj)
        source_data_dict = json.loads(resp.text)

        if xsr_obj.API_type == "Course":
            source_df = extract_ACE_course_data(source_data_dict)

        else:
            source_df = extract_ACE_occupation_data(source_data_dict)
        source_df_list.append(source_df)

    source_df = pd.concat(source_df_list).reset_index(drop=True)
    logger.info("Changing null values to None for source dataframe")
    std_source_df = source_df.where(pd.notnull(source_df),
                                    None)
    logger.info("Completed retrieving data from source")
    return [std_source_df]
