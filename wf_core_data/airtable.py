import wf_core_data.utils
import requests
import pandas as pd
from collections import OrderedDict
# import pickle
# import json
import datetime
import time
import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_DELAY = 0.25
DEFAULT_MAX_REQUESTS = 50
DEFAULT_WRITE_CHUNK_SIZE = 10

SCHOOLS_BASE_ID = 'appJBT9a4f3b7hWQ2'
DATA_DICT_BASE_ID = 'appHMyIWgnHqVJymL'

class AirtableClient:
    def __init__(
        self,
        api_key=None,
        url_base='https://api.airtable.com/v0/'
    ):
        self.api_key = api_key
        self.url_base = url_base
        if self.api_key is None:
            self.api_key = os.getenv('AIRTABLE_API_KEY')

    def fetch_tl_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching TL data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='TLs',
            params=params
        )
        tl_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('teacher_id_at', record.get('id')),
                ('teacher_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('teacher_full_name_at', fields.get('Full Name')),
                ('teacher_first_name_at', fields.get('First Name')),
                ('teacher_middle_name_at', fields.get('Middle Name')),
                ('teacher_last_name_at', fields.get('Last Name')),
                ('teacher_title_at', fields.get('Title')),
                ('teacher_ethnicity_at', fields.get('Race & Ethnicity')),
                ('teacher_ethnicity_other_at', fields.get('Race & Ethnicity - Other')),
                ('teacher_income_background_at', fields.get('Income Background')),
                ('teacher_email_at', fields.get('Email')),
                ('teacher_email_2_at', fields.get('Email 2')),
                ('teacher_email_3_at', fields.get('Email 3')),
                ('teacher_phone_at', fields.get('Phone Number')),
                ('teacher_phone_2_at', fields.get('Phone Number 2')),
                ('teacher_employer_at', fields.get('Employer')),
                ('hub_at', fields.get('Hub')),
                ('pod_at', fields.get('Pod')),
                ('user_id_tc', fields.get('TC User ID'))
            ])
            tl_data.append(datum)
        if format == 'dataframe':
            tl_data = convert_tl_data_to_df(tl_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return tl_data

    def fetch_location_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching location data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Locations',
            params=params
        )
        location_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('location_id_at', record.get('id')),
                ('location_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('location_address_at', fields.get('Address')),
                ('school_id_at', wf_core_data.utils.to_singleton(fields.get('School Name'))),
                ('school_location_start_at', wf_core_data.utils.to_date(fields.get('Start of time at location'))),
                ('school_location_end_at', wf_core_data.utils.to_date(fields.get('End of time at location')))
            ])
            location_data.append(datum)
        if format == 'dataframe':
            location_data = convert_location_data_to_df(location_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return location_data

    def fetch_teacher_school_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching teacher school association data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Teachers x Schools',
            params=params
        )
        teacher_school_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('teacher_school_id_at', record.get('id')),
                ('teacher_school_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('teacher_id_at', fields.get('TL')),
                ('school_id_at', fields.get('School')),
                ('teacher_school_start_at', wf_core_data.utils.to_date(fields.get('Start Date'))),
                ('teacher_school_end_at', wf_core_data.utils.to_date(fields.get('End Date'))),
                ('teacher_school_active_at', wf_core_data.utils.to_boolean(fields.get('Currently Active')))
            ])
            teacher_school_data.append(datum)
        if format == 'dataframe':
            teacher_school_data = convert_teacher_school_data_to_df(teacher_school_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_school_data

    def fetch_school_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching school data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Schools',
            params=params
        )
        school_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('school_id_at', record.get('id')),
                ('school_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('hub_id_at', fields.get('Hub')),
                ('pod_id_at', fields.get('Pod')),
                ('school_name_at', fields.get('Name')),
                ('school_short_name_at', fields.get('Short Name')),
                ('school_status_at', fields.get('School Status')),
                ('school_ssj_stage_at', fields.get('School Startup Stage')),
                ('school_governance_model_at', fields.get('Governance Model')),
                ('school_ages_served_at', fields.get('Ages served')),
                ('school_location_ids_at', fields.get('Locations')),
                ('school_id_tc', fields.get('TC school ID'))
            ])
            school_data.append(datum)
        if format == 'dataframe':
            school_data = convert_school_data_to_df(school_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return school_data

    def fetch_hub_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching hub data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Hubs',
            params=params
        )
        hub_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('hub_id_at', record.get('id')),
                ('hub_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('hub_name_at', fields.get('Name'))
            ])
            hub_data.append(datum)
        if format == 'dataframe':
            hub_data = convert_hub_data_to_df(hub_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return hub_data

    def fetch_pod_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching pod data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Pods',
            params=params
        )
        pod_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('pod_id_at', record.get('id')),
                ('pod_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('pod_name_at', fields.get('Name'))
            ])
            pod_data.append(datum)
        if format == 'dataframe':
            pod_data = convert_pod_data_to_df(pod_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return pod_data

    def fetch_ethnicity_categories(
        self,
        pull_datetime=None,
        params=None,
        base_id=DATA_DICT_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching ethnicity categories from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Ethnicity categories',
            params=params
        )
        ethnicity_categories=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('ethnicity_category_id_at', record.get('id')),
                ('ethnicity_category_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('ethnicity_category', fields.get('ethnicity_category')),
                ('ethnicity_display_name_english', fields.get('ethnicity_display_name_english')),
                ('ethnicity_display_name_spanish', fields.get('ethnicity_display_name_spanish'))            ])
            ethnicity_categories.append(datum)
        if format == 'dataframe':
            ethnicity_categories = convert_ethnicity_categories_to_df(ethnicity_categories)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return ethnicity_categories

    def fetch_gender_categories(
        self,
        pull_datetime=None,
        params=None,
        base_id=DATA_DICT_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching gender categories from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Gender categories',
            params=params
        )
        gender_categories=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('gender_category_id_at', record.get('id')),
                ('gender_category_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('gender_category', fields.get('gender_category')),
                ('gender_display_name_english', fields.get('gender_display_name_english')),
                ('gender_display_name_spanish', fields.get('gender_display_name_spanish'))            ])
            gender_categories.append(datum)
        if format == 'dataframe':
            gender_categories = convert_gender_categories_to_df(gender_categories)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return gender_categories

    def fetch_household_income_categories(
        self,
        pull_datetime=None,
        params=None,
        base_id=DATA_DICT_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching household income categories from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Household income categories',
            params=params
        )
        household_income_categories=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('household_income_category_id_at', record.get('id')),
                ('household_income_category_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('household_income_category', fields.get('household_income_category')),
                ('household_income_display_name_english', fields.get('household_income_display_name_english')),
                ('household_income_display_name_spanish', fields.get('household_income_display_name_spanish'))            ])
            household_income_categories.append(datum)
        if format == 'dataframe':
            household_income_categories = convert_household_income_categories_to_df(household_income_categories)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return household_income_categories

    def fetch_nps_categories(
        self,
        pull_datetime=None,
        params=None,
        base_id=DATA_DICT_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching NPS categories from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='NPS categories',
            params=params
        )
        nps_categories=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('nps_category_id_at', record.get('id')),
                ('nps_category_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('nps_category', fields.get('nps_category')),
                ('nps_display_name_english', fields.get('nps_display_name_english')),
                ('nps_display_name_spanish', fields.get('nps_display_name_spanish'))            ])
            nps_categories.append(datum)
        if format == 'dataframe':
            nps_categories = convert_nps_categories_to_df(nps_categories)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return nps_categories

    def fetch_boolean_categories(
        self,
        pull_datetime=None,
        params=None,
        base_id=DATA_DICT_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching boolean categories from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Boolean categories',
            params=params
        )
        boolean_categories=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('boolean_category_id_at', record.get('id')),
                ('boolean_category_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('boolean_category', fields.get('boolean_category')),
                ('boolean_display_name_english', fields.get('boolean_display_name_english')),
                ('boolean_display_name_spanish', fields.get('boolean_display_name_spanish'))            ])
            boolean_categories.append(datum)
        if format == 'dataframe':
            boolean_categories = convert_boolean_categories_to_df(boolean_categories)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return boolean_categories

    def write_dataframe(
        self,
        df,
        base_id,
        endpoint,
        params=None,
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS,
        write_chunk_size=DEFAULT_WRITE_CHUNK_SIZE
    ):
        num_records = len(df)
        num_chunks = (num_records // write_chunk_size) + 1
        logger.info('Writing {} records in {} chunks'.format(
            num_records,
            num_chunks
        ))
        for chunk_index in range(num_chunks):
            start_row_index = chunk_index*write_chunk_size
            end_row_index = min(
                (chunk_index + 1)*write_chunk_size,
                num_records
            )
            chunk_df = df.iloc[start_row_index:end_row_index]
            chunk_list = chunk_df.to_dict(orient='records')
            chunk_dict = {'records': [{'fields': row_dict} for row_dict in chunk_list]}
            logger.info('Writing chunk {}: rows {} to {}'.format(
                chunk_index,
                start_row_index,
                end_row_index
            ))
            self.post(
                base_id=base_id,
                endpoint=endpoint,
                data=chunk_dict
            )
            time.sleep(delay)

    def bulk_get(
        self,
        base_id,
        endpoint,
        params=None,
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        if params is None:
            params = dict()
        num_requests = 0
        records = list()
        while True:
            data = self.get(
                base_id=base_id,
                endpoint=endpoint,
                params=params
            )
            if 'records' in data.keys():
                logging.info('Returned {} records'.format(len(data.get('records'))))
                records.extend(data.get('records'))
            num_requests += 1
            if num_requests >= max_requests:
                logger.warning('Reached maximum number of requests ({}). Terminating.'.format(
                    max_requests
                ))
                break
            offset = data.get('offset')
            if offset is None:
                break
            params['offset'] = offset
            time.sleep(delay)
        return records

    def post(
        self,
        base_id,
        endpoint,
        data
    ):
        headers = dict()
        if self.api_key is not None:
            headers['Authorization'] = 'Bearer {}'.format(self.api_key)
        r = requests.post(
            '{}{}/{}'.format(
                self.url_base,
                base_id,
                endpoint
            ),
            headers=headers,
            json=data
        )
        if r.status_code != 200:
            error_message = 'Airtable POST request returned status code {}'.format(r.status_code)
            r.raise_for_status()
        return r.json()

    def get(
        self,
        base_id,
        endpoint,
        params=None
    ):
        headers = dict()
        if self.api_key is not None:
            headers['Authorization'] = 'Bearer {}'.format(self.api_key)
        r = requests.get(
            '{}{}/{}'.format(
                self.url_base,
                base_id,
                endpoint
            ),
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            error_message = 'Airtable GET request returned status code {}'.format(r.status_code)
            r.raise_for_status()
        return r.json()

def convert_tl_data_to_df(tl_data):
    if len(tl_data) == 0:
        return pd.DataFrame()
    tl_data_df = pd.DataFrame(
        tl_data,
        dtype='object'
    )
    tl_data_df['pull_datetime'] = pd.to_datetime(tl_data_df['pull_datetime'])
    tl_data_df['teacher_created_datetime_at'] = pd.to_datetime(tl_data_df['teacher_created_datetime_at'])
    # school_data_df['user_id_tc'] = pd.to_numeric(tl_data_df['user_id_tc']).astype('Int64')
    tl_data_df = tl_data_df.astype({
        'teacher_full_name_at': 'string',
        'teacher_middle_name_at': 'string',
        'teacher_last_name_at': 'string',
        'teacher_title_at': 'string',
        'teacher_ethnicity_at': 'string',
        'teacher_ethnicity_other_at': 'string',
        'teacher_income_background_at': 'string',
        'teacher_email_at': 'string',
        'teacher_email_2_at': 'string',
        'teacher_email_3_at': 'string',
        'teacher_phone_at': 'string',
        'teacher_phone_2_at': 'string',
        'teacher_employer_at': 'string',
        'hub_at': 'string',
        'pod_at': 'string',
        'user_id_tc': 'string'
    })
    tl_data_df.set_index('teacher_id_at', inplace=True)
    return tl_data_df

def convert_location_data_to_df(location_data):
    if len(location_data) == 0:
        return pd.DataFrame()
    location_data_df = pd.DataFrame(
        location_data,
        dtype='object'
    )
    location_data_df['pull_datetime'] = pd.to_datetime(location_data_df['pull_datetime'])
    location_data_df['location_created_datetime_at'] = pd.to_datetime(location_data_df['location_created_datetime_at'])
    location_data_df = location_data_df.astype({
        'location_id_at': 'string',
        'location_address_at': 'string',
        'school_id_at': 'string'
    })
    location_data_df.set_index('location_id_at', inplace=True)
    return location_data_df

def convert_teacher_school_data_to_df(teacher_school_data):
    if len(teacher_school_data) == 0:
        return pd.DataFrame()
    teacher_school_data_df = pd.DataFrame(
        teacher_school_data,
        dtype='object'
    )
    teacher_school_data_df['pull_datetime'] = pd.to_datetime(teacher_school_data_df['pull_datetime'])
    teacher_school_data_df['teacher_school_created_datetime_at'] = pd.to_datetime(teacher_school_data_df['teacher_school_created_datetime_at'])
    teacher_school_data_df = teacher_school_data_df.astype({
        'teacher_school_active_at': 'bool'
    })
    teacher_school_data_df.set_index('teacher_school_id_at', inplace=True)
    return teacher_school_data_df

def convert_school_data_to_df(school_data):
    if len(school_data) == 0:
        return pd.DataFrame()
    school_data_df = pd.DataFrame(
        school_data,
        dtype='object'
    )
    school_data_df['pull_datetime'] = pd.to_datetime(school_data_df['pull_datetime'])
    school_data_df['school_created_datetime_at'] = pd.to_datetime(school_data_df['school_created_datetime_at'])
    school_data_df['hub_id_at'] = school_data_df['hub_id_at'].apply(wf_core_data.utils.to_singleton)
    school_data_df['pod_id_at'] = school_data_df['pod_id_at'].apply(wf_core_data.utils.to_singleton)
    school_data_df['school_id_tc'] = pd.to_numeric(school_data_df['school_id_tc']).astype('Int64')
    school_data_df = school_data_df.astype({
        'school_id_at': 'string',
        'hub_id_at': 'string',
        'pod_id_at': 'string',
        'school_name_at': 'string',
        'school_short_name_at': 'string',
        'school_status_at': 'string',
        'school_ssj_stage_at': 'string',
        'school_governance_model_at': 'string',
    })
    school_data_df.set_index('school_id_at', inplace=True)
    return school_data_df

def convert_hub_data_to_df(hub_data):
    if len(hub_data) == 0:
        return pd.DataFrame()
    hub_data_df = pd.DataFrame(
        hub_data,
        dtype='object'
    )
    hub_data_df['pull_datetime'] = pd.to_datetime(hub_data_df['pull_datetime'])
    hub_data_df['hub_created_datetime_at'] = pd.to_datetime(hub_data_df['hub_created_datetime_at'])
    hub_data_df = hub_data_df.astype({
        'hub_id_at': 'string',
        'hub_name_at': 'string'
    })
    hub_data_df.set_index('hub_id_at', inplace=True)
    return hub_data_df

def convert_pod_data_to_df(pod_data):
    if len(pod_data) == 0:
        return pd.DataFrame()
    pod_data_df = pd.DataFrame(
        pod_data,
        dtype='object'
    )
    pod_data_df['pull_datetime'] = pd.to_datetime(pod_data_df['pull_datetime'])
    pod_data_df['pod_created_datetime_at'] = pd.to_datetime(pod_data_df['pod_created_datetime_at'])
    pod_data_df = pod_data_df.astype({
        'pod_id_at': 'string',
        'pod_name_at': 'string'
    })
    pod_data_df.set_index('pod_id_at', inplace=True)
    return pod_data_df

def convert_ethnicity_categories_to_df(ethnicity_categories):
    if len(ethnicity_categories) == 0:
        return pd.DataFrame()
    ethnicity_categories_df = pd.DataFrame(
        ethnicity_categories,
        dtype='object'
    )
    ethnicity_categories_df['pull_datetime'] = pd.to_datetime(ethnicity_categories_df['pull_datetime'])
    ethnicity_categories_df['ethnicity_category_created_datetime_at'] = pd.to_datetime(ethnicity_categories_df['ethnicity_category_created_datetime_at'])
    ethnicity_categories_df = ethnicity_categories_df.astype({
        'ethnicity_category_id_at': 'string',
        'ethnicity_category': 'string',
        'ethnicity_display_name_english': 'string',
        'ethnicity_display_name_spanish': 'string'
    })
    ethnicity_categories_df.set_index('ethnicity_category', inplace=True)
    return ethnicity_categories_df

def convert_gender_categories_to_df(gender_categories):
    if len(gender_categories) == 0:
        return pd.DataFrame()
    gender_categories_df = pd.DataFrame(
        gender_categories,
        dtype='object'
    )
    gender_categories_df['pull_datetime'] = pd.to_datetime(gender_categories_df['pull_datetime'])
    gender_categories_df['gender_category_created_datetime_at'] = pd.to_datetime(gender_categories_df['gender_category_created_datetime_at'])
    gender_categories_df = gender_categories_df.astype({
        'gender_category_id_at': 'string',
        'gender_category': 'string',
        'gender_display_name_english': 'string',
        'gender_display_name_spanish': 'string'
    })
    gender_categories_df.set_index('gender_category', inplace=True)
    return gender_categories_df

def convert_household_income_categories_to_df(household_income_categories):
    if len(household_income_categories) == 0:
        return pd.DataFrame()
    household_income_categories_df = pd.DataFrame(
        household_income_categories,
        dtype='object'
    )
    household_income_categories_df['pull_datetime'] = pd.to_datetime(household_income_categories_df['pull_datetime'])
    household_income_categories_df['household_income_category_created_datetime_at'] = pd.to_datetime(household_income_categories_df['household_income_category_created_datetime_at'])
    household_income_categories_df = household_income_categories_df.astype({
        'household_income_category_id_at': 'string',
        'household_income_category': 'string',
        'household_income_display_name_english': 'string',
        'household_income_display_name_spanish': 'string'
    })
    household_income_categories_df.set_index('household_income_category', inplace=True)
    return household_income_categories_df

def convert_nps_categories_to_df(nps_categories):
    if len(nps_categories) == 0:
        return pd.DataFrame()
    nps_categories_df = pd.DataFrame(
        nps_categories,
        dtype='object'
    )
    nps_categories_df['pull_datetime'] = pd.to_datetime(nps_categories_df['pull_datetime'])
    nps_categories_df['nps_category_created_datetime_at'] = pd.to_datetime(nps_categories_df['nps_category_created_datetime_at'])
    nps_categories_df = nps_categories_df.astype({
        'nps_category_id_at': 'string',
        'nps_category': 'string',
        'nps_display_name_english': 'string',
        'nps_display_name_spanish': 'string'
    })
    nps_categories_df.set_index('nps_category', inplace=True)
    return nps_categories_df

def convert_boolean_categories_to_df(boolean_categories):
    if len(boolean_categories) == 0:
        return pd.DataFrame()
    boolean_categories_df = pd.DataFrame(
        boolean_categories,
        dtype='object'
    )
    boolean_categories_df['pull_datetime'] = pd.to_datetime(boolean_categories_df['pull_datetime'])
    boolean_categories_df['boolean_category_created_datetime_at'] = pd.to_datetime(boolean_categories_df['boolean_category_created_datetime_at'])
    boolean_categories_df = boolean_categories_df.astype({
        'boolean_category_id_at': 'string',
        'boolean_category': 'string',
        'boolean_display_name_english': 'string',
        'boolean_display_name_spanish': 'string'
    })
    boolean_categories_df.set_index('boolean_category', inplace=True)
    return boolean_categories_df
