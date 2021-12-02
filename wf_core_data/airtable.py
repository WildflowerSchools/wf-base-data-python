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

    def fetch_family_survey_school_inputs(
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
        logger.info('Fetching family survey school inputs from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Family survey - school inputs',
            params=params
        )
        school_inputs=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('school_input_id_at', record.get('id')),
                ('school_input_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('school_id_at', fields.get('Schools')),
                ('include_school_in_data', fields.get('Include in data')),
                ('include_school_in_reporting', fields.get('Include in reporting')),
                ('school_data_pending', fields.get('Data pending')),
                ('school_report_language', fields.get('Report language'))
            ])
            school_inputs.append(datum)
        if format == 'dataframe':
            school_inputs = convert_school_inputs_to_df(school_inputs)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return school_inputs

    def fetch_family_survey_hub_inputs(
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
        logger.info('Fetching family survey hub inputs from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Family survey - hub inputs',
            params=params
        )
        hub_inputs=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('hub_input_id_at', record.get('id')),
                ('hub_input_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('hub_id_at', fields.get('Hubs')),
                ('include_hub_in_reporting', fields.get('Include in reporting')),
                ('hub_data_pending', fields.get('Data pending'))
            ])
            hub_inputs.append(datum)
        if format == 'dataframe':
            hub_inputs = convert_hub_inputs_to_df(hub_inputs)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return hub_inputs

    def fetch_family_survey_excluded_classroom_inputs(
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
        logger.info('Fetching family survey excluded classroom inputs from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Family survey - excluded classroom inputs',
            params=params
        )
        excluded_classroom_inputs = list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('excluded_classroom_input_id_at', record.get('id')),
                ('excluded_classroom_input_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('school_id_at', fields.get('Schools')),
                ('classroom_id_tc', fields.get('TC classroom ID'))
            ])
            excluded_classroom_inputs.append(datum)
        if format == 'dataframe':
            excluded_classroom_inputs = convert_excluded_classroom_inputs_to_df(excluded_classroom_inputs)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return excluded_classroom_inputs

    def fetch_family_survey_excluded_student_inputs(
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
        logger.info('Fetching family survey excluded student inputs from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Family survey - excluded student inputs',
            params=params
        )
        excluded_student_inputs = list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('excluded_student_input_id_at', record.get('id')),
                ('excluded_student_input_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('school_id_at', fields.get('Schools')),
                ('student_id_tc', fields.get('TC student ID'))
            ])
            excluded_student_inputs.append(datum)
        if format == 'dataframe':
            excluded_student_inputs = convert_excluded_student_inputs_to_df(excluded_student_inputs)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return excluded_student_inputs

    def fetch_family_survey_field_name_inputs(
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
        logger.info('Fetching family survey field name inputs from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Family survey - field name inputs',
            params=params
        )
        field_name_inputs = list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('field_name_input_id_at', record.get('id')),
                ('field_name_input_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('source_field_name', fields.get('Source field name')),
                ('target_field_name', fields.get('Target field name'))
            ])
            field_name_inputs.append(datum)
        if format == 'dataframe':
            field_name_inputs = convert_field_name_inputs_to_df(field_name_inputs)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return field_name_inputs

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

def convert_school_inputs_to_df(school_inputs):
    if len(school_inputs) == 0:
        return pd.DataFrame()
    school_inputs_df = pd.DataFrame(
        school_inputs,
        dtype='object'
    )
    school_inputs_df['pull_datetime'] = pd.to_datetime(school_inputs_df['pull_datetime'])
    school_inputs_df['school_input_created_datetime_at'] = pd.to_datetime(school_inputs_df['school_input_created_datetime_at'])
    school_inputs_df['school_id_at'] = school_inputs_df['school_id_at'].apply(wf_core_data.utils.to_singleton)
    school_inputs_df = school_inputs_df.astype({
        'school_input_id_at': 'string',
        'school_id_at': 'string',
        'include_school_in_data': 'bool',
        'include_school_in_reporting': 'bool',
        'school_data_pending': 'bool'
    })
    school_inputs_df.set_index('school_input_id_at', inplace=True)
    return school_inputs_df

def convert_hub_inputs_to_df(hub_inputs):
    if len(hub_inputs) == 0:
        return pd.DataFrame()
    hub_inputs_df = pd.DataFrame(
        hub_inputs,
        dtype='object'
    )
    hub_inputs_df['pull_datetime'] = pd.to_datetime(hub_inputs_df['pull_datetime'])
    hub_inputs_df['hub_input_created_datetime_at'] = pd.to_datetime(hub_inputs_df['hub_input_created_datetime_at'])
    hub_inputs_df['hub_id_at'] = hub_inputs_df['hub_id_at'].apply(wf_core_data.utils.to_singleton)
    hub_inputs_df = hub_inputs_df.astype({
        'hub_input_id_at': 'string',
        'hub_id_at': 'string',
        'include_hub_in_reporting': 'bool',
        'hub_data_pending': 'bool'
    })
    hub_inputs_df.set_index('hub_input_id_at', inplace=True)
    return hub_inputs_df

def convert_excluded_classroom_inputs_to_df(excluded_classroom_inputs):
    if len(excluded_classroom_inputs) == 0:
        return pd.DataFrame()
    excluded_classroom_inputs_df = pd.DataFrame(
        excluded_classroom_inputs,
        dtype='object'
    )
    excluded_classroom_inputs_df['pull_datetime'] = pd.to_datetime(excluded_classroom_inputs_df['pull_datetime'])
    excluded_classroom_inputs_df['excluded_classroom_input_created_datetime_at'] = pd.to_datetime(excluded_classroom_inputs_df['excluded_classroom_input_created_datetime_at'])
    excluded_classroom_inputs_df['school_id_at'] = excluded_classroom_inputs_df['school_id_at'].apply(wf_core_data.utils.to_singleton)
    excluded_classroom_inputs_df = excluded_classroom_inputs_df.astype({
        'excluded_classroom_input_id_at': 'string',
        'school_id_at': 'string',
        'classroom_id_tc': 'Int64'
    })
    excluded_classroom_inputs_df.set_index('excluded_classroom_input_id_at', inplace=True)
    return excluded_classroom_inputs_df

def convert_excluded_student_inputs_to_df(excluded_student_inputs):
    if len(excluded_student_inputs) == 0:
        return pd.DataFrame()
    excluded_student_inputs_df = pd.DataFrame(
        excluded_student_inputs,
        dtype='object'
    )
    excluded_student_inputs_df['pull_datetime'] = pd.to_datetime(excluded_student_inputs_df['pull_datetime'])
    excluded_student_inputs_df['excluded_student_input_created_datetime_at'] = pd.to_datetime(excluded_student_inputs_df['excluded_student_input_created_datetime_at'])
    excluded_student_inputs_df['school_id_at'] = excluded_student_inputs_df['school_id_at'].apply(wf_core_data.utils.to_singleton)
    excluded_student_inputs_df = excluded_student_inputs_df.astype({
        'excluded_student_input_id_at': 'string',
        'school_id_at': 'string',
        'student_id_tc': 'Int64'
    })
    excluded_student_inputs_df.set_index('excluded_student_input_id_at', inplace=True)
    return excluded_student_inputs_df

def convert_field_name_inputs_to_df(field_name_inputs):
    if len(field_name_inputs) == 0:
        return pd.DataFrame()
    field_name_inputs_df = pd.DataFrame(
        field_name_inputs,
        dtype='object'
    )
    field_name_inputs_df['pull_datetime'] = pd.to_datetime(field_name_inputs_df['pull_datetime'])
    field_name_inputs_df['field_name_input_created_datetime_at'] = pd.to_datetime(field_name_inputs_df['field_name_input_created_datetime_at'])
    field_name_inputs_df = field_name_inputs_df.astype({
        'field_name_input_id_at': 'string',
        'source_field_name': 'string',
        'target_field_name': 'string'
    })
    field_name_inputs_df.set_index('field_name_input_id_at', inplace=True)
    return field_name_inputs_df
