import pandas as pd
import numpy as np
import scipy.stats
import re

INT_RE = re.compile(r'[0-9]+')

DAYS_PER_YEAR = 365.25
MONTHS_PER_YEAR = 12
DEFAULT_MIN_GROWTH_DAYS = 120
DEFAULT_SCHOOL_YEAR_DURATION_MONTHS = 9

def calculate_percentile_growth_per_school_year(
    starting_percentile,
    ending_percentile,
    days_between_tests,
    min_growth_days=DEFAULT_MIN_GROWTH_DAYS,
    school_year_duration_months=DEFAULT_SCHOOL_YEAR_DURATION_MONTHS
):
    if days_between_tests < min_growth_days:
        return np.nan
    starting_z = scipy.stats.norm.ppf(starting_percentile/100.0)
    ending_z = scipy.stats.norm.ppf(ending_percentile/100.0)
    z_growth = ending_z - starting_z
    z_growth_per_school_year = calculate_score_growth_per_school_year(
        score_growth=z_growth,
        days_between_tests=days_between_tests,
        school_year_duration_months=school_year_duration_months
    )
    ending_z_school_year = starting_z + z_growth_per_school_year
    ending_percentile_school_year = scipy.stats.norm.cdf(ending_z_school_year)*100.0
    percentile_growth_per_school_year = ending_percentile_school_year - starting_percentile
    return percentile_growth_per_school_year

def calculate_score_growth_per_school_year(
    score_growth,
    days_between_tests,
    min_growth_days=DEFAULT_MIN_GROWTH_DAYS,
    school_year_duration_months=DEFAULT_SCHOOL_YEAR_DURATION_MONTHS
):
    if days_between_tests < min_growth_days:
        return np.nan
    score_growth_per_school_year = (DAYS_PER_YEAR*(school_year_duration_months/MONTHS_PER_YEAR)/days_between_tests)*score_growth
    return score_growth_per_school_year

def to_datetime(object):
    try:
        datetime = pd.to_datetime(object, utc=True).to_pydatetime()
        if pd.isnull(datetime):
            date = None
    except:
        datetime = None
    return datetime

def to_date(object):
    try:
        date = pd.to_datetime(object).date()
        if pd.isnull(date):
            date = None
    except:
        date = None
    return date

def to_singleton(object):
    try:
        num_elements = len(object)
        if num_elements > 1:
            raise ValueError('More than one element in object. Conversion to singleton failed')
        if num_elements == 0:
            return None
        return object[0]
    except:
        return object

def to_boolean(object):
    if isinstance(object, bool):
        return object
    if isinstance(object, str):
        if object in ['True', 'true', 'TRUE', 'T']:
            return True
        if object in ['False', 'false', 'FALSE', 'F']:
            return False
        return None
    if isinstance(object, int):
        if object == 1:
            return True
        if object == 0:
            return False
        return None
    return None

def extract_alphanumeric(object):
    if pd.isna(object):
        return None
    try:
        object_string = str(object)
    except:
        return None
    alphanumeric_string = ''.join(ch for ch in object_string if ch.isalnum())
    return alphanumeric_string

def extract_int(object):
    if pd.isna(object):
        return None
    try:
        object_string = str(object)
    except:
        return None
    m = INT_RE.search(object_string)
    if m:
        return pd.to_numeric(m[0]).astype('int')
    else:
        return None

def infer_school_year(
    date,
    rollover_month=7,
    rollover_day=31
):
    if pd.isna(date):
        return None
    if date.month <= rollover_month and date.day <= rollover_day:
        return '{}-{}'.format(
            date.year - 1,
            date.year
        )
    else:
        return '{}-{}'.format(
            date.year,
            date.year + 1
        )

def filter_dataframe(
    dataframe,
    filter_dict=None
):
    if filter_dict is None:
        return dataframe
    index_columns = dataframe.index.names
    dataframe=dataframe.reset_index()
    for key, value_list in filter_dict.items():
        dataframe = dataframe.loc[dataframe[key].isin(value_list)]
    dataframe.set_index(index_columns, inplace=True)
    return dataframe

def select_from_dataframe(
    dataframe,
    select_dict=None
):
    if select_dict is None:
        return dataframe
    keys, values = zip(*select_dict.items())
    for level, value in select_dict.items():
        dataframe = select_index_level(
            dataframe,
            value=value,
            level=level
        )
    return dataframe

def select_index_level(
    dataframe,
    value,
    level
):
    dataframe = (
        dataframe
        .loc[dataframe.index.get_level_values(level) == value]
        .reset_index(level=level, drop=True)
    )
    return dataframe
