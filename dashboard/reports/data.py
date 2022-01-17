import logging
import os
from datetime import datetime, date, timedelta
import re

import utils
import qa.data as qa_data
from qa.gui import qa_pivot
import stats.data as stats_data
import activity.data as activity_data


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')


SESSCOLUMNS = ['SESSION', 'PROJECT', 'DATE', 'SESSTYPE', 'SITE', 'MODALITY'] 


def load_session_info(project):
    df = qa_data.load_data()
    df = df[df.PROJECT == project]
    df = df[SESSCOLUMNS].drop_duplicates().sort_values('SESSION')
    return df


def load_phantom_info():
    return None


def load_activity_info(project):
    df = activity_data.load_data()
    df = df[df.PROJECT == project]
    df['CATEGORY'] = df['DESCRIPTION'].str.split(':', n=1).str[0]
    df['LABEL'] = df['DESCRIPTION'].str.split(':', n=1).str[1]
    df['STATUS'] = 'UNKNOWN'

    return df


def load_stats(project, stattypes):
    # Load that data
    df = stats_data.load_data()
    if df.empty:
        return df

    # Filter by project
    df = df[df.PROJECT == project]

    # Return the DataFrame
    return df


def load_scanqa_info(project, scantypes):
    # Load that data
    df = qa_data.load_data()
    df = df[df.PROJECT == project].sort_values('SESSION')
    dfp = qa_pivot(df).reset_index()
    if not scantypes:
        scantypes = [x for x in dfp.columns if not re.search('_v\d+$', x)]
        #print('scan_types=', scantypes)

    # Filter columns to include
    include_list = SESSCOLUMNS + scantypes
    include_list = [x for x in include_list if x in dfp.columns]
    include_list = list(set(include_list))
    #print('include_list=', include_list)
    dfp = dfp[include_list]

    # Drop columns that are all empty
    dfp = dfp.dropna(axis=1, how='all')

    return dfp


def load_assrqa_info(project, assrtypes):
    # Load that data
    df = qa_data.load_data()
    df = df[df.PROJECT == project].sort_values('SESSION')
    dfp = qa_pivot(df).reset_index()
    if not assrtypes:
        assrtypes = [x for x in dfp.columns if re.search('_v\d+$', x)]
        #print('assr_types=', assrtypes)

    # Filter columns to include
    include_list = SESSCOLUMNS + assrtypes
    include_list = [x for x in include_list if x in dfp.columns]
    include_list = list(set(include_list))
    #print('include_list=', include_list)
    dfp = dfp[include_list]

    return dfp
