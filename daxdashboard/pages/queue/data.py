import logging
import os

import pandas as pd

from ...log import logger
from .. import utils
from ...utils import load_task_data
from ...extensions import cache


def get_data():

    df = _load_task_data()
    print(df)

    df = df[df.STATUS != 'NEED_INPUTS']

    df.reset_index(inplace=True)
    df['LABEL'] = df['ASSESSOR']

    df = df.apply(_get_proctype, axis=1)

    return df


def _load_task_data(projects=None):
    """List of task records."""

    df = load_task_data()

    return df


def _get_proctype(row):
    try:
        if row['YAMLUPLOAD']:
            tmp = os.path.basename(row['YAMLUPLOAD'])
        else:
            tmp = os.path.basename(row['YAMLFILE'])

        # Get just the filename without the directory path
        # Split on periods and grab the 4th value from right,
        # thus allowing periods in the main processor name
        row['PROCTYPE'] = tmp.rsplit('.')[-4]
        row['PROCESSOR'] = tmp
    except (KeyError, IndexError):
        row['PROCTYPE'] = ''
        row['PROCESSOR'] = ''

    return row


def run_refresh():
    df = get_data()

    save_data(df)

    return df


def read_data():
    df = cache.get('queuedata')

    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=['ID', 'PROJECT', 'PROCTYPE', 'USER'])

    return df


def save_data(df):
    # save to cache
    cache.set('queuedata', df)


def load_data(refresh=False):
    if refresh:
        print('queue.data.load_data-refresh')
        df = run_refresh()
    else:
        print('queue.data.load_data-read')
        df = read_data()

    return df


def filter_data(df, proj, proc, user):
    # Filter by project
    if proj:
        logger.debug(f'filtering by project:{proj}')
        df = df[df['PROJECT'].isin(proj)]

    # Filter by proc
    if proc:
        logger.debug(f'filtering by proc:{proc}')
        df = df[(df['PROCTYPE'].isin(proc))]

    # Filter by user
    if user:
        logger.debug(f'filtering by user:{user}')
        df = df[(df['USER'].isin(user))]

    return df
