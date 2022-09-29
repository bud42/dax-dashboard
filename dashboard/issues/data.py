import logging
import os
from datetime import datetime

import dax
import redcap

import utils

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

API_URL = 'https://redcap.vanderbilt.edu/api/'
KEYFILE = os.path.join(os.path.expanduser('~'), '.redcap.txt')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def get_projectkey(projectid, keyfile):
    # Load the dictionary
    d = {}
    with open(keyfile) as f:
        for line in f:
            if line == '':
                continue

            try:
                (i, k, n) = line.strip().split(',')
                d[i] = k
            except:
                pass

    return d.get(projectid, None)


# This is where we save our cache of the data
def get_filename():
    return '{}.pkl'.format('issuesdata')


def get_data(xnat, proj_filter):
    df = pd.DataFrame()
    dfi = pd.DataFrame()

    logging.info('loading issues from REDCap')
    dfi = load_issues()

    # Concatentate all the dataframes into one
    df = pd.concat([dfi], ignore_index=True)

    # Sort by date and reset index
    df.sort_values(by=['DATETIME'], inplace=True, ascending=False)
    df.reset_index(inplace=True)
    df['ID'] = df.index

    return df


def load_issues():
    # type,project,subject,session,date,event,field,description

    # Connect to the main redcap to load currently open issues
    try:
        logging.info('connecting to redcap')
        # TODO: get main id from keyfile
        k = get_projectkey('156730', KEYFILE)
        project = redcap.Project(API_URL, k)
        logging.info('exporting records')
        df = project.export_records(forms=['issues'], format='df')
    except Exception as err:
        logging.error('failed to load issues')
        return pd.DataFrame(columns=[
            'ID', 'LABEL', 'PROJECT', 'SUBJECT', 'SESSION',
            'EVENT', 'FIELD', 'CATEGORY', 'STATUS', 'SOURCE',
            'DESCRIPTION', 'DATETIME'
        ])

    logging.debug('transforming records')
    df = df[df['redcap_repeat_instrument'] == 'issues']
    df = df[df['issues_complete'] != '2']

    df['PROJECT'] = df.index
    df.rename(inplace=True, columns={
        'redcap_repeat_instance': 'ID',
        'issue_subject': 'SUBJECT',
        'issue_session': 'SESSION',
        'issue_event': 'EVENT',
        'issue_field': 'FIELD',
        'issue_type': 'CATEGORY',
        'issue_description': 'DESCRIPTION',
        'issue_date': 'DATETIME',
    })
    df['STATUS'] = 'FAIL'
    df['SOURCE'] = 'ccmutils'
    df['LABEL'] = df['ID']
    df['DESCRIPTION']  = df['DESCRIPTION'].str.slice(0,50)

    return df


def run_refresh(filename):
    proj_filter = []

    with dax.XnatUtils.get_interface() as xnat:
        proj_filter = utils.get_user_favorites(xnat)
        df = get_data(xnat, proj_filter)

    save_data(df, filename)

    return df

def load_field_options(fieldname):
    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logging.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    _options = df[fieldname].unique()

    _options = [x for x in _options if x]

    return sorted(_options)


def load_category_options():
    return load_field_options('CATEGORY')


def load_project_options():
    return load_field_options('PROJECT')


def load_source_options():
    return load_field_options('SOURCE')


def load_data(refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        run_refresh(filename)

    logging.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def filter_data(df, projects, categories, sources):
    # Filter by project
    if projects:
        logging.debug('filtering by project:')
        logging.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by category
    if categories:
        logging.debug('filtering by category:')
        logging.debug(categories)
        df = df[(df['CATEGORY'].isin(categories))]

    # Filter by source
    if sources:
        logging.debug('filtering by source:')
        logging.debug(sources)
        df = df[(df['SOURCE'].isin(sources))]

    return df
