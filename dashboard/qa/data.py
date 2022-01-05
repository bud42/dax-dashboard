import logging
import os
from datetime import datetime, date, timedelta

import dax

import utils
from qa.params import SCAN_EXCLUDE_LIST, ASSR_EXCLUDE_LIST

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd


# Data sources are:
# XNAT (VUIIS XNAT at Vanderbilt)

# TODO: use REDCap (project settings REDCap instance to filter types)

# This app does not access ACCRE or SLURM. The ony local file access is to 
# write/read the cached data in a pickle file. We save to pickle the results
#  of each query, we reuse the pickle data when a filter changes. Then anytime 
# user clicks refresh, we query xnat again.

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')



ASSR_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
proc:genprocdata/label,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/validation/status'


ASSR_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'session_type': 'SESSTYPE',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'proc:genprocdata/label': 'ASSR',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/validation/status': 'QCSTATUS',
    'xsiType': 'XSITYPE'}


SCAN_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagescandata/id,\
xnat:imagescandata/type,\
xnat:imagescandata/quality'


SCAN_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'session_type': 'SESSTYPE',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'xnat:imagescandata/id': 'SCANID',
    'xnat:imagescandata/type': 'SCANTYPE',
    'xnat:imagescandata/quality': 'QUALITY',
    'xsiType': 'XSITYPE'}


SCAN_STATUS_MAP = {
    'usable': 'P',
    'questionable': 'P',
    'unusable': 'F'}


ASSR_STATUS_MAP = {
    'Passed': 'P',
    'Good': 'P',
    'Passed with edits': 'P',
    'Questionable': 'P',
    'Failed': 'F',
    'Bad': 'F',
    'Needs QA': 'Q',
    'Do Not Run': 'N'}


QA_COLS = [
    'SESSION', 'PROJECT', 'SITE', 'DATE', 'TYPE', 'STATUS',
    'ARTTYPE', 'SCANTYPE', 'PROCTYPE', 'XSITYPE', 'SESSTYPE']


def get_filename():
    return '{}.pkl'.format('qadata')


def run_refresh(filename):
    proj_filter = []
    proc_filter = []
    scan_filter = []

    # force a requery
    with dax.XnatUtils.get_interface() as xnat:
        proj_filter = utils.get_user_favorites(xnat)
        df = get_data(xnat, proj_filter, proc_filter, scan_filter)

    save_data(df, filename)

    return df


# TODO: combine these load_x_options to only read the file once
def load_scan_options(project_filter=None):
    # Read stypes from file and filter by projects

    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logging.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    if project_filter:
        scantypes = df[df.PROJECT.isin(project_filter)].SCANTYPE.unique()
    else:
        scantypes = df.SCANTYPE.unique()

    scantypes = [x for x in scantypes if x]

    return sorted(scantypes)

# TODO: combine these load_x_options to only read the file once
def load_sess_options(project_filter=None):
    # Read stypes from file and filter by projects

    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logging.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    if project_filter:
        sesstypes = df[df.PROJECT.isin(project_filter)].SESSTYPE.unique()
    else:
        sesstypes = df.SESSTYPE.unique()

    sesstypes = [x for x in sesstypes if x]

    return sorted(sesstypes)


def load_proc_options(project_filter=None):
    # Read ptypes from file and filter by projects

    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logging.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    if project_filter:
        proctypes = df[df.PROJECT.isin(project_filter)].PROCTYPE.unique()
    else:
        proctypes = df.PROCTYPE.unique()

    proctypes = [x for x in proctypes if x]

    return sorted(proctypes)


def load_proj_options():
    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logging.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    return sorted(df.PROJECT.unique())


def load_data(refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        # TODO: check for old file and refresh too
        run_refresh(filename)

    logging.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def set_modality(row):
    xsi = row['XSITYPE']
    mod = 'UNK'
    
    # TODO: use a dict/map with default value

    if xsi == 'xnat:eegSessionData':
        mod = 'EEG'
    elif xsi == 'xnat:mrSessionData':
        mod = 'MR'
    elif xsi == 'xnat:petSessionData':
        mod = 'PET'

    return mod



def get_data(xnat, proj_filter, stype_filter, ptype_filter):
    # Load that data
    scan_df = load_scan_data(xnat, proj_filter)
    assr_df = load_assr_data(xnat, proj_filter)

    # Make a common column for type
    assr_df['TYPE'] = assr_df['PROCTYPE']
    scan_df['TYPE'] = scan_df['SCANTYPE']

    assr_df['SCANTYPE'] = None
    scan_df['PROCTYPE'] = None

    assr_df['ARTTYPE'] = 'assessor'
    scan_df['ARTTYPE'] = 'scan'

    # Concatenate the common cols to a new dataframe
    df = pd.concat([assr_df[QA_COLS], scan_df[QA_COLS]], sort=False)

    # relabel caare
    df.PROJECT = df.PROJECT.replace(['TAYLOR_CAARE'], 'CAARE')

    # set modality
    df['MODALITY'] = df.apply(set_modality, axis=1)

    print('end of get_data columns=', df.columns)

    return df


def load_assr_data(xnat, project_filter):
    logging.info('loading XNAT data, projects={}'.format(project_filter))

    # Build the uri to query with filters and run it
    _uri = ASSR_URI
    _uri += '&project={}'.format(','.join(project_filter))
    print(_uri)
    _json = utils.get_json(xnat, _uri)
    dfa = pd.DataFrame(_json['ResultSet']['Result'])

    # Rename columns
    dfa.rename(columns=ASSR_RENAME, inplace=True)

    # Get subset of columns
    dfa = dfa[[
        'PROJECT', 'SESSION', 'SUBJECT', 'DATE', 'SITE', 'ASSR',
        'QCSTATUS', 'PROCSTATUS', 'PROCTYPE', 'XSITYPE', 'SESSTYPE']].copy()

    dfa.drop_duplicates(inplace=True)

    # Filter out excluded types
    dfa = dfa[~dfa['PROCTYPE'].isin(ASSR_EXCLUDE_LIST)]

    # Drop any rows with empty proctype
    dfa.dropna(subset=['PROCTYPE'], inplace=True)
    dfa = dfa[dfa.PROCTYPE != '']

    # Create shorthand status
    dfa['STATUS'] = dfa['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

    # Handle failed jobs
    dfa['STATUS'][dfa.PROCSTATUS == 'JOB_FAILED'] = 'X'

    # Handle running jobs
    dfa['STATUS'][dfa.PROCSTATUS == 'JOB_RUNNING'] = 'R'

    return dfa


def load_scan_data(xnat, project_filter):
    #  Load data
    logging.info('loading XNAT scan data, projects={}'.format(project_filter))

    # Build the uri query with filters and run it
    _uri = SCAN_URI
    _uri += '&project={}'.format(','.join(project_filter))
    print(_uri)
    _json = utils.get_json(xnat, _uri)
    dfs = pd.DataFrame(_json['ResultSet']['Result'])

    # Rename columns, get subset and drop dupes
    dfs.rename(columns=SCAN_RENAME, inplace=True)
    dfs = dfs[[
        'PROJECT', 'SESSION', 'SUBJECT', 'DATE', 'SITE', 'SCANID',
        'SCANTYPE', 'QUALITY', 'XSITYPE', 'SESSTYPE']].copy()
    dfs.drop_duplicates(inplace=True)

    # Filter out excluded types
    dfs = dfs[~dfs['SCANTYPE'].isin(SCAN_EXCLUDE_LIST)]

    # Drop any rows with empty type
    dfs.dropna(subset=['SCANTYPE'], inplace=True)
    dfs = dfs[dfs.SCANTYPE != '']

    # Create shorthand status
    dfs['STATUS'] = dfs['QUALITY'].map(SCAN_STATUS_MAP).fillna('U')

    return dfs


def filter_data(df, projects, proctypes, scantypes, timeframe, sesstypes):

    # Filter by project
    if projects:
        logging.debug('filtering by project:')
        logging.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by artefact type
    #if arttype == 'assessor':
    #    # only assessors
    #    df = df[df['ARTTYPE'] == 'assessor']
    #elif arttype == 'scan':
    #    # only scans
    #    df = df[df['ARTTYPE'] == 'scan']

    # Filter by proc type
    if proctypes:
        logging.debug('filtering by proc types:')
        logging.debug(proctypes)
        df = df[(df['PROCTYPE'].isin(proctypes)) | (df['ARTTYPE'] == 'scan')]

    # Filter by scan type
    if scantypes:
        logging.debug('filtering by scan types:')
        logging.debug(scantypes)
        df = df[(df['SCANTYPE'].isin(scantypes)) | (df['ARTTYPE'] == 'assessor')]

    # Filter by timeframe
    if timeframe in ['1day', '7day', '30day', '365day']:
        logging.debug('filtering by ' + timeframe)
        then_datetime = datetime.now() - pd.to_timedelta(timeframe)
        df = df[pd.to_datetime(df.DATE) > then_datetime]
    elif timeframe == 'lastmonth':
        logging.debug('filtering by ' + timeframe)

        # Set range to first and last day of previous month
        _end = date.today().replace(day=1) - timedelta(days=1)
        _start = date.today().replace(day=1) - timedelta(days=_end.day)
        print(_start, _end)
        df = df[pd.to_datetime(df.DATE).isin(pd.date_range(_start, _end))]
    else:
        # ALL
        logging.debug('not filtering by time')
        pass

    # Filter by sesstype
    if sesstypes:
        df = df[df['SESSTYPE'].isin(sesstypes)]

    return df
