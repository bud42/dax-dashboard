import logging
import os

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


BOTH_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
subject_label,\
session_label,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagescandata/id,\
xnat:imagescandata/type,\
xnat:imagescandata/quality,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
proc:genprocdata/label,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/validation/status'

BOTH_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'proc:genprocdata/label': 'ASSR',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/validation/status': 'QCSTATUS',
    'xnat:imagescandata/id': 'SCANID',
    'xnat:imagescandata/type': 'SCANTYPE',
    'xnat:imagescandata/quality': 'QUALITY'}

ASSR_URI = '/REST/experiments?xsiType=proc:genprocdata\
&columns=\
ID,\
label,\
project,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/validation/status'

ASSR_RENAME = {
    'ID': 'ID',
    'session_label': 'SESSION',
    'label': 'LABEL',
    'project': 'PROJECT',
    'xnat:imagesessiondata/date': 'DATE',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/validation/status': 'QCSTATUS'}

SCAN_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
ID,\
label,\
project,\
URI,\
subject_label,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagescandata/id,\
xnat:imagescandata/type,\
xnat:imagescandata/quality,\
xnat:imagesessiondata/date'


SCAN_RENAME = {
    'ID': 'ID',
    'label': 'SESSION',
    'project': 'PROJECT',
    'URI': 'URI',
    'subject_label': 'SUBJECT',
    'xnat:imagescandata/id': 'SCANID',
    'xnat:imagescandata/type': 'SCANTYPE',
    'xnat:imagescandata/quality': 'QUALITY',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE'}

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

QA_COLS = ['SESSION', 'PROJECT', 'DATE', 'TYPE', 'STATUS', 'ARTTYPE', 'SCANTYPE', 'PROCTYPE']



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

    return df.PROJECT.unique()


def load_data(proj_filter=None, scan_filter=None, proc_filter=None, refresh=False):
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


def get_data(xnat, proj_filter, stype_filter, ptype_filter):
    # Load that data
    scan_df, assr_df = load_both_data(xnat, proj_filter)

    # Make a common column for type
    assr_df['TYPE'] = assr_df['PROCTYPE']
    scan_df['TYPE'] = scan_df['SCANTYPE']

    assr_df['SCANTYPE'] = None
    scan_df['PROCTYPE'] = None

    assr_df['ARTTYPE'] = 'assessor'
    scan_df['ARTTYPE'] = 'scan'

    # Concatenate the common cols to a new dataframe
    df = pd.concat([assr_df[QA_COLS], scan_df[QA_COLS]], sort=False)

    # set a column for session visit type, i.e. baseline if session name
    # ends with a or MR1 or something else, otherwise it's a followup
    df['ISBASELINE'] = df['SESSION'].apply(utils.is_baseline_session)

    # relabel caare
    df.PROJECT = df.PROJECT.replace(['TAYLOR_CAARE'], 'CAARE')

    return df


def load_both_data(xnat, project_filter):
    #  Load data
    logging.info('loading XNAT data, projects={}'.format(project_filter))

    # Build the uri to query with filters
    both_uri = BOTH_URI
    both_uri += '&project={}'.format(','.join(project_filter))

    # Query xnat
    both_json = utils.get_json(xnat, both_uri)
    df = pd.DataFrame(both_json['ResultSet']['Result'])

    # Rename columns
    df.rename(columns=BOTH_RENAME, inplace=True)

    # remove test sessions
    df = df[~df.SESSION.isin(['Pitt_Test_Upload_MR1', 'UIC_test_v2_MR1'])]

    # assessors
    dfa = df[[
        'PROJECT', 'SESSION', 'SUBJECT', 'DATE',
        'ASSR', 'QCSTATUS', 'PROCSTATUS', 'PROCTYPE']].copy()

    dfa.drop_duplicates(inplace=True)

    # Filter out excluded types
    dfa = dfa[~dfa['PROCTYPE'].isin(ASSR_EXCLUDE_LIST)]

    # Drop any rows with empty proctype
    dfa.dropna(subset=['PROCTYPE'], inplace=True)
    dfa = dfa[dfa.PROCTYPE != '']

    # Create shorthand status
    dfa['STATUS'] = dfa['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

    # scans
    dfs = df[[
        'PROJECT', 'SESSION', 'SUBJECT', 'SITE', 'DATE',
        'SCANID', 'SCANTYPE', 'QUALITY']].copy()

    dfs.drop_duplicates(inplace=True)

    # Filter out excluded types
    dfs = dfs[~dfs['SCANTYPE'].isin(SCAN_EXCLUDE_LIST)]

    # Drop any rows with empty proctype
    dfs.dropna(subset=['SCANTYPE'], inplace=True)
    dfs = dfs[dfs.SCANTYPE != '']

    # Create shorthand status
    dfs['STATUS'] = dfs['QUALITY'].map(SCAN_STATUS_MAP).fillna('U')

    return (dfs, dfa)


def load_assr_data(xnat, project_filter, proctype_filter):
    df = pd.DataFrame()

    # Load assr data
    logging.debug('loading assr data')

    try:
        # Build the uri to query with filters
        assr_uri = ASSR_URI
        assr_uri += '&project={}'.format(','.join(project_filter))
        if proctype_filter:
            assr_uri += '&proc:genprocdata/proctype={}'.format(
                ','.join(proctype_filter))

        assr_json = utils.get_json(xnat, assr_uri)

        df = pd.DataFrame(assr_json['ResultSet']['Result'])

        # Rename columns
        df.rename(columns=ASSR_RENAME, inplace=True)

        # Create shorthand status
        df['STATUS'] = df['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

        logging.debug('finishing assr data')
    except AttributeError as err:
        logging.warn('failed to load assessor data:' + str(err))
        df = pd.DataFrame(columns=ASSR_RENAME.keys())

    # Filter out excluded types
    df = df[~df['PROCTYPE'].isin(ASSR_EXCLUDE_LIST)]

    # return the assessor data
    logging.info('loaded {} assessors'.format(len(df)))
    return df


def load_scan_data(xnat, project_filter, scantype_filter):
    df = pd.DataFrame()

    # Load scan data
    logging.debug('loading scan data')

    try:
        # Build the uri to query with filters
        scan_uri = '{}&project={}'.format(
            SCAN_URI,
            ','.join(project_filter))

        scan_json = utils.get_json(xnat, scan_uri)
        df = pd.DataFrame(scan_json['ResultSet']['Result'])
        logging.debug('finishing scan data')

        # Rename columns
        df.rename(columns=SCAN_RENAME, inplace=True)

        # Create shorthand status
        df['STATUS'] = df['QUALITY'].map(SCAN_STATUS_MAP).fillna('U')
    except AttributeError as err:
        logging.warn('failed to load scan data:' + str(err))
        # Create an empty table with column names from SCAN_RENAME
        df = pd.DataFrame(columns=SCAN_RENAME.keys())

    # Filter out excluded types
    df = df[~df['SCANTYPE'].isin(SCAN_EXCLUDE_LIST)]

    # remove test sessions
    df = df[df.SESSION != 'Pitt_Test_Upload_MR1']

    # return the scan data
    logging.info('loaded {} scans'.format(len(df)))

    return df
