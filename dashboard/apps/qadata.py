import logging
import os

import json
import pandas as pd

from dax import XnatUtils


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


ASSR_TYPE_URI = '/REST/experiments?xsiType=proc:genprocdata\
&columns=\
ID,\
label,\
project,\
proc:genprocdata/proctype'

SCAN_TYPE_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
ID,\
label,\
project,\
xnat:imagescandata/id,\
xnat:imagescandata/type'

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
    'questionable': 'Q',
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

QA_COLS = ['SESSION', 'PROJECT', 'DATE', 'TYPE', 'STATUS']

# TODO: move this to an environ var
username = 'boydb1'

# TODO: move this inside the functions, declare at top and pass down,
# use a with statement so it hopefully get's closed
xnat = XnatUtils.get_interface()


def is_baseline_session(session):
    # TODO: re-implement this by getting list of sessions for each subject,
    # sorted by date and set the first session to basline
    return (
        session.endswith('a') or
        session.endswith('_bl') or
        session.endswith('_MR1'))


def get_user_projects():
    logging.debug('loading user projects')

    uri = '/xapi/users/{}/groups'.format(username)

    # get from xnat and convert to list
    data = json.loads(xnat._exec(uri, 'GET'))

    # format of group name is PROJECT_ROLE,
    # so we split on the underscore
    data = sorted([x.rsplit('_', 1)[0] for x in data])

    return data


def get_ptypes(project_list):
    # Load assr data
    logging.debug('loading ptypes')
    try:
        # Build the uri to query with filters
        assr_uri = '{}&project={}'.format(
            ASSR_TYPE_URI,
            ','.join(project_list))

        assr_json = json.loads(xnat._exec(assr_uri, 'GET'))
        df = pd.DataFrame(assr_json['ResultSet']['Result'])

        df.rename(columns=ASSR_RENAME, inplace=True)

        logging.debug('finishing assr data')
    except AttributeError as err:
        logging.warn('failed to load assessor data:' + str(err))
        return []

    # return the assessor data
    logging.info('loaded {} assessors'.format(len(df)))
    return df.PROCTYPE.unique()


def get_stypes(project_list):
    # Load scan data
    logging.debug('loading stypes')
    try:
        # Build the uri to query with filters
        scan_uri = '{}&project={}'.format(SCAN_TYPE_URI, ','.join(project_list))

        # Query xnat
        scan_json = json.loads(xnat._exec(scan_uri, 'GET'))

        # Build dataframe from result
        df = pd.DataFrame(scan_json['ResultSet']['Result'])
        logging.debug('finishing scan data')

        # Rename columns
        df.rename(columns=SCAN_RENAME, inplace=True)
    except AttributeError as err:
        logging.warn('failed to load scan data:' + str(err))
        # Create an empty table with column names from SCAN_RENAME
        df = pd.DataFrame(columns=SCAN_RENAME.keys())

    # return the scan data types
    logging.info('loaded {} scans'.format(len(df)))
    return df.SCANTYPE.unique()


def get_filename():
    return '{}.pkl'.format(username)


def load_data():
    filename = get_filename()

    if os.path.exists(filename):
        df = pd.read_pickle(filename)
    else:
        # no filters
        df = get_data()

        # save to cache
        save_data(df)

    return df


def save_data(df):
    filename = get_filename()

    # save to cache
    df.to_pickle(filename)
    return df


def set_data(proj_filter=[], stype_filter=[], ptype_filter=[]):

    if not proj_filter:
        # Select first project
        proj_list = get_user_projects()
        proj_filter = proj_list[0:1]

    if not stype_filter:
        # Load scan types
        stype_list = get_stypes(xnat, proj_filter)

        # Pick a scan type
        stype_filter = stype_list[0:1]

    if not ptype_filter:
        # Load proc types
        ptype_list = get_ptypes(xnat, proj_filter)

        # Pick a scan type
        ptype_filter = ptype_list[0:1]

    df = get_data(proj_filter, stype_filter, ptype_filter)

    # save to cache
    save_data(df)

    return df


def get_data(proj_filter, stype_filter, ptype_filter):

    # Load that data
    assr_df = load_assr_data(proj_filter, ptype_filter)
    scan_df = load_scan_data(proj_filter, stype_filter)

    # Make a common column for type
    assr_df['TYPE'] = assr_df['PROCTYPE']
    scan_df['TYPE'] = scan_df['SCANTYPE']

    # Concatenate the common cols to a new dataframe
    df = pd.concat([assr_df[QA_COLS], scan_df[QA_COLS]], sort=False)

    # set a column for session visit type, i.e. baseline if session name
    # ends with a or MR1 or something else, otherwise it's a followup
    df['ISBASELINE'] = df['SESSION'].apply(is_baseline_session)

    return df


def refresh_data():
    df = load_data()

    # Hacky way to get reverse-engineer the filters
    proj_filter = df.PROJECT.unique()

    stype_list = get_stypes(proj_filter)

    type_set = set(df.TYPE.unique())

    stype_filter = list(set(stype_list).intersection(type_set))

    ptype_filter = list(type_set - set(stype_filter))

    # Get the data again with same filters
    df = get_data(proj_filter, stype_filter, ptype_filter)

    # save to cache
    save_data(df)

    return df


def load_assr_data(project_filter, proctype_filter):
    df = pd.DataFrame()

    # Load assr data
    logging.debug('loading assr data')
    try:
        # Build the uri to query with filters
        assr_uri = ASSR_URI
        assr_uri += '&project={}'.format(','.join(project_filter))
        assr_uri += '&proc:genprocdata/proctype={}'.format(
            ','.join(proctype_filter))

        assr_json = get_json(assr_uri)

        df = pd.DataFrame(assr_json['ResultSet']['Result'])

        # Rename columns
        df.rename(columns=ASSR_RENAME, inplace=True)

        # Create shorthand status
        df['STATUS'] = df['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

        logging.debug('finishing assr data')
    except AttributeError as err:
        logging.warn('failed to load assessor data:' + str(err))
        df = pd.DataFrame(columns=ASSR_RENAME.keys())

    # return the assessor data
    logging.info('loaded {} assessors'.format(len(df)))
    return df


def load_scan_data(project_filter, scantype_filter):
    df = pd.DataFrame()

    # Load scan data
    logging.debug('loading scan data')
    try:
        # Build the uri to query with filters
        scan_uri = '{}&project={}'.format(
            SCAN_URI,
            ','.join(project_filter))

        # this doesn't work, but maybe we don't need to filter scans from
        # the xnat query
        # if type_list:
        #     type_filter = 'xnat:imagescandata/type={}'.format(
        # ','.join(self.scantype_filter))
        #     scan_uri += '&' + type_filter

        scan_json = get_json(scan_uri)
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

    # TODO: move this filtering to the uri if we can, not currently working
    # Filter by scan type
    df = df[df['SCANTYPE'].isin(scantype_filter)]

    # return the scan data
    logging.info('loaded {} scans'.format(len(df)))

    return df


def get_json(uri):
    _data = json.loads(xnat._exec(uri, 'GET'))
    return _data
