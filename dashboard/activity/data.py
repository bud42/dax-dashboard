import logging
import os
from datetime import datetime

import pandas as pd
import redcap
import dax

import utils
import shared


ASSR_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
subject_label,\
session_label,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
proc:genprocdata/label,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/jobstartdate,\
proc:genprocdata/validation/status,\
proc:genprocdata/validation/date,\
proc:genprocdata/validation/validated_by'

ASSR_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'xnat:imagesessiondata/date': 'DATE',
    'proc:genprocdata/label': 'ASSR',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/jobstartdate': 'JOBDATE',
    'proc:genprocdata/validation/status': 'QCSTATUS',
    'proc:genprocdata/validation/date': 'QCDATE',
    'proc:genprocdata/validation/validated_by': 'QCBY'}


# This is where we save our cache of the data
def get_filename():
    #return 'DATA/activitydata.pkl'
    datadir = 'DATA'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)

    filename = f'{datadir}/activitydata.pkl'
    return filename


def load_activity_redcap():
    LABELFIELDS = ['PROJECT', 'SUBJECT', 'SESSION', 'SCAN', 'EVENT', 'FIELD']

    df = pd.DataFrame(columns=[
        'ID', 'LABEL', 'PROJECT', 'SUBJECT', 'SESSION', 'EVENT', 'FIELD',
        'CATEGORY', 'STATUS', 'SOURCE', 'DESCRIPTION', 'DATETIME'
    ])

    try:
        keyfile = shared.KEYFILE
        logging.info('connecting to redcap')
        i = utils.get_projectid("main", keyfile)
        k = utils.get_projectkey(i, keyfile)
        mainrc = redcap.Project(shared.API_URL, k)

        logging.info('exporting activity records')
        df = mainrc.export_records(
            forms=['main', 'activity'],
            format_type='df')
        df = df[df['redcap_repeat_instrument'] == 'activity']

        logging.debug('transforming records')

        df['PROJECT'] = df.index

        df.rename(inplace=True, columns={
            'redcap_repeat_instance': 'ID',
            'activity_description': 'DESCRIPTION',
            'activity_datetime': 'DATETIME',
            'activity_event': 'EVENT',
            'activity_field': 'FIELD',
            'activity_result': 'RESULT',
            'activity_scan': 'SCAN',
            'activity_subject': 'SUBJECT',
            'activity_session': 'SESSION',
            'activity_type': 'CATEGORY',
        })
        df['SOURCE'] = 'ccmutils'
        df['STATUS'] = 'COMPLETE'
        df['LABEL'] = df[LABELFIELDS].apply(
            lambda x: ','.join(x[x.notnull()]), axis=1)
        df['DESCRIPTION'] = df['CATEGORY'] + ':' + df['LABEL']
    except Exception as err:
        logging.error(f'failed to load activity:{err}')

    return df


def get_data(xnat, proj_filter):
    df = pd.DataFrame()
    dfc = pd.DataFrame()
    dfi = pd.DataFrame()
    dfq = pd.DataFrame()
    dfj = pd.DataFrame()

    # This week: monday of this week
    # import datetime
    # today = datetime.date.today()
    # startdate = today - datetime.timedelta(days=today.weekday())

    # This month: first date of current month
    # startdate = datetime.datetime.today().replace(day=1)

    # Past month
    from dateutil.relativedelta import relativedelta
    startdate = datetime.today() - relativedelta(months=1)
    startdate = startdate.strftime('%Y-%m-%d')

    dfc = load_activity_redcap()

    # Load qa data
    logging.info('loading recent data from xnat')
    dfx = load_xnat_data(xnat, proj_filter)

    dfq = load_recent_qa(dfx, startdate=startdate)
    logging.info('loaded {} qa records'.format(len(dfq)))

    dfj = load_recent_jobs(dfx, startdate=startdate)
    logging.info('loaded {} job records'.format(len(dfj)))

    # Concatentate all the dataframes into one
    df = pd.concat([dfi, dfc, dfq, dfj], ignore_index=True)
    df.sort_values(by=['DATETIME'], inplace=True, ascending=False)
    df.reset_index(inplace=True)
    df['ID'] = df.index

    return df


def load_xnat_data(xnat, project_filter):
    df = pd.DataFrame()

    #  Load data
    logging.info('loading XNAT QA data, projects={}'.format(project_filter))

    # Build the uri to query with filters
    assr_uri = ASSR_URI
    assr_uri += '&project={}'.format(','.join(project_filter))

    # Query xnat
    qa_json = utils.get_json(xnat, assr_uri)
    df = pd.DataFrame(qa_json['ResultSet']['Result'])

    # Rename columns
    df.rename(columns=ASSR_RENAME, inplace=True)

    return df


def load_recent_qa(df, startdate):
    df = df.copy()

    df['LABEL'] = df['ASSR']
    df['CATEGORY'] = df['PROCTYPE']

    # Filter by qc date
    df = df[df['QCDATE'] >= startdate]

    df['STATUS'] = df['QCSTATUS'].map({
        'Failed': 'FAIL',
        'Passed': 'PASS'}).fillna('UNKNOWN')

    df['SOURCE'] = 'qa'

    df['CATEGORY'] = df['PROCTYPE']

    df['DESCRIPTION'] = 'QA' + ':' + df['LABEL']

    df['DATETIME'] = df['QCDATE']

    return df


def load_recent_jobs(df, startdate):
    df = df.copy()
    df['LABEL'] = df['ASSR']
    df['CATEGORY'] = df['PROCTYPE']

    # Filter by jobstartdate date, include anything with job running
    df = df[(df['JOBDATE'] >= startdate) | (df['PROCSTATUS'] == 'JOB_RUNNING')]

    df['STATUS'] = df['PROCSTATUS'].map({
        'COMPLETE': 'COMPLETE',
        'JOB_FAILED': 'FAIL',
        'JOB_RUNNING': 'NPUT'}).fillna('UNKNOWN')

    df['SOURCE'] = 'dax'

    df['CATEGORY'] = df['PROCTYPE']

    df['DESCRIPTION'] = 'JOB' + ':' + df['LABEL']

    df['DATETIME'] = df['JOBDATE']

    return df


def run_refresh(filename):
    proj_filter = []

    with dax.XnatUtils.get_interface() as xnat:
        proj_filter = utils.get_user_favorites(xnat)
        df = get_data(xnat, proj_filter)

    utils.save_data(df, filename)

    return df


def load_field_options(fieldname):
    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh(filename)

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
    return utils.read_data(filename)


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
