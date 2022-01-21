import logging
import os
from datetime import datetime

import dax

import utils

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd


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


# Data sources are  XNAT, issues.csv and completed.log files
# as maintained by ccmutils audits
ISSUESFILE = os.path.join(os.path.expanduser("~"), 'issues.csv')
COMPLETEDFILE = os.path.join(os.path.expanduser("~"), 'completed.log')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')


# This is where we save our cache of the data
def get_filename():
    return '{}.pkl'.format('activitydata')


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
    print(startdate)

    # TODO: change this to try/catch
    if os.path.exists(ISSUESFILE):
        # Load issues data
        logging.info('loading issues from file')
        dfi = load_issues_file()

    if os.path.exists(COMPLETEDFILE):
        # Load completed data
        logging.info('loading completed log from file')
        dfc = load_completed_file()

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
        'JOB_RUNNING': 'TBD'}).fillna('UNKNOWN')

    df['SOURCE'] = 'dax'

    df['CATEGORY'] = df['PROCTYPE']

    df['DESCRIPTION'] = 'JOB' + ':' + df['LABEL']

    df['DATETIME'] = df['JOBDATE']

    return df


def load_issues_file():
    # type,project,subject,session,date,event,field,description

    df = pd.read_csv(ISSUESFILE)

    LABELFIELDS = ['project', 'subject', 'session', 'event', 'field']
    df['LABEL'] = df[LABELFIELDS].stack().groupby(level=0).agg(','.join)

    df['PROJECT'] = df['project']

    df['STATUS'] = 'FAIL'

    df['SOURCE'] = 'ccmutils'

    df['CATEGORY'] = df['type']

    df['DESCRIPTION'] = 'ISSUE' + ':' + df['CATEGORY'] + ':' + df['LABEL'] + ':' + df['description']

    df['DATETIME'] = df['datetime']

    return df


def load_completed_file():
    # datetime, result, type, subject, session, scan, event, field, project

    # read each line and convert to a dictionary and append to list
    # then convert list to a dataframe
    data = []
    with open(COMPLETEDFILE, 'r') as f:
        for line in f:
            pairs = line.strip().split(',')
            row = dict(keyval.split("=") for keyval in pairs)
            if 'session' not in row:
                row['session'] = ''

            data.append(row)

    # Make a dataframe from all rows
    df = pd.DataFrame(data)

    LABELFIELDS = ['project', 'subject', 'session', 'event', 'field']
    df['LABEL'] = df[LABELFIELDS].stack().groupby(level=0).agg(','.join)

    df['PROJECT'] = df['project']

    df['STATUS'] = 'COMPLETE'

    df['SOURCE'] = 'ccmutils'

    df['CATEGORY'] = df['type']

    df['DESCRIPTION'] = df['CATEGORY'] + ':' + df['LABEL']

    df['DATETIME'] = df['datetime']

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
