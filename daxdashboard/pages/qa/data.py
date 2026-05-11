"""QA Dashboard."""

import numpy as np
import pandas as pd

from ...log import logger
from ...extensions import cache
from ...utils import load_scan_data, load_assr_data, load_sgp_data, load_project_names


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
    'SESSION', 'SUBJECT', 'PROJECT', 'SCANID', 'ASSR',
    'SESSIONLINK', 'SUBJECTLINK', 'PDFLINK', 'OUTPUTLINK', 'LOGLINK', 'PBSLINK',
    'PDF', 'LOG', 'EDAT', 'NIFTI', 'JSON',
    'SITE', 'NOTE', 'DATE', 'TYPE', 'STATUS',
    'ARTTYPE', 'SCANTYPE', 'PROCTYPE', 'XSITYPE', 'SESSTYPE', 'MODALITY',
    'FRAMES', 'DURATION', 'TR', 'THICK', 'SENSE', 'MB', 'RESOURCES',
    'JOBDATE', 'TIMEUSED', 'MEMUSED', 'JOBNODE',
    'AGE', 'SEX', 'GROUP'
]


def run_refresh(projects):
    # force a requery
    df = get_data(projects)

    save_data(df)

    return df


def update_data(projects):
    # Load what we have now
    df = read_data()

    # Remove projects not selected
    df = df[df.PROJECT.isin(projects)]

    # Find new projects in selected
    new_projects = [x for x in projects if x not in df.PROJECT.unique()]

    if new_projects:
        # Save a file with new projects placeholders (hacky lock)
        for p in new_projects:
            _newdf = pd.DataFrame.from_records([{'PROJECT': p}])
            df = pd.concat([df, _newdf], ignore_index=True)

        save_data(df)

        # Load the new projects
        dfp = get_data(new_projects)

        # Merge our new data with old data
        df = read_data()
        df = df[~df.PROJECT.isin(new_projects)]
        df = pd.concat([df, dfp])

        # Save it for later
        save_data(df)

    return df


def load_data(projects=[], refresh=False, maxmins=60, hidetypes=True):

    if refresh:
        df = run_refresh(projects)
    elif set(projects) != set(read_data().PROJECT.unique()):
        logger.debug('updating data')

        # Different projects selected, update
        df = update_data(projects)
    else:
        df = read_data()

    if df.empty:
        return df

    if hidetypes:
        logger.debug('applying autofilter to hide unused types')
        scantypes = ['T1']
        assrtypes = ['FS7_v1']
        logger.debug(f'done filtering by types:{len(df)}')

    # Filter projects
    df = df[df['PROJECT'].isin(projects)]

    # Must have type
    df = df.dropna(subset=['TYPE'])
    df = df[df.TYPE != '']

    return df


def read_data():
    df = cache.get('qadata')

    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=QA_COLS)
 
    return df


def save_data(df):
    # save to cache
    cache.set('qadata', df)


def get_data(projects):
    df = pd.DataFrame(columns=QA_COLS)
    scan_df = df.copy()
    assr_df = df.copy()
    subj_df = df.copy()

    if not projects:
        # No projects selected so we don't query
        return df

    try:
        # Load data
        logger.debug(f'load data:{projects}')

        logger.debug(f'load scan data:{projects}')
        scan_df = _load_scan_data(projects)
        
        logger.debug(f'load assr data:{projects}')
        assr_df = _load_assr_data(projects)

        logger.debug(f'load sgp data:{projects}')
        subj_df = _load_sgp_data(projects)

        logger.debug(f'all loaded')
    except Exception as err:
        logger.error(f'load failed:{err}')
        return pd.DataFrame(columns=QA_COLS)

    logger.debug(f'merging data:{projects}')

    # Make a common column for type
    assr_df['TYPE'] = assr_df['PROCTYPE']
    scan_df['TYPE'] = scan_df['SCANTYPE']
    subj_df['TYPE'] = subj_df['PROCTYPE']
    assr_df['ARTTYPE'] = 'assessor'
    scan_df['ARTTYPE'] = 'scan'
    subj_df['ARTTYPE'] = 'sgp'

    for x in ['SESSION', 'SITE', 'NOTE', 'SESSTYPE', 'MODALITY']:
        subj_df[x] = 'SGP'

    # Concatenate the common cols to a new dataframe
    for c in QA_COLS:
        if c not in assr_df.columns:
            assr_df[c] = ''

        if c not in scan_df.columns:
            scan_df[c] = ''

        if c not in subj_df.columns:
            subj_df[c] = ''

    try:
        df = pd.concat([assr_df[QA_COLS], scan_df[QA_COLS]], sort=False)
    except Exception as err:
        print(f'concat failed:{err}')

    df = pd.concat([df[QA_COLS], subj_df[QA_COLS]], sort=False)


    # Convert duration from string of total seconds to formatted HH:MM:SS
    if 'DURATION' in df:
        df['DURATION'] = df['DURATION'].fillna(np.nan).replace(
            '', np.nan).replace('None', np.nan)
        df['DURATION'] = pd.to_datetime(
            df.DURATION.astype(float),
            unit='s',
            errors='coerce').dt.strftime("%-M:%S")

    df.loc[df.RESOURCES.str.contains('EDAT') == False, 'EDAT'] = ''
    df.loc[df.RESOURCES.str.contains('JSON') == False, 'JSON'] = ''
    df.loc[df.RESOURCES.str.contains('NIFTI') == False, 'NIFTI'] = ''

    df['GROUP'] = 'UNKNOWN'
    df['AGE'] = ''
    df['SEX'] = ''

    return df


def _filter(scan_df, assr_df, scantypes, assrtypes):

    # Apply filters
    if scantypes is not None:
        logger.debug(f'filtering scan by types:{len(scan_df)}')
        scan_df = scan_df[scan_df['SCANTYPE'].isin(scantypes)]

    if assrtypes is not None:
        logger.debug(f'filtering assr by types:{len(assr_df)}')
        assr_df = assr_df[assr_df['PROCTYPE'].isin(assrtypes)]

    logger.debug(f'done filtering by types:{len(scan_df)}:{len(assr_df)}')

    return scan_df, assr_df


def _load_assr_data(project_filter):
    dfa = load_assr_data(project_filter)

    # Drop any rows with empty proctype
    dfa.dropna(subset=['PROCTYPE'], inplace=True)
    dfa = dfa[dfa.PROCTYPE != '']

    # Create shorthand status
    dfa['STATUS'] = dfa['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

    # Handle uploading jobs
    dfa.loc[dfa.PROCSTATUS == 'UPLOADING', 'STATUS'] = 'R'

    # Handle failed jobs
    dfa.loc[dfa.PROCSTATUS == 'JOB_FAILED', 'STATUS'] = 'X'

    # Handle running jobs
    dfa.loc[dfa.PROCSTATUS == 'JOB_RUNNING', 'STATUS'] = 'R'

    # Handle NEED INPUTS
    dfa.loc[dfa.PROCSTATUS == 'NEED_INPUTS', 'STATUS'] = 'N'

    return dfa


def _load_sgp_data(project_filter):
    df = load_sgp_data(project_filter)

    # Get subset of columns
    df = df[[
        'PROJECT', 'SUBJECT', 'DATE', 'ASSR', 'QCSTATUS', 'XSITYPE',
        'PROCSTATUS', 'PROCTYPE', 'JOBDATE', 'TIMEUSED', 'MEMUSED', 'JOBNODE']]

    df.drop_duplicates(inplace=True)

    # Drop any rows with empty proctype
    df.dropna(subset=['PROCTYPE'], inplace=True)
    df = df[df.PROCTYPE != '']

    # Create shorthand status
    df['STATUS'] = df['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

    # Handle failed jobs
    df.loc[df.PROCSTATUS == 'JOB_FAILED', 'STATUS'] = 'X'

    # Handle running jobs
    df.loc[df.PROCSTATUS == 'JOB_RUNNING', 'STATUS'] = 'R'

    # Handle NEED INPUTS
    df.loc[df.PROCSTATUS == 'NEED_INPUTS', 'STATUS'] = 'N'

    return df



def _load_scan_data(project_filter):
     #  Load data
     dfs = load_scan_data(project_filter)

     dfs = dfs[[
         'PROJECT', 'SESSION', 'SUBJECT', 'NOTE', 'DATE', 'SITE', 'SCANID',
         'SCANTYPE', 'QUALITY', 'XSITYPE', 'SESSTYPE', 'MODALITY',
         'FRAMES', 'DURATION', 'TR', 'THICK', 'SENSE', 'MB', 'RESOURCES',
         'full_path']].copy()

     dfs.drop_duplicates(inplace=True)

     # Drop any rows with empty type
     dfs.dropna(subset=['SCANTYPE'], inplace=True)

     # Create shorthand status
     dfs['STATUS'] = dfs['QUALITY'].map(SCAN_STATUS_MAP).fillna('U')

     return dfs


def filter_data(df, projects, proctypes, scantypes, starttime, endtime, sesstypes):

    # Filter by project
    if projects:
        logger.debug('filtering by project:')
        logger.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by proc type
    if proctypes:
        logger.debug('filtering by proc types:')
        logger.debug(proctypes)
        df = df[(df['PROCTYPE'].isin(proctypes)) | (df['ARTTYPE'] == 'scan')]

    # Filter by scan type
    if scantypes:
        logger.debug('filtering by scan types:')
        logger.debug(scantypes)
        df = df[(df['SCANTYPE'].isin(scantypes)) | (df['ARTTYPE'] == 'assessor') | (df['ARTTYPE'] == 'sgp')]

    if starttime:
        logger.debug(f'filtering by start time:{starttime}')
        df = df[pd.to_datetime(df.DATE) >= starttime]

    if endtime:
        df = df[pd.to_datetime(df.DATE) <= endtime]

    # Filter by sesstype
    if sesstypes:
        df = df[df['SESSTYPE'].isin(sesstypes)]

    return df


def project_names():
    return load_project_names()
