import logging
import os
from datetime import datetime

import pandas as pd
import redcap
import dax

import utils
from stats.params import STATS_RENAME, STATIC_COLUMNS, VAR_LIST
import shared


# Data sources are:
# REDCap (using keys in shared.keyfile)
#
# Note this app does not access ACCRE or SLURM. The ony local file access
# is to write the cached data in a pickle file named statsdata.pkl

# Now only loads the selected redcaps rather than loading them first and then
# filtering


SESS_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
xsiType,\
project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label'


SESS_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'xsiType': 'XSITYPE',
    'session_type': 'SESSTYPE'}


def static_columns():
    return STATIC_COLUMNS


def get_vars():
    return get_variables()


def get_variables():
    return VAR_LIST


def get_filename():
    return 'DATA/statsdata.pkl'


def load_data(projects, proctypes, refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        run_refresh(filename, projects, proctypes)

    logging.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def get_xnat_data(xnat, project_filter):
    # TODO: recode this to get_session_data and move somewhere shared

    #  Load data
    logging.info('loading XNAT data, projects={}'.format(project_filter))

    # Build the uri to query with filters
    xnat_uri = SESS_URI
    xnat_uri += '&project={}'.format(','.join(project_filter))

    # Query xnat
    both_json = utils.get_json(xnat, xnat_uri)
    df = pd.DataFrame(both_json['ResultSet']['Result'])

    # Rename columns
    df.rename(columns=SESS_RENAME, inplace=True)

    # sessions
    dfs = df[[
        'PROJECT', 'SESSION', 'SUBJECT', 'DATE', 'SITE', 'XSITYPE', 'SESSTYPE']].copy()

    dfs.drop_duplicates(inplace=True)

    return dfs


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def get_data(projects, proctypes):
    # Load that data
    df = load_stats_data(projects, proctypes)
    if df.empty:
        return df

    # Merge in XNAT data to get SITE, SESSTYPE
    projects = list(df.PROJECT.unique())
    if projects:
        # Merge in xnat information to get SITE and SESSTYPE
        logging.debug('merging in xnat data for projects')
        with dax.XnatUtils.get_interface() as xnat:
            dfp = get_xnat_data(xnat, projects)

        # Merge by session to get SITE and SESSTYPE
        _cols = ['SESSION', 'SUBJECT', 'SESSTYPE', 'SITE']
        df = df.merge(dfp[_cols], on='SESSION', how='left')

    logging.info('loading demographic data')
    _df = load_demographic_data()
    df = pd.merge(
        df,
        _df,
        how='left',
        left_on='SUBJECT',
        right_index=True)

    logging.info('loading madrs data')
    _df = load_madrs_data()
    df = pd.merge(
        df,
        _df,
        how='outer',
        left_on=['SUBJECT', 'SESSTYPE'],
        right_on=['SUBJECT', 'SESSTYPE'])

    # Fill with blanks so we don't lose to nans
    df['AGE'] = df['AGE'].fillna('')
    df['SEX'] = df['SEX'].fillna('')
    df['DEPRESS'] = df['DEPRESS'].fillna('')
    df['SESSTYPE'] = df['SESSTYPE'].fillna('UNKNOWN')

    return df


def run_refresh(filename, projects, proctypes):
    df = get_data(projects, proctypes)

    # Apply the var list filter here?
    #df = df[df.
    #var_list = [x for x in VAR_LIST if x in df and not pd.isnull(df[x]).all()]

    save_data(df, filename)

    return df


def parse_redcap_name(name):
    (proj, tmp) = name.split('-', 1)
    (proc, res) = tmp.rsplit('-', 1)
    return (proj, proc, res)


def load_redcap_stats(api_url, api_key):
    # Load the redcap project, lazy for speed
    _rc = redcap.Project(api_url, api_key)

    # Load the data, specify index since we loaded lazy
    _df = _rc.export_records(format_type='df', df_kwargs={'index_col': 'record_id'})

    if 'wml_volume' in _df:
        # rename wml for NIC
        _df['lst_stats_wml_volume'] = _df['wml_volume']

    return _df


def load_stats_data(projects, proctypes):
    #df = pd.DataFrame(columns=static_columns())
    df = pd.DataFrame()

    logging.debug('loading stats data')

    with open(shared.KEYFILE) as f:
        for line in f:
            # Parse the line to get redcap name
            try:
                (i, k, n) = line.strip().split(',')
            except:
                continue

            # Skip if not a stats redcap
            if i != 'stats':
                continue

            # Parse the name to get project and proc type
            try:
                (proj, proc, res) = parse_redcap_name(n)
            except ValueError:
                continue

            if (not projects or proj not in projects):
                # Filter based on selected projects, nothing yields nothing
                continue

            if (not proctypes or proc not in proctypes):
                # Filter based on selected projects, nothing yields nothing
                continue

            logging.info(f'loading redcap:{n}')
            try:
                cur_df = load_redcap_stats(shared.API_URL, k)
                df = pd.concat([df, cur_df], ignore_index=True, sort=False)
            except Exception as err:
                logging.error(f'error exporting redcap:{n}:{err}')
                import traceback
                traceback.print_exc()
                continue

    # Rename columns
    df.rename(columns=STATS_RENAME, inplace=True)

    # Filter out columns we don't want by keeping intersection
    _static = static_columns()
    _var = get_vars()
    _keep = df.columns
    _keep = [x for x in _keep if (x in _var or x in _static)]
    df = df[_keep]

    # return the stats data
    logging.info('loaded {} stats'.format(len(df)))
    return df


def filter_data(df, projects, proctypes, timeframe, sesstypes):
    if df.empty:
        # It's already empty, just send it back
        return df

    # Filter by project
    if projects:
        logging.debug('filtering by project:')
        logging.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by proctype
    if proctypes:
        logging.debug('filtering by proctypes:')
        logging.debug(proctypes)
        df = df[df['TYPE'].isin(proctypes)]
    else:
        logging.debug('no proctypes')
        df = df[df['TYPE'].isin([])]

    # Filter by timeframe
    if timeframe in ['1day', '7day', '30day', '365day']:
        logging.debug('filtering by ' + timeframe)
        then_datetime = datetime.now() - pd.to_timedelta(timeframe)
        df = df[pd.to_datetime(df.DATE) > then_datetime]
    else:
        # ALL
        logging.debug('not filtering by time')
        pass

     # Filter by sesstype
    if sesstypes:
        logging.debug(f'filtering by sesstypes:{sesstypes}')
        df = df[df['SESSTYPE'].isin(sesstypes)]
    else:
        logging.debug('no sesstypes')

    return df


def load_madrs_data():
    logging.info('loading madrs data')

    data = pd.DataFrame()

    i = utils.get_projectid('DepMIND2 primary', shared.KEYFILE)
    k = utils.get_projectkey(i, shared.KEYFILE)
    if k:
        _cols = ['ma_tot']
        _fields = ['record_id', 'ma_tot']
        _map = {
            'week_0baseline_arm_1': 'Baseline',
            'week_6_arm_1': 'Week6',
            'week_12_arm_1': 'Week12',
            'week_3_arm_1': 'Week3', 
            'week_9_arm_1': 'Week9',
        }
        _events = _map.keys()

        # Connect to the redcap project
        logging.info('connecting to redcap')
        _proj = redcap.Project(shared.API_URL, k)

        # Load secondary ID
        def_field = _proj.def_field
        sec_field = _proj.export_project_info()['secondary_unique_field']
        rec = _proj.export_records(fields=[def_field, sec_field], format_type='df')
        rec.dropna(subset=[sec_field], inplace=True)
        rec[sec_field] = rec[sec_field].astype(int).astype(str)
        rec = rec.reset_index()
        rec = rec.drop('redcap_event_name', axis=1)

        # Load madrs data
        data = _proj.export_records(
            raw_or_label='raw', format_type='df', fields=_fields, events=_events)

        # Merge in subject_number (probably could do this with a column map)
        data = data.reset_index()
        data = pd.merge(
            data,
            rec,
            how='left',
            left_on='record_id',
            right_on='record_id',
            sort=True)

        # Rename for consistency with other studies
        data.rename(columns={'subject_number': 'SUBJECT'}, inplace=True)

        # Map redcap event to xnat session type
        data['SESSTYPE'] = data['redcap_event_name'].map(_map)
        data = data.drop('redcap_event_name', axis=1)

        data = data.dropna()

        # Force int format
        data['ma_tot'] = data['ma_tot'].astype(int)

        data = data.sort_values('SUBJECT')

    return data


def load_demographic_data():
    df = pd.DataFrame()

    _fields = [
        'record_id',
        'subject_number',
        'age',
        'sex_xcount']
    _events = ['screening_arm_1']
    _rename = {
        'subject_number': 'SUBJECT',
        'age': 'AGE',
        'sex_xcount': 'SEX'}

    # Load the records from redcap
    k = utils.get_projectkeybyname("DepMIND2 primary", shared.KEYFILE)
    _proj = redcap.Project(shared.API_URL, k)
    df = _proj.export_records(
        raw_or_label='label',
        format_type='df',
        fields=_fields,
        events=_events)

    # Transform for dashboard data
    df = df.rename(columns=_rename)
    df = df.dropna(subset=['SUBJECT'])
    df['SUBJECT'] = df['SUBJECT'].astype(int).astype(str)
    df = df.set_index('SUBJECT', verify_integrity=True)

    # All DM2 are depressed
    df['DEPRESS'] = '1'

    return df


def load_options(projects, proctypes):
    # Only filter proctypes if projects are selected
    # Only filter projects by proctypes selected
    proj_options = []
    proc_options = []

    logging.info('loading stats options')

    with open(shared.KEYFILE) as f:
        for line in f:
            # Parse the line to get redcap name
            try:
                (i, k, n) = line.strip().split(',')
            except:
                continue

            # Skip if not a stats redcap
            if i != 'stats':
                continue

            # Parse the name to get project and proc type
            try:
                (proj, proc, res) = parse_redcap_name(n)
            except ValueError:
                continue

            # Include in projects list if no proctypes are 
            # selected or if it is one of the selected proc types
            if not proctypes or len(proctypes) == 0 or proc in proctypes:
                proj_options.append(proj)

            # Include in proc types list if no projects are selected or if it is
            # one of the selected projects
            if not projects or len(projects) == 0 or proj in projects:
                proc_options.append(proc)

    # Return projects, processing types
    return sorted(list(set(proj_options))), sorted(list(set(proc_options)))
