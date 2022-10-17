import logging
import os
from datetime import datetime

import pandas as pd
import yaml
import redcap
import dax

import utils
from stats.params import REDCAP_FILE, STATS_RENAME, STATIC_COLUMNS, VAR_LIST
import shared

# Data sources are:
#
# REDCap (using keys in file as specified in REDCAP_FILE) this is the
# source of the stats data.
#
# Note this app does not access ACCRE or SLURM. The ony local file access
# is to write the cached data in a pickle file. This file is named stats.pkl

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
    return '{}.pkl'.format('stats')


def load_data(projects, proctypes, refresh=False):
    filename = get_filename()

    print(projects)
    print(proctypes)

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

    #print(xnat_uri)

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
            print(dfp.columns)

        # Merge by session to get SITE and SESSTYPE
        _cols = ['SESSION', 'SUBJECT', 'SESSTYPE', 'SITE']
        df = df.merge(dfp[_cols], on='SESSION', how='left')

    # if DEMOG_KEYS:
    #     print('loading demographic data')
    #     _df = load_demographic_data(REDCAP_URL, DEMOG_KEYS)
    #     df = pd.merge(
    #         df,
    #         _df,
    #         how='left',
    #         left_on='SUBJECT',
    #         right_index=True)

    #     print('loading madrs')
    #     _df = load_madrs_data()
    #     print(_df)
    #     df = pd.merge(
    #         df,
    #         _df,
    #         how='outer',
    #         left_on=['SUBJECT', 'SESSTYPE'],
    #         right_on=['SUBJECT', 'SESSTYPE'])

    #     # Fill with blanks so we don't lose to nans
    #     df['AGE'] = df['AGE'].fillna('')
    #     df['SEX'] = df['SEX'].fillna('')
    #     df['DEPRESS'] = df['DEPRESS'].fillna('')
    # else:
    #     # Fill with blanks so we don't lose to nans
    #     df['AGE'] = ''
    #     df['SEX'] = ''
    #     df['DEPRESS'] = ''

    # TODO: load MADRS
    #if DEMOG_KEYS:
    #    _df = load_other_data()
    #    df = pd.merge(df, _df, how='left', left_on='', right_on='')

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

    #print(_df.columns)

    return _df


def load_stats_data(projects, proctypes):
    my_redcaps = []
    df = pd.DataFrame()

    # Load assr data
    logging.debug('loading stats data')

    try:
        # Read inputs yaml as dictionary
        with open(REDCAP_FILE, 'rt') as file:
            redcap_data = yaml.load(file, yaml.SafeLoader)
    except EnvironmentError:
        logging.info('REDCap settings file not found, not loading stats')
        df = pd.DataFrame(columns=static_columns())
        return df

    api_url = redcap_data['api_url']

    with dax.XnatUtils.get_interface() as xnat:
        my_projects = utils.get_user_favorites(xnat)

    for r in redcap_data['projects']:
        name = r['name']

        try:
            (proj, proc, res) = parse_redcap_name(name)
        except ValueError:
            continue

        if (proj not in my_projects):
            # Filter the list of redcaps based on our project access
            continue

        if (not projects or proj not in projects):
            # Filter based on selected projects, nothing yields nothing
            continue

        if (not proctypes or proc not in proctypes):
            # Filter based on selected projects, nothing yields nothing
            continue

        my_redcaps.append(r)


    # Load data from each redcap
    icount = len(my_redcaps)
    for i, r in enumerate(my_redcaps):
        name = r['name']
        api_key = r['key']
        (proj, proc, res) = parse_redcap_name(name)
        logging.info('{}/{} loading redcap:{}'.format(i+1, icount, name))
        try:
            cur_df = load_redcap_stats(api_url, api_key)
            df = pd.concat([df, cur_df], ignore_index=True, sort=False)
        except Exception as err:
            logging.error('error exporting redcap:{}:{}'.format(name, err))
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
    #print('_keep', _keep)
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

    data = pd.DataFrame()

    i = utils.get_projectid("DepMIND2 primary", shared.KEYFILE)
    k = utils.get_projectkey(i, shared.KEYFILE)
    if k:
        print('loading DepMIND2 MADRS data')
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


def load_demographic_data(redcapurl, redcapkeys):
    df = pd.DataFrame()

    if 'DepMIND2' in redcapkeys:
        print('loading DepMIND2 demographic data')
        _key = redcapkeys['DepMIND2']
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
        _proj = redcap.Project(redcapurl, _key)
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
    proj_options = []
    proc_options = []

    # Only filter proctypes if projects are selected
    # Only filter projects by proctypes selected

    logging.debug('loading stats options')
    try:
        # Read inputs yaml as dictionary
        with open(REDCAP_FILE, 'rt') as file:
            redcap_data = yaml.load(file, yaml.SafeLoader)
    except EnvironmentError:
        logging.info('REDCap settings file not found, not loading stats')
        return [], []

    with dax.XnatUtils.get_interface() as xnat:
        my_projects = utils.get_user_favorites(xnat)

    # Filter the list of redcaps based on our project access
    for r in redcap_data['projects']:
        name = r['name']

        try:
            (proj, proc, res) = parse_redcap_name(name)
        except ValueError:
            continue
  
        # Only projects we can access
        if (proj not in my_projects):
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
