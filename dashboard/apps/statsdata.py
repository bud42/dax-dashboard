import logging
import os
import yaml
import redcap
import pandas as pd
import json

from dax import XnatUtils


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

# TODO: move this to an environ var
username = 'boydb1'
REDCAP_FILE = '/home/boydb1/dashboard.redcap.yaml'
PROCTYPES = ['EDATQA_v1', 'fmriqa_v4', 'LST_v1', 'AMYVIDQA_v1']
PROJECTS = ['CHAMP']

STATS_RENAME = {
    'experiment': 'SESSION',
    'proctype': 'TYPE',
    'project': 'PROJECT',
    'proc_date': 'DATE',
    'stats_edatqa_acc_mean': 'accuracy',
    'stats_edatqa_rt_mean': 'RT',
    'stats_edatqa_trial_count': 'trials',
    'lst_stats_wml_volume': 'WML',
    'fmriqa_stats_wide_voxel_displacement_mm_95prctile': 'VOXD',
    'fmriqa_stats_wide_dvars_mean': 'DVARS',
    'stats_amyvid_compgm_suvr': 'compgm_suvr'}

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


def get_filename():
    return '{}.pkl'.format('stats')


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


def get_data():
    # Load that data
    df = load_stats_data()

    # set a column for session visit type, i.e. baseline if session name
    # ends with a or MR1 or something else, otherwise it's a followup
    df['ISBASELINE'] = df['SESSION'].apply(is_baseline_session)

    return df


def refresh_data():
    df = get_data()

    # save to cache
    save_data(df)

    return df


def parse_redcap_name(name):
    (proj, tmp) = name.split('-', 1)
    (proc, res) = tmp.rsplit('-', 1)
    return (proj, proc, res)


def load_stats_data():
    my_proctypes = PROCTYPES
    my_projects = PROJECTS
    my_redcaps = []
    df = pd.DataFrame()

    # Load assr data
    logging.debug('loading stats data')

    # Read inputs yaml as dictionary
    with open(REDCAP_FILE, 'rt') as file:
        redcap_data = yaml.load(file, yaml.SafeLoader)

    api_url = redcap_data['api_url']

    # Filter the list of redcaps based on our project access
    for r in redcap_data['projects']:
        name = r['name']

        try:
            (proj, proc, res) = parse_redcap_name(name)
        except ValueError:
            continue

        if (proj not in my_projects) or (proc not in my_proctypes):
            continue

        my_redcaps.append(r)

    # Load data from each redcap
    for r in my_redcaps:
        name = r['name']
        key = r['key']
        (proj, proc, res) = parse_redcap_name(name)
        try:
            cur_df = redcap.Project(api_url, key).export_records(format='df')
            df = pd.concat([df, cur_df], ignore_index=True, sort=False)
        except:
            print('error exporting:' + name)
            continue

    # Rename columns
    df.rename(columns=STATS_RENAME, inplace=True)

    # return the assessor data
    logging.info('loaded {} stats'.format(len(df)))
    return df
