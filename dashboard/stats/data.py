import logging
import os

import yaml
import redcap
import dax

import utils
from stats.params import REDCAP_FILE, STATS_RENAME, DEFAULT_COLUMNS

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd


# Data sources are:
#
# REDCap (using keys in file as specified in REDCAP_FILE) this is the
# source of the stats data.
#
# Note this app does not access ACCRE or SLURM. The ony local file access
# is to write the cached data in a pickle file. This file is named stats.pkl


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')


def is_baseline_session(session):
    # TODO: re-implement this by getting list of sessions for each subject,
    # sorted by date and set the first session to basline
    return (
        str(session).endswith('a') or
        str(session).endswith('_bl') or
        str(session).endswith('_MR1'))


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


def load_redcap_stats(api_url, api_key):
    # Load the redcap project, lazy for speed
    _rc = redcap.Project(api_url, api_key, lazy=True)

    # Load the data, specify index since we loaded lazy
    _df = _rc.export_records(format='df', df_kwargs={'index_col': 'record_id'})

    if 'wml_volume' in _df:
        # rename wml for NIC
        _df['lst_stats_wml_volume'] = _df['wml_volume']

    return _df


def load_stats_data():
    my_redcaps = []
    df = pd.DataFrame()

    # Load assr data
    logging.debug('loading stats data')

    try:
        # Read inputs yaml as dictionary
        with open(REDCAP_FILE, 'rt') as file:
            redcap_data = yaml.load(file, yaml.SafeLoader)
    except FileNotFoundError:
        logging.info('REDCap settings file not found, not loading stats')
        df = pd.DataFrame(columns=DEFAULT_COLUMNS)
        return df

    api_url = redcap_data['api_url']

    with dax.XnatUtils.get_interface() as xnat:
        my_projects = utils.get_user_favorites(xnat)

    # Filter the list of redcaps based on our project access
    for r in redcap_data['projects']:
        name = r['name']

        try:
            (proj, proc, res) = parse_redcap_name(name)
        except ValueError:
            continue

        if (proj in my_projects):
            my_redcaps.append(r)

    # Load data from each redcap
    icount = len(my_redcaps)
    for i, r in enumerate(my_redcaps):
        name = r['name']
        api_key = r['key']
        (proj, proc, res) = parse_redcap_name(name)
        logging.info('{}/{} loading redcap:{}'.format(i, icount, name))
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

    # return the assessor data
    logging.info('loaded {} stats'.format(len(df)))
    return df
