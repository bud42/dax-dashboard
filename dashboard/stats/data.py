import logging
import os
from datetime import datetime

import yaml
import redcap
import dax

import utils
from stats.params import REDCAP_FILE, STATS_RENAME, STATIC_COLUMNS, VAR_LIST

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


def static_columns():
    return STATIC_COLUMNS


def get_vars():
    return get_variables()


def get_variables():
    return VAR_LIST


def get_filename():
    return '{}.pkl'.format('stats')


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


def get_data():
    # Load that data
    df = load_stats_data()

    # set a column for session visit type, i.e. baseline if session name
    # ends with a or MR1 or something else, otherwise it's a followup
    df['ISBASELINE'] = df['SESSION'].apply(utils.is_baseline_session)

    # set the site
    df['SITE'] = df['SESSION'].apply(utils.set_site)

    return df


def run_refresh(filename):
    df = get_data()

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
    _rc = redcap.Project(api_url, api_key, lazy=True)

    # Load the data, specify index since we loaded lazy
    _df = _rc.export_records(format='df', df_kwargs={'index_col': 'record_id'})

    if 'wml_volume' in _df:
        # rename wml for NIC
        _df['lst_stats_wml_volume'] = _df['wml_volume']

    print(_df.columns)

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
    except EnvironmentError:
        logging.info('REDCap settings file not found, not loading stats')
        df = pd.DataFrame(columns=static_columns())
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
    print('_keep', _keep)
    df = df[_keep]

    # return the stats data
    logging.info('loaded {} stats'.format(len(df)))
    return df


def filter_data(df, projects, proctypes, timeframe, sesstype):
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

    if len(df) > 0:
        # Filter by sesstype
        if sesstype == 'baseline':
            logging.debug('filtering by baseline only')
            df = df[df['ISBASELINE']]
        elif sesstype == 'followup':
            logging.debug('filtering by followup only')
            df = df[~df['ISBASELINE']]
        else:
            logging.debug('not filtering by sesstype')
            pass

        # remove test sessions
        df = df[df.SESSION != 'Pitt_Test_Upload_MR1']

    return df
