import os

import pandas as pd

from ...log import logger
from ...utils import load_project_names, load_processors_data
from ...extensions import cache


def run_refresh(projects):
    df = get_data(projects)

    save_data(df)

    return df


def project_names():
    return load_project_names()


def load_data(projects=None, refresh=False):
    if refresh:
        run_refresh(projects)

    return read_data()


def read_data():
    df = cache.get('processors')

    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=['PROJECT', 'COMPLETE', 'TYPE'])
 
    return df


def save_data(df):
    # save to cache
    cache.set('processors', df)


def get_data(projects):
    # Load
    df = _load_processors_data(projects)

    df['FILE'] = df['FILE'].apply(os.path.basename)

    df = df.sort_values(['PROJECT', 'FILE'])

    return df


def filter_data(df):
    # TBD

    return df


def _load_processors_data(projects=None):
    """List of records."""
    df = load_processors_data()

    return df
