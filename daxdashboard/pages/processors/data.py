import os

import pandas as pd

from ...log import logger
from ...data import load_project_names, load_processors, save_data, read_data


def run_refresh():
    df = _load_processors()

    save_data('processors', df)

    return df


def project_names():
    return load_project_names()


def load_data(refresh=False):
    df = read_data('processors')

    if df is None or refresh:
        df = run_refresh()

    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=['PROJECT', 'COMPLETE', 'TYPE'])
    
    return df


def filter_data(df, projects=None):
    # Filter by project
    if projects:
        logger.debug('filtering by project:')
        logger.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    return df


def _load_processors():
    """List of records."""
    df = load_processors()

    return df
