import os

import pandas as pd

from ...log import logger
from ...utils import load_project_names, load_processors_data, save_data, read_data


def run_refresh(projects):
    df = get_data(projects)

    save_data('processors', df)

    return df


def project_names():
    return load_project_names()


def load_data(projects=None, refresh=False):
    if refresh:
        run_refresh(projects)

    df =  read_data('processors')

    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=['PROJECT', 'COMPLETE', 'TYPE'])
    
    return df


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
