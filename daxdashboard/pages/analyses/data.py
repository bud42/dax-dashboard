import pandas as pd

from ...log import logger
from ...data import load_project_names, load_analyses, read_data, save_data


def run_refresh():
    df = get_data()

    save_data('analyses', df)

    return df


def load_data(refresh=False):
    df = read_data('analyses')

    if refresh or df is None:
        df = run_refresh()
    
    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=['PROJECT', 'ID', 'SUBJECTS', 'INVESTIGATOR', 'STATUS'])

    return df


def get_data():
    # Load
    df = _load_analyses()

    return df


def _load_analyses(projects=None):
    """List of analyses records."""

    df = load_analyses()

    return df


def filter_data(df, projects=None, leads=None, statuses=None, time=None):
     # Filter by project
    if projects:
        logger.debug('filtering by project:')
        logger.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Then by leads
    if leads:
        df = df[df['INVESTIGATOR'].isin(leads)]

    # finally by status
    if statuses:
        df = df[df['STATUS'].isin(statuses)]

    # Filter
    if time:
        pass

    return df


def project_names():
    return load_project_names()
