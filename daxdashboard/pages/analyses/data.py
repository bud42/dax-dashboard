import pandas as pd

from ...log import logger
from ...utils import load_project_names, load_analyses_data
from ...extensions import cache


def run_refresh():
    df = get_data()

    save_data(df)

    return df


def load_data(refresh=False):
    if refresh:
        run_refresh()

    return read_data()


def read_data():
    df = cache.get('analysesdata')

    if df is None or len(df) == 0:
        df = pd.DataFrame(columns=[
            'PROJECT', 'SUBJECTS', 'INVESTIGATOR', 'STATUS']
        )
 
    return df


def save_data(df):
    # save to cache
    cache.set('analysesdata', df)


def get_data():
    # Load
    df = _load_analyses_data()

    # Pad with zeros
    df['ID'] = df['ID'].astype(str).str.zfill(3)

    df['OUTPUTLINK'] = 'xnat_host' + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files'

    df['LOGLINK'] = 'xnat_host' + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files/' + \
        df['OUTPUT'] + \
        '.txt'

    df['PDFLINK'] = 'xnat_host' + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files/report.pdf'

    df['PBSLINK'] = 'xnat_host' + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files/' + \
        df['OUTPUT'] + \
        '.slurm'

    return df


def _load_analyses_data(projects=None):
    """List of analyses records."""

    df = load_analyses_data()

    return df


def filter_data(df, time=None):
    # Filter
    if time:
        pass

    return df


def project_names():
    return load_project_names()
