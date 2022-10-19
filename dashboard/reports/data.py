import logging
from pathlib import Path

import pandas as pd
import redcap

import utils
import shared


# This is where we save our cache of the data
def get_filename():
    return 'DATA/reportdata.pkl'


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def load_data(refresh=False):
    filename = get_filename()

    if refresh or not Path(filename).is_file():
        run_refresh(filename)

    logging.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def run_refresh(filename):
    df = pd.DataFrame(columns=[])

    # download latest PDF for each project for each tyoe of report?
    try:
        logging.info('connecting to redcap')
        i = utils.get_projectid("main", shared.KEYFILE)
        k = utils.get_projectkey(i, shared.KEYFILE)
        mainrc = redcap.Project(shared.API_URL, k)

        # Get double entry reports
        logging.info('loading double reports from redcap')
        dfd = mainrc.export_records(forms=['main', 'double'], format_type='df')
        dfd = dfd.reset_index()

        # Download the latest report
        dfpdf = dfd.dropna(subset=['double_datetime', 'double_resultspdf'])
        dfpdf = dfpdf.sort_values(by="double_datetime").drop_duplicates(subset=["main_name"], keep="last")
        for i, r in dfpdf.iterrows():
            _proj = r['main_name']
            _id = r['redcap_repeat_instance']
            _field = 'double_resultspdf'
            _name = r[_field]
            _file = f'assets/double/{_name}'

            # Check for existing
            if Path(_file).is_file():
                logging.info(f'file exists, will not overwrite:{_file}')
                continue

            logging.info(f'download file:{_file}')
            utils.download_file(mainrc, _proj, None, _field, _file, _id)

        # Get progress reports
        logging.info('loading progress reports from redcap')
        dfp = mainrc.export_records(forms=['main', 'progress'], format_type='df')
        dfp = dfp.reset_index()

        # download recent progress reports
        dfpdf = dfp.dropna(subset=['progress_datetime', 'progress_pdf'])
        dfpdf = dfpdf.sort_values(by='progress_datetime').drop_duplicates(subset=['main_name'], keep='last')
        for i, r in dfpdf.iterrows():
            _proj = r['main_name']
            _id = r['redcap_repeat_instance']
            _field = 'progress_pdf'
            _name = r[_field]
            _file = f'assets/progress/{_name}'

            # Check for existing
            if Path(_file).is_file():
                logging.info(f'file exists, will not overwrite:{_file}')
                continue

            logging.info(f'download file:{_file}')
            utils.download_file(mainrc, _proj, None, _field, _file, _id)

        # Concat the report lists and save to file
        df = pd.concat([dfd, dfp], ignore_index=True)
        save_data(df, filename)
    except Exception as err:
        logging.error(f'failed to connect to main redcap:{err}')

    return df
