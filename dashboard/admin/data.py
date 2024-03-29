import logging
import re
import tempfile
from datetime import datetime
import shutil

import redcap
import qa.data as qa_data
from qa.gui import qa_pivot
import stats.data as stats_data
import activity.data as activity_data
import utils
import shared
from .progress_report import make_project_report


SESSCOLUMNS = ['SESSION', 'PROJECT', 'DATE', 'SESSTYPE', 'SITE', 'MODALITY']


def get_projects():
    logging.info('get_projects')
    projects = []

    # Get list of projects from main redcap
    try:
        logging.info('connecting to redcap')
        i = utils.get_projectid("main", shared.KEYFILE)
        k = utils.get_projectkey(i, shared.KEYFILE)
        mainrc = redcap.Project(shared.API_URL, k)

        # Get list of projects
        maindata = mainrc.export_records(forms=['main'])
        projects = sorted(list(set([x['main_name'] for x in maindata])))
    except Exception as err:
        logging.error(f'failed to connect to main redcap:{err}')

    return projects


def load_session_info(project):
    df = qa_data.load_data()
    df = df[df.PROJECT == project]
    df = df[SESSCOLUMNS].drop_duplicates().sort_values('SESSION')
    return df


def load_phantom_info(project):
    df = qa_data.load_data()
    df = df[df.PROJECT == project]
    df = df[SESSCOLUMNS].drop_duplicates().sort_values('SESSION')
    return df


def load_activity_info(project):
    df = activity_data.load_data()
    df = df[df.PROJECT == project]
    return df


def load_stats(project, stattypes):
    # Load that data
    df = stats_data.load_data([project], stattypes, refresh=True)
    if df.empty:
        return df

    # Filter by project
    df = df[df.PROJECT == project]

    # Sort it
    df = df.sort_values('SESSION')

    # Return the DataFrame
    return df


def load_scanqa_info(project, scantypes):
    # Load that data
    df = qa_data.load_data()
    df = df[df.PROJECT == project].sort_values('SESSION')
    dfp = qa_pivot(df).reset_index()
    if not scantypes:
        scantypes = [x for x in dfp.columns if not re.search('_v\d+$', x)]

    # Filter columns to include
    include_list = SESSCOLUMNS + scantypes
    include_list = [x for x in include_list if x in dfp.columns]
    include_list = list(set(include_list))
    dfp = dfp[include_list]

    # Drop columns that are all empty
    dfp = dfp.dropna(axis=1, how='all')

    return dfp


def load_assrqa_info(project, assrtypes):
    # Load that data
    df = qa_data.load_data()
    df = df[df.PROJECT == project].sort_values('SESSION')
    dfp = qa_pivot(df).reset_index()
    if not assrtypes:
        assrtypes = [x for x in dfp.columns if re.search('_v\d+$', x)]

    # Filter columns to include
    include_list = SESSCOLUMNS + assrtypes
    include_list = [x for x in include_list if x in dfp.columns]
    include_list = list(set(include_list))
    dfp = dfp[include_list]

    return dfp


def update_double_reports(project_filter):
    from .dataentry_compare import update_reports

    # Get list of projects from main redcap
    try:
        logging.info('connecting to redcap')
        i = utils.get_projectid("main", shared.KEYFILE)
        k = utils.get_projectkey(i, shared.KEYFILE)
        mainrc = redcap.Project(shared.API_URL, k)
    except Exception as err:
        logging.error(f'failed to connect to main redcap:{err}')
        return

    update_reports(mainrc, shared.KEYFILE, project_filter)


def update_redcap_reports(project_filter):
    results = []

    # Get list of projects from main redcap
    try:
        logging.info('connecting to redcap')
        i = utils.get_projectid("main", shared.KEYFILE)
        k = utils.get_projectkey(i, shared.KEYFILE)
        mainrc = redcap.Project(shared.API_URL, k)
    except Exception as err:
        logging.error(f'failed to connect to main redcap:{err}')
        return

    # Get list of projects
    maindata = mainrc.export_records(forms=['main'])
    proj_list = sorted(list(set([x['main_name'] for x in maindata])))

    with tempfile.TemporaryDirectory() as outdir:
        # Update each project
        for proj_name in proj_list:
            if proj_name == 'root':
                logging.debug('skipping root')
                return

            if project_filter and proj_name != project_filter:
                logging.debug(f'skipping project {proj_name}')
                continue

            scantypes = []
            assrtypes = []
            now = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
            filename = f'{outdir}/{proj_name}_report_{now}.pdf'

            logging.info(f'updating project {proj_name}:{filename}')

            proj_data = mainrc.export_records(
                forms=['main'], records=[proj_name]).pop()

            # Get phantom project name
            phan_project = proj_data.get('main_phanproject', '')

            # Get the scantypes, assrtypes from scanning forms
            scan_data = mainrc.export_records(
                forms=['scanning'],
                records=[proj_name],
                export_checkbox_labels=True,
                raw_or_label='label')

            for cur_data in scan_data:
                for k, v in cur_data.items():
                    # Append the scan/assr types for this scanning record
                    if v and k.startswith('scanning_scantypes'):
                        scantypes.append(v)

                    if v and k.startswith('scanning_proctypes'):
                        assrtypes.append(v)

            # Make the lists unique
            scantypes = list(set((scantypes)))
            assrtypes = list(set((assrtypes)))
            stattypes = assrtypes

            logging.debug(f'phantom_project={phan_project}')
            logging.debug(scantypes)
            logging.debug(assrtypes)
            logging.debug(stattypes)

            results += make_project_report(
                filename,
                proj_name,
                scantypes=scantypes,
                assrtypes=assrtypes,
                stattypes=stattypes,
                xsesstypes=[],
                phantom_project=phan_project)

            logging.debug(f'uploading report:{proj_name}:{filename}')
            upload_report(filename, mainrc, proj_name)

            # Save PDF to reports
            shutil.copy(filename, 'assets/progress/')

    return results


def upload_report(filename, mainrc, project_name):
    # Add new record
    try:
        progress_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record = {
            'progress_datetime': progress_datetime,
            'main_name': project_name,
            'redcap_repeat_instrument': 'progress',
            'redcap_repeat_instance': 'new',
            'progress_complete': '2',
        }
        response = mainrc.import_records([record])
        assert 'count' in response
        logging.info('successfully created new record')

        # Determine the new record id
        logging.info('locating new record')
        _ids = utils.match_repeat(
            mainrc,
            project_name,
            'progress',
            'progress_datetime',
            progress_datetime)
        repeat_id = _ids[-1]

        # Upload output files
        logging.info(f'uploading files to:{repeat_id}')
        utils.upload_file(
            mainrc,
            project_name,
            None,
            'progress_pdf',
            filename,
            repeat_id=repeat_id)
    except AssertionError as err:
        logging.error(f'upload failed:{err}')
    except (ValueError, redcap.RedcapError) as err:
        logging.error(f'error uploading:{err}')


def check_issues(project_filter):
    from .audits import run_audits, update_issues

    try:
        logging.info('connecting to redcap')
        i = utils.get_projectid("main", shared.KEYFILE)
        k = utils.get_projectkey(i, shared.KEYFILE)
        mainrc = redcap.Project(shared.API_URL, k)

        # Identify current issues by running audit
        logging.info('running audits to find issues')
        issues = run_audits(mainrc, project_filter)

        # Save issues to redcap
        logging.info('updating check_issues')
        update_issues(issues, mainrc, project_filter)

    except Exception as err:
        logging.error(f'failed to connect to main redcap:{err}')
        return
