
import json

import pandas as pd
from cryptography.fernet import Fernet
from flask import session, current_app
from flask_login import current_user
from dax.XnatUtils import get_interface
from redcap import Project

from .log import logger


DONE_LIST = ['COMPLETE', 'JOB_FAILED', 'DELETED']


SCAN_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
xnat:imagesessiondata/sharing/share/project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/note,\
xnat:imagesessiondata/date,\
tracer_name,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/label,\
xnat:imageSessionData/dcmPatientId,\
xnat:imagescandata/id,\
xnat:imagescandata/type,\
xnat:imagescandata/quality,\
xnat:imagescandata/frames,\
xnat:imagescandata/file/label'


ASSR_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
xnat:imagesessiondata/sharing/share/project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/note,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
proc:genprocdata/label,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/validation/status,\
proc:genprocdata/validation/date,\
proc:genprocdata/validation/validated_by,\
proc:genprocdata/jobstartdate,\
proc:genprocdata/walltimeused,\
proc:genprocdata/memused,\
proc:genprocdata/jobnode,\
last_modified,\
proc:genprocdata/inputs'


SGP_URI = '/REST/subjects?xsiType=xnat:subjectdata\
&columns=\
project,\
label,\
proc:subjgenprocdata/label,\
proc:subjgenprocdata/date,\
proc:subjgenprocdata/procstatus,\
proc:subjgenprocdata/proctype,\
proc:subjgenprocdata/validation/status,\
proc:subjgenprocdata/inputs,\
proc:subjgenprocdata/jobstartdate,\
proc:subjgenprocdata/walltimeused,\
proc:subjgenprocdata/memused,\
proc:subjgenprocdata/jobnode,\
last_modified'

XSI2MOD = {
    'xnat:eegSessionData': 'EEG',
    'xnat:mrSessionData': 'MR',
    'xnat:petSessionData': 'PET'}

SCAN_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'session_type': 'SESSTYPE',
    'tracer_name': 'TRACER',
    'xnat:imagesessiondata/note': 'NOTE',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'xnat:imagescandata/id': 'SCANID',
    'xnat:imagescandata/type': 'SCANTYPE',
    'xnat:imagescandata/quality': 'QUALITY',
    'xsiType': 'XSITYPE',
    'xnat:imagescandata/file/label': 'RESOURCES',
    'xnat:imagescandata/frames': 'FRAMES',
}

ASSR_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'session_type': 'SESSTYPE',
    'xnat:imagesessiondata/note': 'NOTE',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'proc:genprocdata/label': 'ASSR',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/jobstartdate': 'JOBDATE',
    'proc:genprocdata/validation/status': 'QCSTATUS',
    'proc:genprocdata/validation/date': 'QCDATE',
    'proc:genprocdata/validation/validated_by': 'QCBY',
    'xsiType': 'XSITYPE',
    'proc:genprocdata/inputs': 'INPUTS',
    'proc:genprocdata/walltimeused': 'TIMEUSED',
    'proc:genprocdata/memused': 'MEMUSED',
    'proc:genprocdata/jobnode': 'JOBNODE',
}

SGP_RENAME = {
    'project': 'PROJECT',
    'label': 'SUBJECT',
    'proc:subjgenprocdata/date': 'DATE',
    'proc:subjgenprocdata/label': 'ASSR',
    'proc:subjgenprocdata/procstatus': 'PROCSTATUS',
    'proc:subjgenprocdata/proctype': 'PROCTYPE',
    'proc:subjgenprocdata/validation/status': 'QCSTATUS',
    'proc:subjgenprocdata/inputs': 'INPUTS',
    'proc:subjgenprocdata/jobstartdate': 'JOBDATE',
    'proc:subjgenprocdata/walltimeused': 'TIMEUSED',
    'proc:subjgenprocdata/memused': 'MEMUSED',
    'proc:subjgenprocdata/jobnode': 'JOBNODE',
}

TASKS_RENAME = {
    'task_assessor': 'ASSESSOR',
    'task_status': 'STATUS',
    'task_inputlist': 'INPUTLIST',
    'task_var2val': 'VAR2VAL',
    'task_memreq': 'MEMREQ',
    'task_walltime': 'WALLTIME',
    'task_procdate': 'PROCDATE',
    'task_timeused': 'TIMEUSED',
    'task_memused': 'MEMUSED',
    'task_yamlfile': 'YAMLFILE',
    'task_userinputs': 'USERINPUTS',
    'task_failcount': 'FAILCOUNT',
    'task_yamlupload': 'YAMLUPLOAD',
}

ANALYSES_RENAME = {
    'redcap_repeat_instance': 'ID',
    'analysis_name': 'NAME',
    'analysis_lead': 'INVESTIGATOR',
    'analysis_include': 'SUBJECTS',
    'analysis_processor': 'PROCESSOR',
    'analysis_input': 'INPUT',
    'analysis_output': 'OUTPUT',
    'analyses_complete': 'COMPLETE',
    'analysis_status': 'STATUS',
    'analysis_covars': 'COVARS',
    'analysis_notes': 'NOTES',
}

PROCESSORS_RENAME = {
    'redcap_repeat_instance': 'ID',
    'processor_file': 'FILE',
    'processor_filter': 'FILTER',
    'processor_args': 'ARGS',
    'processing_complete': 'COMPLETE',
}

SCAN_COLUMNS = [
    'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'TRACER', 'NOTE', 'DATE', 'SITE',
    'DURATION', 'FRAMES', 'TR', 'THICK', 'SENSE', 'MB',
    'SCANID', 'SCANTYPE', 'QUALITY', 'RESOURCES', 'MODALITY', 'XSITYPE', 'full_path',
]

ASSR_COLUMNS = [
    'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'NOTE', 'DATE', 'SITE',
    'ASSR', 'PROCSTATUS', 'PROCTYPE', 'JOBDATE', 'TIMEUSED', 'MEMUSED', 'JOBNODE',
    'QCSTATUS', 'QCDATE', 'QCBY', 'XSITYPE', 'INPUTS', 'MODALITY', 'full_path'
]

SGP_COLUMNS  = [
    'PROJECT', 'SUBJECT', 'ASSR', 'PROCSTATUS', 'PROCTYPE', 'QCSTATUS', 
    'INPUTS', 'DATE', 'XSITYPE', 'JOBDATE', 'TIMEUSED', 'MEMUSED', 'JOBNODE'
]

TASK_COLUMNS = [
    'ID', 'IDLINK', 'PROJECT', 'STATUS', 'PROCTYPE', 'MEMREQ', 'WALLTIME',
    'TIMEUSED', 'MEMUSED', 'ASSESSOR', 'PROCDATE', 'INPUTLIST', 'VAR2VAL',
    'IMAGEDIR', 'JOBTEMPLATE', 'YAMLFILE', 'YAMLUPLOAD', 'USERINPUTS', 
    'FAILCOUNT', 'USER'
]

ANALYSES_COLUMNS = [
    'PROJECT', 'ID', 'NAME', 'STATUS', 'EDIT', 'NOTES', 'SUBJECTS', 
    'PROCESSOR', 'INVESTIGATOR', 'OUTPUT'
]

PROCESSORS_COLUMNS = [
    'ID', 'PROJECT', 'TYPE', 'EDIT', 'FILE', 'FILTER', 'ARGS',
    'YAMLUPLOAD', 'EDIT', 'COMPLETE', 'CUSTOM'
]


def decrypt_key(cipher_suite, encrypted_key):
    decrypted_text = cipher_suite.decrypt(encrypted_key.encode())
    return decrypted_text.decode()


def encrypt_key(cipher_suite, key):
    encrypted_text = cipher_suite.encrypt(key.encode())
    return encrypted_text.decode()


def get_xnat_alias(xnat_host, xnat_user, xnat_pass):
    try:
        # Connect to xnat
        xnat = get_interface(host=xnat_host, user=xnat_user, pwd=xnat_pass)
    except Exception as err:
        raise err

    # Get an alias/token for user using tokens issue service
    uri = '/data/services/tokens/issue'
    result = json.loads(xnat._exec(uri, 'GET'), strict=False)

    alias = result.get('alias')
    token = result.get('secret')

    return (alias, token)


def get_redcap_info(redcap_url, redcap_key):
    info = {}

    try:
        rc = Project(redcap_url, redcap_key)
        info['redcap_version'] = str(rc.redcap_version)
        info['redcap_pid'] = str(rc.export_project_info().get('project_id'))
    except Exception as err:
        raise Exception('failed to retrieve redcap info')

    return info


def load_project_names():
    # TODO: store in cache and load
    print('dashboard.utils.load_project_names()')

    if not current_user.is_authenticated:
        raise Exception('no user logged in')

    user_name = current_user.id


    if user_name == 'admin':
        logger.debug('loading admin projects')
        xnat_names = get_admin_projects()
    else:
        logger.debug(f'not admin, loading user projects:{user_name}')
        xnat_names = get_my_projects()

    return xnat_names


def load_scan_data(projects):
    xnat_host = session['xnat_host']
    scans = []
    uri = SCAN_URI + f'&project={",".join(projects)}'

    result = _get_result(uri)

    # Get shared
    uri = SCAN_URI + f'&xnat:imagesessiondata/sharing/share/project={",".join(projects)}'
    result2 = _get_result(uri)

    # Set project to shared name
    for r in result2:
        r['project'] = r['xnat:imagesessiondata/sharing/share/project']

    # Append shared
    result += result2

    # Change from one row per resource to one row per scan
    scans = {}
    for r in result:
        k = (r['project'], r['session_label'], r['xnat:imagescandata/id'])
        if k in scans.keys():
            # Append to list of resources
            _resource = r['xnat:imagescandata/file/label']
            scans[k]['RESOURCES'] += ',' + _resource
        else:
            scans[k] = _scan_info(r)

    scans = list(scans.values())
    df = pd.DataFrame(scans, columns=SCAN_COLUMNS)

    df['SESSIONLINK'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION']

    df['SUBJECTLINK'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT']

    df['NIFTI'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION'] + \
        '/scans/' + df['SCANID'] + \
        '/resources/NIFTI/files?format=zip'

    df['JSON'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION'] + \
        '/scans/' + df['SCANID'] + \
        '/resources/JSON/files?format=zip'

    df['EDAT'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION'] + \
        '/scans/' + df['SCANID'] + \
        '/resources/EDAT/files?format=zip'

    return df


def load_assr_data(projects):
    """Get assessor info from XNAT as list of dicts."""
    xnat_host = session['xnat_host']
    assessors = []
    uri = ASSR_URI + f'&project={",".join(projects)}'

    result = _get_result(uri)

    # Get shared
    uri = ASSR_URI + f'&xnat:imagesessiondata/sharing/share/project={",".join(projects)}'
    result2 = _get_result(uri)

    # Set project to shared name
    for r in result2:
        r['project'] = r['xnat:imagesessiondata/sharing/share/project']

    # Append shared
    result += result2

    # Load extended info
    for r in result:
        assessors.append(_assessor_info(r))

    df =  pd.DataFrame(assessors, columns=ASSR_COLUMNS)

    df['SESSIONLINK'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION']

    df['SUBJECTLINK'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT']

    df['PDF'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION'] + \
        '/assessors/' + df['ASSR'] + \
        '/out/resources/PDF/files/' + \
        'report_' + df['ASSR'] + '.pdf'

    df['LOG'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION'] + \
        '/assessors/' + df['ASSR'] + \
        '/out/resources/OUTLOG/files/' + \
        df['ASSR'] + '.txt'

    return df


def load_sgp_data(projects):
    """Get assessor info from XNAT as list of dicts."""
    xnat_host = session['xnat_host']
    assessors = []
    uri = SGP_URI + f'&project={",".join(projects)}'

    logger.debug(f'get_result uri=:{uri}')
    result = _get_result(uri)

    for r in result:
        assessors.append(_sgp_info(r))

    df =  pd.DataFrame(assessors, columns=SGP_COLUMNS)

    df['SUBJECTLINK'] = xnat_host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT']

    return df


def _get_result(uri):
    """Get result of xnat query."""

    if not current_user.is_authenticated:
        raise Exception('no user logged in')

    # Connect to our encryption tool
    fernet = Fernet(current_app.config['SECRET_KEY'])

    xnat_host = session['xnat_host']
    xnat_alias = decrypt_key(fernet, session['xnat_alias'])
    xnat_token = decrypt_key(fernet, session['xnat_token'])

    logger.debug(xnat_host)
    logger.debug(uri)

    with get_interface(xnat_host, xnat_alias, xnat_token) as xnat:
        json_data = json.loads(xnat._exec(uri, 'GET'), strict=False)
        result = json_data['ResultSet']['Result']

    return result


def _redcap():
    if not current_user.is_authenticated:
        raise Exception('no user logged in')

    # Connect to our encryption tool
    fernet = Fernet(current_app.config['SECRET_KEY'])

    # Get redcap params from web session
    rc_host = session['rc_host']
    rc_key = decrypt_key(fernet, session['rc_key'])

    return Project(rc_host, rc_key)


def _scan_info(record):
    """Get scan info."""
    info = {}

    for k, v in SCAN_RENAME.items():
        info[v] = record[k]

    # set_modality
    info['MODALITY'] = XSI2MOD.get(info['XSITYPE'], 'UNK')

    # Get the full path
    _p = '/projects/{0}/subjects/{1}/experiments/{2}/scans/{3}'.format(
        info['PROJECT'],
        info['SUBJECT'],
        info['SESSION'],
        info['SCANID'])
    info['full_path'] = _p

    return info

def _assessor_info(record):
    """Get assessor info."""
    info = {}

    for k, v in ASSR_RENAME.items():
        info[v] = record[k]

    # Decode inputs into list
    #info['INPUTS'] = utils_xnat.decode_inputs(info['INPUTS'])

    # Get the full path
    _p = '/projects/{0}/subjects/{1}/experiments/{2}/assessors/{3}'.format(
        info['PROJECT'],
        info['SUBJECT'],
        info['SESSION'],
        info['ASSR'])
    info['full_path'] = _p

    # set_modality
    info['MODALITY'] = XSI2MOD.get(info['XSITYPE'], 'UNK')

    return info


def _sgp_info(record):
    """Get subject assessor info."""
    info = {}

    # Copy with new var names
    for k, v in SGP_RENAME.items():
        info[v] = record[k]

    info['XSITYPE'] = 'proc:subjgenprocdata'

    # Decode inputs into list
    #info['INPUTS'] = utils_xnat.decode_inputs(info['INPUTS'])

    # Get the full path
    _p = '/projects/{0}/subjects/{1}/assessors/{2}'.format(
        info['PROJECT'],
        info['SUBJECT'],
        info['ASSR'])
    info['full_path'] = _p

    return info


def get_admin_projects():
    """Get result of xnat query."""
    uri = '/data/archive/projects?accessible=true'
    logger.debug(uri)

    result = _get_result(uri)

    if len(result) == 0:
        return []

    return [x['id'] for x in result]


def get_my_projects():
    """Get result of xnat query."""
    roles = ['Owners', 'Members', 'Collaborators']
    uri = '/data/archive/projects?accessible=true'

    logger.debug(uri)

    result = _get_result(uri)

    if len(result) == 0:
        return []

    user_role_column = None
    # Search for user role column
    for k in result[0].keys():
        if k.startswith('user_role_'):
            user_role_column = k
            break

    if user_role_column is None:
        logger.info(f'could not determine user role column')
        return []

    logger.debug(f'user role column:{user_role_column}')

    return [x['id'] for x in result if x[user_role_column] in roles]


def load_task_data():
    # Load data from redcap
    rc = _redcap()
    def_field = rc.def_field

    # Load task records
    rec = rc.export_records(
        #records=projects,
        forms=['taskqueue'],
        fields=[def_field])

    # Load instance names 
    rec2 = rc.export_records(
        #records=projects,
        fields=[def_field, 'gen_daxinstance'],
        raw_or_label='label')

    # Remove unwanted rows
    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'taskqueue']

    # Hide done
    rec = [x for x in rec if x['task_status'] not in DONE_LIST]

    df = pd.DataFrame(rec)
    if df.empty:
        return pd.DataFrame(columns=TASK_COLUMNS)

    # Set project namne from main record name
    df['PROJECT'] = df[def_field]

    # Set instance name for each task record
    p2u = {x[def_field]: x['gen_daxinstance'] for x in rec2 if x['gen_daxinstance']}
    df['USER'] = df['PROJECT'].map(p2u)

    # Set task ID same as redcap record number
    df['ID'] = df['redcap_repeat_instance'].astype(str)

    # Make ID link back to redcap
    _url = session['rc_host']
    _version = session['rc_version']
    _pid = session['rc_pid']

    if _url.endswith('/api/'):
        _url = _url[:-5]

    df['IDLINK'] = _url + '/redcap_v' + _version + '/DataEntry/index.php?pid=' + _pid + '&page=taskqueue&id=' + df['PROJECT'] + '&instance=' + df['ID']
    df['PROCTYPE'] = ''
    df['IMAGEDIR'] = ''
    df['JOBTEMPLATE'] = ''

    # Rename columns to shorter names
    df = df.rename(columns=TASKS_RENAME)

    # Get subset of columns
    df = df[TASK_COLUMNS]

    return df


def load_processors_data():
    data = []
    def_field = ''
    rec = []
    #projects = load_project_names()

    # Load data from redcap
    rc = _redcap()
    def_field = rc.def_field
    rec = rc.export_records(
        #records=projects,
        forms=['processing'],
        fields=[def_field])

    # Filter out unwanted rows
    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'processing']

    # Only enabled processing
    rec = [x for x in rec if str(x['processing_complete']) == '2']

    for r in rec:
        # Initialize record with project
        project_id = r[def_field]
        repeat_id = r['redcap_repeat_instance']
        link = _get_redcap_link('processing', project_id, repeat_id)
        d = {
            'PROJECT': project_id,
            'EDIT': link,
            'ID': repeat_id,
        }

        # Find the yaml file
        if r['processor_yamlupload']:
            filepath = r['processor_yamlupload']
        else:
            filepath = r['processor_file']

        #if not os.path.isabs(filepath):
        #    # Prepend lib location
        #    filepath = os.path.join(self._yamldir, filepath)


        # Get renamed variables
        for k, v in PROCESSORS_RENAME.items():
            d[v] = r.get(k, '')

        d['FILE'] = filepath
        #d['TYPE'] = self._get_proctype(d['FILE'])
        d['TYPE'] = d['FILE']

        # Finally, add to our list
        data.append(d)

    return pd.DataFrame(data, columns=PROCESSORS_COLUMNS)


def load_analyses_data():
    data = []
    rec = []
    def_field = ''

    # Load data from redcap
    rc = _redcap()
    def_field = rc.def_field

    # Load records
    rec = rc.export_records(
        #records=projects,
        forms=['analyses'],
        fields=[def_field])

    # Filter out unwanted rows
    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'analyses']

    # Apply hideshow
    rec = [x for x in rec if x['analysis_hideshow'] != '1']

    for r in rec:
        # Initialize record
        project_id = r[def_field]
        repeat_id = r['redcap_repeat_instance']
        link = _get_redcap_link('analyses', project_id, repeat_id)
        d = {
            'PROJECT': project_id,
            'ID': repeat_id,
            'EDIT': link,
        }

        # Get renamed variables
        for k, v in ANALYSES_RENAME.items():
            d[v] = r.get(k, '')

        if r['analysis_procrepo']:
            d['PROCESSOR'] = r['analysis_procrepo']

        # Finally, add to our list
        data.append(d)

    df = pd.DataFrame(data, columns=ANALYSES_COLUMNS)

    # Pad with zeros
    df['ID'] = df['ID'].astype(str).str.zfill(3)

    xnat_host = session['xnat_host']

    df['OUTPUTLINK'] = xnat_host + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files'

    df['LOGLINK'] = xnat_host + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files/' + \
        df['OUTPUT'] + \
        '.txt'

    df['PDFLINK'] = xnat_host + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files/report.pdf'

    df['PBSLINK'] = xnat_host + \
        '/data/projects/' + \
        df['PROJECT'] + \
        '/resources/' + \
        df['OUTPUT'] + \
        '/files/' + \
        df['OUTPUT'] + \
        '.slurm'

    return df


def _get_redcap_link(instrument, project_id, repeat_id):
        redcap_url = session['rc_host']
        redcap_version = session['rc_version']
        redcap_pid = session['rc_pid']

        if redcap_url.endswith('/api/'):
            redcap_url = redcap_url[:-5]

        redcap_link = f'{redcap_url}/redcap_v{redcap_version}/DataEntry/index.php?pid={redcap_pid}&page={instrument}&id={project_id}&instance={repeat_id}'

        return redcap_link
