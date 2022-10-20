import os
from datetime import datetime
import logging
import argparse
import tempfile

import redcap
import dax

from utils_xnat import get_unmatched
import utils


def audit_imaging(
    project,
    events,
    date_field,
    src_sess_field,
    src_project_name,
    dst_project_name,
    event2sess,
    use_secondary=False
):

    from .audit_imaging import audit

    return audit(
        project,
        events,
        date_field,
        src_sess_field,
        None,
        src_project_name,
        dst_project_name,
        use_secondary=use_secondary,
        event2sess=event2sess)


def audit_edat(
    project,
    events,
    raw_field,
    tab_field,
    ready_field,
    use_secondary=False
):

    from .audit_edat import audit

    return audit(
        project,
        events,
        raw_field,
        tab_field,
        ready_field,
        use_secondary=use_secondary)


def audit_project(primaryrc, proj_maindata, maindata):
    issues = []
    event2sess = {}
    proj_name = proj_maindata['main_name']
    if proj_maindata['main_usesecondary'] == '1':
        use_secondary = True
    else:
        use_secondary = False

    # Get subsets of records from main
    _data = [x for x in maindata if x['main_name'] == proj_name]
    edat_data = [x for x in _data if x['redcap_repeat_instrument'] == 'edat']
    scan_data = [x for x in _data if x['redcap_repeat_instrument'] == 'scanning']

    # Audit edats
    for rec in edat_data:
        # Load event list from comma-delimited value
        events = rec['edat_events']
        events = [x.strip() for x in events.split(',')]

        # Run the audit
        issues += audit_edat(
            primaryrc,
            events,
            rec['edat_rawfield'],
            rec['edat_convfield'],
            rec['edat_readyfield'],
            use_secondary=use_secondary)

    # Scanning
    for rec in scan_data:
        # Determine events in the primary redcap
        events = rec['scanning_events']
        if events:
            # Convert events from string to list
            events = [x.strip() for x in events.split(',')]
            event2sess = {e: rec['scanning_xnatsuffix'] for e in events}
        else:
            events = None
            event2sess = {'None': rec['scanning_xnatsuffix']}

        # Check for alternate redcap
        if rec['scanning_altprimary'] != '':
            _id = rec['scanning_altprimary']
            _key = utils.get_projectkey(_id, keyfile)

            if not _key:
                logging.info(f'no key found for project:{_id}')
                continue

            _rc = redcap.Project(primaryrc.url, _key)
        else:
            _rc = primaryrc

        issues += audit_imaging(
            _rc,
            events,
            rec['scanning_datefield'],
            rec['scanning_srcsessfield'],
            rec['scanning_srcproject'],
            proj_name,
            event2sess,
            use_secondary=use_secondary)

    # Ensure project is set
    for i in issues:
        i['project'] = proj_name

    return issues


def audit_root(maindata, project_filter=None):
    issues = []
    pimap = []
    pi2main = {}
    pi2ignore = {}
    main2phan = {}
    params = {}

    # Build dictionary of main project to phantom project names
    for rec in maindata:
        if rec['main_phanproject']:
            k = rec['main_name']
            v = rec['main_phanproject']
            main2phan[k] = v

    # Get subset of records from main
    scan_data = [x for x in maindata if x['redcap_repeat_instrument'] == 'scanning']

    # Build map of pi 2 main lists from scanning autos
    for rec in scan_data:
        src_project = rec['scanning_srcproject']
        dst_project = rec['main_name']
        ignore_sessions = sorted(rec['scanning_ignore'].replace(' ','').split(','))

        # Append main project to PI list
        if src_project not in pi2main:
            # Make a new list
            pi2main[src_project] = [dst_project]
        elif dst_project not in pi2main[src_project]:
            # Add to existing lists
            pi2main[src_project].append(dst_project)

        else:
            # it's already in the list from a previous record
            pass

        # Append ignore sessions to PI list
        if src_project not in pi2ignore:
            pi2ignore[src_project] = ignore_sessions
        else:
            # Add to existing list
            ignore_sessions = sorted(list(set(ignore_sessions + pi2ignore[src_project])))
            pi2ignore[src_project] = ignore_sessions

        # Append phantom project to PI list
        if dst_project in main2phan and main2phan[dst_project] not in pi2main[src_project]:
            # Add to existing lists
            pi2main[src_project].append(main2phan[dst_project])

    # Transform pimap to list with each item a dict with keys pi and main where
    # main is a list of project names
    for k, v in pi2main.items():
        pimap.append({'pi': k, 'main': v, 'ignore': pi2ignore[k]})

    # Find any sessions in PI projects not in main projects
    try:
        params = {'pimap': pimap}
        logging.info('get unmatched sessions')
        unmatched = get_unmatched(params)
        logging.info(f'unmatched count={len(unmatched)}')

        for u in sorted(unmatched):
            logging.debug('{}:{}'.format('unmatched session', u))
            pi, sess = u.split('_', 1)

            # Create an issue for each main project, we don't know which it is
            for p in pi2main[pi]:
                if project_filter and p != project_filter:
                    continue

                issues.append({
                    'project': p,
                    'type': 'UNMATCHED_SESSION',
                    'session': sess,
                    'description': u})

    except Exception as err:
        logging.error(f'failed to get unmatched sessions:{err}')
        for k in pi2main.keys():
            for p in pi2main[k]:
                if project_filter and p != project_filter:
                    continue

                issues.append({
                    'project': p,
                    'type': 'ERROR',
                    'description': ' Failed to get unmatched sessions',
                    'error': str(err)})

    return issues


def run_audits(mainrc, project_filter=None, function_filter=None):
    issues = []
    maindata = mainrc.export_records()

    # Get list of projects
    proj_list = sorted(list(set([x['main_name'] for x in maindata])))

    # Now iterate each project
    for p in proj_list:
        if project_filter and p != project_filter:
            logging.debug(f'skipping project {p}')
            continue

        logging.info(f'auditing project:{p}')

        # Get main data for project
        proj_maindata = {}
        for rec in maindata:
            if rec['main_name'] == p and rec['redcap_repeat_instrument'] == '':
                proj_maindata = rec

        # Connect to the primary redcap for this project
        primaryid = proj_maindata['project_primary']
        if not primaryid:
            logging.info(f'no primary id found for project:{p}')
            continue

        _key = utils.get_projectkey(primaryid, keyfile)
        if not _key:
            logging.info(f'no key found for project:{p}')
            continue

        primaryrc = redcap.Project(mainrc.url, _key)

        try:
            issues += audit_project(primaryrc, proj_maindata, maindata)
        except Exception as err:
            msg = f'error auditing project:{p}:{err}'
            logging.error(msg)
            issues.append({
                'type': 'ERROR',
                'project': p,
                'description': msg
            })
            import traceback
            traceback.print_exc()

    # Find UMATCHED_SESSION and other issues not project-specific
    issues += audit_root(maindata, project_filter=project_filter)

    return issues


def matching_issues(issue1, issue2):
    # Matching means both issues are of the same Type
    # on the same Project/Subject
    # and as applicable, the same XNAT Session/Scan
    # and as applicable the same REDCap Event/Field
    keys = [
        'main_name', 'issue_type', 'issue_subject',
        'issue_session', 'issue_scan', 'issue_event', 'issue_field']

    for k in keys:
        if (k in issue1) and (issue1[k] != issue2[k]):
            return False

    return True


def delete_old_issues(project, project_filter=None, days=7):
    # Load the currently completed issues data
    records = project.export_records(forms=['main', 'issues'])
    records = [x for x in records if x['redcap_repeat_instrument'] == 'issues']
    records = [x for x in records if str(x['issues_complete']) == '2']

    if project_filter:
        # Filter by project so we don't affect other projects
        records = [x for x in records if x['main_name'] == project_filter]

    for r in records:
        # Find how many days old the record is
        record_date = r['issue_closedate']
        try:
            record_date = datetime.strptime(record_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            record_date = datetime.strptime(record_date, '%Y-%m-%d')

        days_old = (datetime.now() - record_date).days

        # Delete if more than requested days
        if days_old >= days:
            _main = r['main_name'],
            _id = r['redcap_repeat_instance']
            logging.debug(f'deleting:issues:{_main}:{_id}:{days_old} days old')
            # https://redcap.vanderbilt.edu/api/help/?content=del_records
            try:
                _payload = {
                    'action': 'delete',
                    'returnFormat': 'json',
                    'records[0]': _main,
                    'instrument': 'issues',
                    'repeat_instance': _id,
                    'content': 'record',
                    'token': project.token,
                    'format': 'json'}

                project._call_api(_payload, 'del_record')
            except Exception as err:
                logging.error(f'failed to delete records:{err}')


def update_issues(records, project, project_filter=None):
    new_issues = []
    old_issues = []
    has_errors = False
    field_map = {
        'project': 'main_name',
        'date': 'issue_date',
        'type': 'issue_type',
        'subject': 'issue_subject',
        'session': 'issue_session',
        'scan': 'issue_scan',
        'event': 'issue_event',
        'field': 'issue_field',
        'description': 'issue_description'}

    # First check existing issues,
    # import new issues and update existing,
    # complete(or delete???) any no longer found

    # Check for errors
    for r in records:
        if r['type'] == 'ERROR':
            has_errors = True
            break

    # Remap field names to match redcap
    records = [{field_map[k]:r[k] for k in r} for r in records]

    # Complete other required fields
    for r in records:
        r['redcap_repeat_instrument'] = 'issues'
        r['redcap_repeat_instance'] = 'new'
        r['issue_date'] = r.get(
            'issue_date',
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Load the current existing issues data
    cur = project.export_records(forms=['main', 'issues'])
    cur = [x for x in cur if x['redcap_repeat_instrument'] == 'issues']
    cur = [x for x in cur if str(x['issues_complete']) != '2']

    if project_filter:
        # Filter by project so we don't affect other projects
        cur = [x for x in cur if x['main_name'] == project_filter]

    # Find new issues
    for r in records:
        isnew = True
        for c in cur:
            # Try to find a matching record
            if matching_issues(r, c):
                isnew = False
                _id = c['redcap_repeat_instance']
                _proj = c['main_name']
                logging.debug(f'matches existing issue:{_proj}:{_id}')
                break

        if isnew:
            new_issues.append(r)

    # Upload new records
    if new_issues:
        logging.info(f'uploading {len(new_issues)} new issues to redcap')
        try:
            response = project.import_records(new_issues)
            assert 'count' in response
            logging.info('issues successfully uploaded')
        except AssertionError as err:
            logging.error(f'issues upload failed:{err}')
    else:
        logging.info('no new issues to upload')

    # Find old issues
    logging.debug('checking for old issues')
    print(len(cur))
    for c in cur:
        isold = True
        # Try to find a matching record
        for r in records:
            if matching_issues(r, c):
                isold = False
                _id = c['redcap_repeat_instance']
                _proj = c['main_name']
                logging.debug(f'matches existing issue:{_proj}:{_id}')
                break

        if isold:
            # Append to list as closed with current time
            _proj = c['main_name']
            _id = c['redcap_repeat_instance']
            logging.debug(f'found old issue:{_proj}:{_id}')
            old_issues.append({
                'main_name': _proj,
                'redcap_repeat_instrument': c['redcap_repeat_instrument'],
                'redcap_repeat_instance': _id,
                'issues_complete': 2,
                'issue_closedate': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

    # Handle old issues
    if has_errors:
        logging.info(f'errors during audit, not closing old issues')
    elif old_issues:
        # Close old issues
        logging.info(f'setting {len(old_issues)} old issues to complete')
        try:
            response = project.import_records(old_issues)
            assert 'count' in response
            logging.info('issues successfully completed')
        except AssertionError as err:
            logging.error(f'failed to set issues to complete:{err}')

        # Delete old completed issues
        delete_old_issues(project, project_filter)

    else:
        logging.info('no old issues to complete')

