from logs import logger

from dax import XnatUtils

import utils


# we want to audit all the mri data form in redcap
# start from the end result which is an mri session with data processed, 
# including fmri with eprime data. then what can go wrong at each step back to 
# the scanner then reverse it!


def audit(
    project,
    events,
    date_field,
    src_sess_field,
    dst_sess_field,
    src_project_name,
    dst_project_name,
    use_secondary=False,
    event2sess=None
):
    results = []
    def_field = project.def_field
    fields = [def_field, date_field, src_sess_field, dst_sess_field]
    id2subj = {}

    # TODO: compare date

    with XnatUtils.InterfaceTemp(xnat_retries=0) as xnat:

        # check that projects exist on XNAT
        if not xnat.select.project(src_project_name).exists():
            msg = 'source project does not exist in XNAT:' + src_project_name
            logger.error(msg)
            results.append({
                'type': 'ERROR',
                'description': msg})
            return results

        # check that projects exist on XNAT
        if not xnat.select.project(dst_project_name).exists():
            msg = 'destination project does not exist:' + dst_project_name
            logger.error(msg)
            results.append({
                'type': 'ERROR',
                'description': msg})
            return results

        logger.info('loading session information from XNAT')
        dst_sess_list = utils.session_label_list(xnat, dst_project_name)
        src_sess_list = utils.session_label_list(xnat, src_project_name)

        if use_secondary:
            # Handle secondary ID
            sec_field = project.export_project_info()['secondary_unique_field']
            if not sec_field:
                logger.error('secondary enabled but no secondary field found')
                return

            rec = project.export_records(fields=[def_field, sec_field])
            id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

        # Get mri records from redcap
        rec = project.export_records(fields=fields, events=events)

        # Process each record
        for r in rec:
            record_id = r[def_field]
            if 'redcap_event_name' in r:
                event_id = r['redcap_event_name']
            else:
                event_id = 'None'

            # Get the source labels
            if '_' in r[src_sess_field]:
                # Ignore PI prefix if present
                src_sess = r[src_sess_field].split('_')[1]
            else:
                src_sess = r[src_sess_field]

            src_date = r[date_field]

            # Get the destination labels
            if use_secondary:
                try:
                    dst_subj = id2subj[record_id]
                except KeyError as err:
                    logger.debug(f'record without subject number:{err}')
                    continue
            else:
                dst_subj = record_id

            if event2sess is not None:
                # Check if event2sess is not none, then get destination session
                # label by mapping event 2 session and then concatenate with
                # subject
                try:
                    suffix = event2sess[event_id]
                    dst_sess = dst_subj + suffix
                except KeyError as err:
                    logger.error('{}:{}:{}:{}'.format(
                        record_id, event_id,
                        'failed to map event to session suffix:', str(err)))
                    continue
            elif dst_sess_field:
                dst_sess = r[dst_sess_field]
            else:
                logger.info('{}:{}:{}'.format(
                    record_id, event_id, 'failed to get session ID'))
                continue

            # Ignore other fields if destination session exists on XNAT
            # this helps with scans from other sites where some fields
            # are not used
            if dst_sess in dst_sess_list:
                logger.debug('{}:{}'.format(dst_sess, 'already on XNAT'))
                continue

            if not src_sess:
                # TODO: are we ok ignoring this scenario?
                # his means we would not catch the case where the coordinator
                # forgets to enter the source session. we woud eventually
                # catch it when the scan shows up in XNAT. We need
                # be extra vigilant when we know scans are not
                # automatically going to XNAT for whatever reason.
                msg = '{}:{}:{}'.format(
                    record_id, event_id, 'source session not set')
                logger.debug(msg)
                # results.append({
                #    'type': 'MISSING_VALUE',
                #    'subject': dst_subj,
                #    'session': dst_sess,
                #    'event': event_id,
                #    'description': msg})
                continue

            # Check for missing values
            if not dst_sess:
                msg = '{}:{}:{}'.format(
                    record_id, event_id, 'XNAT session ID not set')
                logger.info(msg)
                results.append({
                    'type': 'MISSING_VALUE',
                    'subject': dst_subj,
                    'event': event_id,
                    'date': src_date,
                    'description': msg})
                continue

            if not src_date:
                # TODO: are we ok ignoring this scenario?
                # his means we would not catch the case where the coordinator
                # forgets to enter the session date. we woud eventually
                # catch it when the scan shows up in XNAT. We need to
                # be extra vigilant when we know scans are not
                # automatically going to XNAT for whatever reason.
                msg = '{}:{}:{}'.format(record_id, event_id, 'date not set')
                logger.info(msg)
                # results.append({
                #    'type': 'MISSING_VALUE',
                #    'subject': dst_subj,
                #    'session': dst_sess,
                #    'event': event_id,
                #    'description': msg})
                continue

            # Check that session does actually exist in source project
            if src_sess not in src_sess_list:
                msg = '{}:{}'.format(src_sess, 'not on XNAT request repush?')
                logger.info(msg)
                results.append({
                    'type': 'MISSING_SESSION',
                    'subject': dst_subj,
                    'session': dst_sess,
                    'event': event_id,
                    'date': src_date,
                    'description': msg})
                continue

            # Add issue that auto archive needs to run?
            if dst_sess not in dst_sess_list:
                msg = '{}_{}:{}'.format(
                    src_project_name, src_sess, 'auto archive working?')
                logger.info(msg)
                results.append({
                    'type': 'NEEDS_AUTO',
                    'subject': dst_subj,
                    'session': dst_sess,
                    'event': event_id,
                    'date': src_date,
                    'description': msg})
                continue

    return results
