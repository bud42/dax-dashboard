import logging


def audit(
    project,
    events,
    raw_field,
    tab_field,
    ready_field,
    use_secondary=False,
):
    results = []

    def_field = project.def_field
    fields = [def_field, raw_field, tab_field, ready_field]
    id2subj = {}

    if use_secondary:
        # Handle secondary ID
        sec_field = project.export_project_info()['secondary_unique_field']
        if not sec_field:
            logging.error('secondary enabled, but no secondary field found')
            return

        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get mri records
    rec = project.export_records(fields=fields, events=events)

    # Process each record
    for r in rec:
        record_id = r[def_field]
        event = r['redcap_event_name']
        if use_secondary:
            try:
                subj = id2subj[record_id]
            except KeyError as err:
                logging.debug(f'record without subject number:{err}')
                continue
        else:
            subj = record_id

        # Make visit name for logging
        visit = '{}:{}'.format(subj, event)

        # Skip if not ready
        if not r[ready_field]:
            logging.debug(visit + ':not ready yet')
            continue

        # Check for edat file
        if not r[raw_field]:
            # Missing edat
            logging.info(visit + ':missing edat')
            results.append({
                'type': 'MISSING_EDAT',
                'subject': subj,
                'event': event,
                'field': raw_field,
                'description': 'need to run edat2tab or manually upload'})
            continue

        # Check for missing data flag
        if 'MISSING_DATA' in r[raw_field]:
            logging.debug('{}:{}:{}'.format(subj, event, 'missing data flag'))
            continue

        # Check for converted edat file
        if not r[tab_field]:
            # Missing converted edat
            logging.info(visit + ':missing converted edat')
            results.append({
                'type': 'MISSING_CONVERTED_EDAT',
                'subject': subj,
                'event': event,
                'field': tab_field,
                'description': 'need to check converter vm'})
            continue

        # TODO: Check for edat on xnat

        # TODO: Check for etl

        # TODO: compare date in EDAT to XNAT

    return results
