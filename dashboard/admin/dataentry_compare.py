import os
import sys
import shutil
import logging
from datetime import datetime
import tempfile

import pandas as pd
from fpdf import FPDF
import redcap

import utils


# Compares two REDCap projects, as First and Second where Second is the
# subset of fields that should be double-entered. Outputs a PDF file
# and an excel file. The PDF provides an overview/summary of the Excel
# file. The excel file contains the specific missing and conflicting items.
# Output files are named with the specified prefix.


# Names and descriptions of the sheets in the excel output file
SHEETS = [{
'name': 'Mismatches',
'description': 'Mismatches is the list of discrepancies between the First \
and Second REDCap projects with one row per mismatch.'''
},
{
'name': 'MissingSubjects',
'description': 'MissingSubjects is the list of subjects that are found in \
the First REDCap project but are completely missing from the Second.'
},
{
'name': 'MissingEvents',
'description': 'MissingEvents is the list of subject events that are found \
in the First REDCap project, but are completely missing from the Second.'
},
{
'name': 'MissingValues',
'description': 'MissingEvents is the list of values that are found \
in the First REDCap project, but are blank in the Second.'
},
{
'name': 'FieldsCompare',
'description': 'FieldsCompare is the list of fields that are INCLUDED for \
comparison. This list includes all fields in FieldsCommon excluding those \
in Fields2ndNan.'
},
{
'name': 'FieldsCommon',
'description': 'FieldsCommon is the list of fields that are found in both \
First and Second REDCap projects.'
},
{
'name': 'Fields1stOnly',
'description': 'Fields1stOnly is the list of fields that are found only in \
the First REDCap. These fields are EXCLUDED from comparisons.'
},
{
'name': 'Fields2ndOnly',
'description': 'Fields2ndOnly is the list of fields that are found only in \
the Second REDCap project. This should be an empty list.'
},
{
'name': 'Fields2ndNan',
'description': 'Fields2ndNan is the list of fields that are found in both \
REDCap projects, but all values are blank in the Second REDCap. This list \
should be empty.'
}]


# Our custom PDF file format
class MYPDF(FPDF):
    def footer(self):
        today = datetime.now().strftime("%Y-%m-%d")
        self.date = today
        self.title = 'Double Entry'
        self.subtitle = '{}'.format(datetime.now().strftime("%B %Y"))

        self.set_y(-0.35)
        self.set_x(0.5)

        # Write date, title, page number
        self.set_font('helvetica', size=10)
        self.set_text_color(100, 100, 100)
        self.set_draw_color(100, 100, 100)
        self.line(x1=0.2, y1=10.55, x2=8.3, y2=10.55)
        self.cell(w=1, txt=self.date)
        self.cell(w=5, align='C', txt=self.title)
        self.cell(w=2.5, align='C', txt=f'{self.page_no()} of {{nb}}')


def make_pdf(results, filename):
    logging.debug('making PDF')

    # Initialize a new PDF letter size and shaped
    pdf = MYPDF(orientation="P", unit='in', format='letter')
    pdf.add_page()

    # Give it a title at the top
    title = 'Double Data Entry Comparison'
    pdf.set_font('helvetica', size=18)
    pdf.cell(w=7, h=0.5, txt=title, border=0, ln=1)

    # Iterate the heading section
    for key, val in results['session'].items():
        pdf.set_font('helvetica', size=13)
        pdf.cell(w=1.3, h=.6, txt=key, border=0)
        pdf.set_font('courier', size=14)
        pdf.cell(w=6, h=.6, txt=val, border=1, ln=1)

    # Show results counts section
    counts = results['counts']

    pdf.ln(0.1)
    pdf.set_font('helvetica', size=14)
    pdf.cell(1, 0.4, 'RESULTS:', ln=1)

    txt = 'Mismatches'
    val = str(counts['mismatches'])
    dsc = 'see Mismatches sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    txt = 'Missing Subjects'
    val = str(counts['missing_subjects'])
    dsc = 'see MissingSubjects sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    txt = 'Missing Events'
    val = str(counts['missing_events'])
    dsc = 'see MissingEvents sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    txt = 'Missing Values'
    val = str(counts['missing_values'])
    dsc = 'see MissingValues sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    pdf.ln(0.5)

    pdf.set_font('helvetica', size=10)
    _txt = 'The sheets in the excel file are:'
    pdf.cell(w=7.5, h=0.3, txt=_txt, border='T', ln=1)

    # Add sheet descriptions
    for s in SHEETS:
        add_sheet_description(pdf, s['name'], s['description'])

    # Save to file
    logging.info(f'saving PDF to file:{filename}')
    try:
        pdf.output(filename)
    except Exception as err:
        logging.error(f'error while saving PDF:{filename}:{err}')


def add_sheet_description(pdf, name, description):
    # Write the name and description to the PDF
    pdf.set_font(style='B', size=8)
    pdf.cell(w=1.2, h=0.4, txt=name, border=0)
    pdf.set_font(style='')
    pdf.multi_cell(w=6.3, h=0.4, txt=description, border='B', ln=1, align='L')


def get_fields(p1, p2):
    # Get all the records so we can check for all nan
    df = p2.export_records(format_type='df')

    common_fields = sorted(list(set(p1.field_names) & set(p2.field_names)))
    p1_only_fields = sorted(list(set(p1.field_names) - set(p2.field_names)))
    p2_only_fields = sorted(list(set(p2.field_names) - set(p1.field_names)))
    p2_used_fields = sorted(list((df.dropna(how='all', axis=1)).columns))
    p2_nan_fields = sorted(list(set(common_fields) - set(p2_used_fields)))
    compare_fields = sorted(list(set(common_fields) - set(p2_nan_fields)))

    fields = {
        'compare': compare_fields,
        'common': common_fields,
        'p1_only': p1_only_fields,
        'p2_only': p2_only_fields,
        'p2_nan': p2_nan_fields,
    }

    return fields


def write_sheet(data, writer, name):
    # Format as string to avoid formatting mess
    df = pd.DataFrame(data=data, dtype=str)

    # Sort by all columns in order
    df = df.sort_values(by=list(df.columns))

    # Write the dataframe to the excel file
    df.to_excel(writer, sheet_name=name, index=False)

    # Auto-adjust columns' width
    col_fmt = writer.book.add_format({'num_format': '@'})
    for column in df:
        col_width = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets[name].set_column(col_idx, col_idx, col_width, col_fmt)

    # Ignore errors
    _range = '{}{}:{}{}'.format('A', 1, 'F', len(df) + 1)
    writer.sheets[name].ignore_errors({'number_stored_as_text': _range})


def write_excel(info, outfile):
    # Write each sheet to excel
    with pd.ExcelWriter(outfile) as w:
        if info['mismatches']:
            write_sheet(info['mismatches'], w, 'Mismatches')

        if info['missing_subjects']:
            write_sheet(info['missing_subjects'], w, 'MissingSubjects')

        if info['missing_events']:
            write_sheet(info['missing_events'], w, 'MissingEvents')

        if info['missing_values']:
            write_sheet(info['missing_values'], w, 'MissingValues')

        write_sheet(info['fields']['common'], w, 'FieldsCommon')
        write_sheet(info['fields']['compare'], w, 'FieldsCompare')

        write_sheet(info['fields']['p1_only'], w, 'Fields1stOnly')
        write_sheet(info['fields']['p2_only'], w, 'Fields2ndOnly')
        write_sheet(info['fields']['p2_nan'], w, 'Fields2ndNan')


def compare_projects(p1, p2):
    # Compares two redcap projects and returns the results
    results = {}
    missing_subjects = []
    missing_events = []
    missing_values = []
    mismatches = []
    sec_field = None

    # Create index of record ID to subject ID in p1
    def_field = p1.def_field
    sec_field = p1.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = p1.export_records(fields=[def_field, sec_field])
        id2subj1 = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

        # Create index of subject ID to record ID in p2
        def_field = p2.def_field
        sec_field = p2.export_project_info()['secondary_unique_field']
        rec = p2.export_records(fields=[def_field, sec_field])
        subj2id2 = {x[sec_field]: x[def_field] for x in rec if x[sec_field]}

    # Determine which fields to compare
    fields = get_fields(p1, p2)
    compare_fields = fields['compare']

    # Get the records from the First project
    records1 = p1.export_records()

    # Compare each record
    for r1 in records1:
        rid1 = r1[def_field]
        eid = r1['redcap_event_name']

        # Get the subject number for this record
        if sec_field:
            try:
                sid = id2subj1[rid1]
            except KeyError as err:
                logging.debug(f'blank subject ID for record:{rid1}:{err}')
                continue
        else:
            # No secondary id, use main id
            sid = rid1

        # Check that we already found as missing
        if sid in missing_subjects:
            # Skip this subject, already missing
            logging.debug(f'subject already missing:{sid}')
            continue

        if (sid, eid) in missing_events:
            # Skip this event, already missing
            logging.debug(f'event already missing:{sid},{eid}')
            continue

        # Get id in the secondary redcap project
        if sec_field:
            # Find the record to compare in second db by
            # using subject id to find correct record by using and index
            # then we query by record id
            try:
                rid = subj2id2[sid]
            except KeyError as err:
                logging.debug(f'missing subject:{rid1}:{err}')
                missing_subjects.append(sid)
                continue
        else:
            # No secondary id, use main id
            rid = sid

        _rrname = r1.get('redcap_repeat_instrument', '')
        _rrnum = r1.get('redcap_repeat_instance', '')
        logging.debug(f'comparing:{sid},{eid},{_rrname},{_rrnum}')

        r1['sid'] = sid
        try:
            e2 = p2.export_records(
                records=[rid], events=[eid], fields=compare_fields)
            e2_count = len(e2)

            if e2_count == 0:
                logging.debug(f'No record in double:{rid}:{sid}:{eid}')
                missing_events.append((sid, eid))
                continue
            elif e2_count > 1:
                # Handle multiple records for event
                # Try to find a record
                # that completely matches and continue, if none are found
                # append the mismatches
                mism = misv = None
                for i in range(e2_count):
                    r2 = e2[i]
                    match_found = False
                    (mism, misv) = compare_records(r1, r2, compare_fields)
                    if not mism and not misv:
                        # we have an exact match, so we good, exit the loop
                        match_found = True
                        break

                if not match_found:
                    # Did not find perfect match
                    mismatches += mism
                    missing_values += misv

            else:
                assert(e2_count == 1)
                r2 = e2[0]
                (mism, misv) = compare_records(r1, r2, compare_fields)
                mismatches += mism
                missing_values += misv

        except Exception as err:
            logging.debug(f'missing event:{err}')
            missing_events.append((sid, eid))

    # Count results
    results['counts'] = {
        'missing_values': len(missing_values),
        'missing_events': len(missing_events),
        'missing_subjects': len(missing_subjects),
        'mismatches': len(mismatches),
    }

    # Convert subjects list of dicts, we do this here so we can keep
    # a simple list during the loop to check for already missing
    if missing_subjects:
        missing_subjects = [{'SUBJECT': s} for s in missing_subjects]
    else:
        missing_subjects = None

    # Convert events to list of dicts
    if missing_events:
        _keys = ['SUBJECT', 'EVENT']
        missing_events = [dict(zip(_keys, v)) for v in missing_events]
    else:
        missing_events = None

    # Append results
    results['missing_subjects'] = missing_subjects
    results['missing_events'] = missing_events
    results['missing_values'] = missing_values
    results['mismatches'] = mismatches

    # Append fields information
    results['fields'] = {}
    for k in ['compare', 'common', 'p1_only', 'p2_only', 'p2_nan']:
        if fields[k]:
            results['fields'][k] = [{'FIELD': v} for v in fields[k]]
        else:
            results['fields'][k] = [{'FIELDS': ''}]

    # Get project titles and ids
    p1_info = p1.export_project_info()
    p2_info = p2.export_project_info()
    name1 = '{} ({})'.format(p1_info['project_title'], p1_info['project_id'])
    name2 = '{} ({})'.format(p2_info['project_title'], p2_info['project_id'])

    # Build the info for pdf
    results['session'] = {
        'REDCap 1': name1,
        'REDCap 2': name2,
        'DATE': datetime.now().strftime("%Y-%m-%d"),
    }

    return results


def compare_records(r1, r2, fields, show_one_null=False, show_two_null=True):
    mismatches = []
    misvalues = []

    for k in sorted(fields):
        # Get value from the first redcap
        try:
            v1 = r1[k]
        except KeyError:
            logging.error(f'r1:KeyError:{k}')
            continue

        # Get the value from the second
        try:
            v2 = r2[k]
        except KeyError:
            logging.error(f'r2:KeyError:{k}')
            continue

        mis = {
            'SUBJECT': r1['sid'],
            'EVENT': r1['redcap_event_name'],
            'FIELD': k,
        }

        if 'redcap_repeat_instrument' in r1 and r1['redcap_repeat_instance']:
            mis['REPEAT_INSTANCE'] = r1['redcap_repeat_instance']

        if v1 == '':
            # Both blank
            if show_one_null:
                mismatches.append(mis)
        elif v2 == '':
            # First has value, Second is blank
            if show_two_null:
                misvalues.append(mis)

        elif str(v1).strip().lower() != str(v2).strip().lower():
            # Both have values, but don't match, show the values
            mis['1stVALUE'] = v1
            mis['2ndVALUE'] = v2
            mismatches.append(mis)
        else:
            # it matches, so do nothing
            pass

    return (mismatches, misvalues)


def write_results(results, pdf_file, excel_file):
    # Make the summary PDF
    make_pdf(results, pdf_file)

    # Save excel file with results
    write_excel(results, excel_file)


def match_repeat(mainrc, record_id, repeat_name, match_field, match_value):

    # Load potential matches
    records = mainrc.export_records(records=[record_id])

    # Find records with matching vaue
    matches = [x for x in records if x[match_field] == match_value]

    # Return ids of matches
    return [x['redcap_repeat_instance'] for x in matches]


def test_finish(outdir, outpref):
    info = {}
    excel_file = os.path.join(outdir, f'{outpref}.xlsx')
    pdf_file = os.path.join(outdir, f'{outpref}.pdf')

    info['session'] = {
        'REDCap 1': '1',
        'REDCap 2': '2',
        'DATE': datetime.now().strftime("%Y-%m-%d"),
    }

    info['counts'] = {
        'fields_compare': 0,
        'missing_events': 0,
        'missing_subjects': 0,
        'mismatches': 0,
        'missing_values': 0,
    }

    info['missing_subjects'] = []
    info['missing_events'] = []
    info['mismatches'] = []
    info['missing_values'] = []

    info['fields'] = {
        'compare': [{'FIELD': v} for v in []],
        'common': [{'FIELD': v} for v in []],
        'p1_only': [{'FIELD': v} for v in []],
        'p2_only': [{'FIELD': v} for v in []],
        'p2_nan': [{'FIELD': v} for v in []],
    }

    # Write results
    write_results(info, pdf_file, excel_file)


def run_compare(p1, p2, outdir, outpref):
    # Build filenames
    excel_file = os.path.join(outdir, f'{outpref}.xlsx')
    pdf_file = os.path.join(outdir, f'{outpref}.pdf')

    # Get the compare results
    results = compare_projects(p1, p2)

    # Write output files
    write_results(results, pdf_file, excel_file)


def run_project_compare(mainrc, proj_maindata, keyfile):
    proj_name = proj_maindata['main_name']
    proj_primary = proj_maindata['project_primary']
    proj_secondary = proj_maindata['project_secondary']

    if not proj_primary:
        logging.warning(f'cannot compare, primary id not set:{proj_name}')
        return

    if not proj_secondary:
        logging.warning(f'cannot compare, secondary id not set:{proj_name}')
        return

    # Get the projects to compare
    k1 = utils.get_projectkey(proj_primary, keyfile)
    k2 = utils.get_projectkey(proj_secondary, keyfile)
    p1 = redcap.Project(mainrc.url, k1)
    p2 = redcap.Project(mainrc.url, k2)

    # Create temp locations for result files
    with tempfile.TemporaryDirectory() as outdir:
        logging.info(f'created temporary directory:{outdir}')

        # Name the output files
        _today = datetime.now().strftime("%Y-%m-%d")
        outpref = f'{proj_name}_{_today}'

        # Run it
        logging.info(f'comparing {proj_name}:{proj_primary} to {proj_secondary}')
        run_compare(p1, p2, outdir, outpref)

        logging.info(f'handling results')

        # Build filenames
        excel_file = os.path.join(outdir, f'{outpref}.xlsx')
        pdf_file = os.path.join(outdir, f'{outpref}.pdf')

        # Add new record
        try:
            double_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record = {
               'double_datetime': double_datetime,
               'main_name': proj_name,
               'redcap_repeat_instrument': 'double',
               'redcap_repeat_instance': 'new',
               'double_complete': '2',
            }
            response = mainrc.import_records([record])
            assert 'count' in response
            logging.info('successfully created new record')

            # Get the new record id from the response
            logging.info('locating new record')
            _ids = match_repeat(mainrc, proj_name, 'double', 'double_datetime', double_datetime)
            repeat_id = _ids[0]

            # Upload output files
            logging.info(f'uploading files to:{repeat_id}')
            utils.upload_file(
                mainrc, proj_name, None, 'double_resultsfile', excel_file, repeat_id=repeat_id)
            utils.upload_file(
                mainrc, proj_name, None, 'double_resultspdf', pdf_file, repeat_id=repeat_id)

            # Save PDF to reports
            shutil.copy(pdf_file, 'assets/double/')


        except AssertionError as err:
            logging.error(f'upload failed:{err}')
        except (ValueError, redcap.RedcapError) as err:
            logging.error(f'error uploading:{err}')


def update_reports(mainrc, keyfile, project_filter):
    logging.info('running compare')
    maindata = mainrc.export_records()

    # Get list of projects
    proj_list = sorted(list(set([x['main_name'] for x in maindata])))

    # Now iterate each project
    for p in proj_list:
        if project_filter and p != project_filter:
            logging.debug(f'skipping project {p}')
            continue

        logging.info(f'comparing double entry for project:{p}')

        # Get main data for project
        proj_maindata = {}
        for rec in maindata:
            if rec['main_name'] == p and rec['redcap_repeat_instrument'] == '':
                proj_maindata = rec

        run_project_compare(mainrc, proj_maindata, keyfile)

    return
