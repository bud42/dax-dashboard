import logging
import io
import re
import itertools
import os
import os.path
from datetime import datetime, date, timedelta
import time
import tempfile

import humanize
import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash
import plotly.express as px
from fpdf import FPDF
from PIL import Image

from app import app
from shared import ASTATUS2COLOR, QASTATUS2COLOR
import admin.data as data
from stats.data import get_variables
from qa.gui import get_metastatus


class MYPDF(FPDF):
    def set_filename(self, filename):
        self.filename = filename

    def set_project(self, project):
        self.project = project
        today = datetime.now().strftime("%Y-%m-%d")
        self.date = today
        self.title = '{} Monthly Report'.format(project)
        self.subtitle = '{}'.format(datetime.now().strftime("%B %Y"))

    def footer(self):
        self.set_y(-0.35)
        self.set_x(0.5)

        # Write date, title, page number
        self.set_font('helvetica', size=10)
        self.set_text_color(100, 100, 100)
        self.set_draw_color(100, 100, 100)
        self.line(x1=0.2, y1=10.55, x2=8.3, y2=10.55)
        self.cell(w=1, txt=self.date)
        self.cell(w=5, align='C', txt=self.title)
        self.cell(w=2.5, align='C', txt=str(self.page_no()))


def blank_letter():
    p = MYPDF(orientation="P", unit='in', format='letter')
    p.set_top_margin(0.5)
    p.set_left_margin(0.5)
    p.set_right_margin(0.5)
    p.set_auto_page_break(auto=False, margin=0.5)

    return p


def draw_counts(pdf, sessions, rangetype=None):
    # Counts of each session type with sums
    # sessions column names are: SESSION, PROJECT, DATE, SESSTYPE, SITE
    type_list = sessions.SESSTYPE.unique()
    site_list = sessions.SITE.unique()

    # Get the data
    df = sessions.copy()

    if rangetype == 'lastmonth':
        pdf.set_fill_color(114, 172, 77)

        # Get the dates of lst month
        _end = date.today().replace(day=1) - timedelta(days=1)
        _start = date.today().replace(day=1) - timedelta(days=_end.day)

        # Get the name of last month
        lastmonth = _start.strftime("%B")

        # Filter the data to last month
        df = df[df.DATE >= _start.strftime('%Y-%m-%d')]
        df = df[df.DATE <= _end.strftime('%Y-%m-%d')]

        # Create the lastmonth header
        _txt = 'Session Counts ({})'.format(lastmonth)

    else:
        pdf.set_fill_color(94, 156, 211)
        _txt = 'Session Counts (all)'

    # Draw heading
    pdf.set_font('helvetica', size=18)
    pdf.cell(w=7.5, h=0.5, txt=_txt, align='C', border=0, ln=1)

    # Header Formatting
    pdf.cell(w=1.0)
    pdf.set_text_color(245, 245, 245)
    pdf.set_line_width(0.01)
    _kwargs = {'w': 1.2, 'h': 0.7, 'border': 1, 'align': 'C', 'fill': True}
    pdf.cell(w=0.7, border=0, fill=False)

    # Column header for each session type
    for cur_type in type_list:
        pdf.cell(**_kwargs, txt=cur_type)

    # Got to next line
    pdf.ln()

    # Row formatting
    pdf.set_fill_color(255, 255, 255)
    pdf.set_text_color(0, 0, 0)
    _kwargs = {'w': 1.2, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}
    _kwargs_site = {'w': 1.7, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}

    # Row for each site
    for cur_site in site_list:
        dfs = df[df.SITE == cur_site]
        _txt = cur_site

        pdf.cell(**_kwargs_site, txt=_txt)

        # Count each type for this site
        for cur_type in type_list:
            cur_count = str(len(dfs[dfs.SESSTYPE == cur_type]))
            pdf.cell(**_kwargs, txt=cur_count)

        # Total for site
        cur_count = str(len(dfs))
        pdf.cell(**_kwargs, txt=cur_count)
        pdf.ln()

    # TOTALS row
    pdf.cell(w=1.0)
    pdf.cell(w=0.7, h=0.5)
    for cur_type in type_list:
        pdf.set_font('helvetica', size=18)
        cur_count = str(len(df[df.SESSTYPE == cur_type]))
        pdf.cell(**_kwargs, txt=cur_count)

    pdf.cell(**_kwargs, txt=str(len(df)))

    pdf.ln()

    return pdf


def plot_timeline(df, startdate=None, enddate=None):
    palette = itertools.cycle(px.colors.qualitative.Plotly)
    type_list = df.SESSTYPE.unique()
    mod_list = df.MODALITY.unique()
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    for mod, sesstype in itertools.product(mod_list, type_list):
        # Get subset for this session type
        dfs = df[(df.SESSTYPE == sesstype) & (df.MODALITY == mod)]
        if dfs.empty:
            continue

        # Advance color here, before filtering by time
        _color = next(palette)

        if startdate:
            dfs = dfs[dfs.DATE >= startdate.strftime('%Y-%m-%d')]

        if enddate:
            dfs = dfs[dfs.DATE <= enddate.strftime('%Y-%m-%d')]

        # Nothing to plot so go to next session type
        if dfs.empty:
            logging.debug('nothing to plot:{}:{}'.format(mod, sesstype))
            continue

        # markers symbols, see https://plotly.com/python/marker-style/
        if mod == 'MR':
            symb = 'circle-dot'
        elif mod == 'PET':
            symb = 'diamond-wide-dot'
        else:
            symb = 'diamond-tall-dot'

        # Convert hex to rgba with alpha of 0.5
        if _color.startswith('#'):
            _rgba = 'rgba({},{},{},{})'.format(
                int(_color[1:3], 16),
                int(_color[3:5], 16),
                int(_color[5:7], 16),
                0.7)
        else:
            _r, _g, _b = _color[4:-1].split(',')
            _a = 0.7
            _rgba = 'rgba({},{},{},{})'.format(_r, _g, _b, _a)

        # Plot this session type
        try:
            _row = 1
            _col = 1
            fig.append_trace(
                go.Box(
                    name='{} {} ({})'.format(sesstype, mod, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs['SITE'],
                    boxpoints='all',
                    jitter=0.7,
                    text=dfs['SESSION'],
                    pointpos=0.5,
                    orientation='h',
                    marker={
                        'symbol': symb,
                        'color': _rgba,
                        'size': 12,
                        'line': dict(width=2, color=_color)
                    },
                    line={'color': 'rgba(0,0,0,0)'},
                    fillcolor='rgba(0,0,0,0)',
                    hoveron='points',
                ),
                _row,
                _col)
        except Exception as err:
            logging.error(err)
            return None

    # show lines so we can better distinguish categories
    fig.update_yaxes(showgrid=True)

    # Set the size
    fig.update_layout(width=900)

    # Export figure to image
    _png = fig.to_image(format="png")
    image = Image.open(io.BytesIO(_png))
    return image


def plot_activity(df, pivot_index):
    status2rgb = ASTATUS2COLOR

    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Draw bar for each status, these will be displayed in order
    dfp = pd.pivot_table(
        df, index=pivot_index, values='LABEL', columns=['STATUS'],
        aggfunc='count', fill_value=0)

    for status, color in status2rgb.items():
        ydata = sorted(dfp.index)
        if status not in dfp:
            xdata = [0] * len(dfp.index)
        else:
            xdata = dfp[status]

        fig.append_trace(go.Bar(
            x=ydata,
            y=xdata,
            name='{} ({})'.format(status, sum(xdata)),
            marker=dict(color=color),
            opacity=0.9), 1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=True, width=900)

    # Export figure to image
    _png = fig.to_image(format="png")
    image = Image.open(io.BytesIO(_png))
    return image


def add_page1(pdf, sessions):
    mr_sessions = sessions[sessions.MODALITY == 'MR'].copy()

    # Start the page with titles
    pdf.add_page()
    pdf.set_font('helvetica', size=22)
    pdf.cell(w=7.5, h=0.4, align='C', txt=pdf.title, ln=1)
    pdf.cell(w=7.5, h=0.4, align='C', txt=pdf.subtitle, ln=1, border='B')
    pdf.ln(0.25)

    # Show all MRI session counts
    pdf.set_font('helvetica', size=18)
    pdf.cell(w=7.5, h=0.4, align='C', txt='MRI')
    pdf.ln(0.25)
    draw_counts(pdf, mr_sessions)
    pdf.ln(1)

    if len(mr_sessions.SITE.unique()) > 3:
        # Start a new page so it fits
        pdf.add_page()

    # Show MRI session counts in date range
    pdf.cell(w=7.5, h=0.4, align='C', txt='MRI')
    pdf.ln(0.25)
    draw_counts(pdf, mr_sessions, rangetype='lastmonth')
    pdf.ln(1)

    return pdf


def add_other_page(pdf, sessions):
    # Get non-MRI sessions
    other_sessions = sessions[sessions.MODALITY != 'MR'].copy()

    if len(other_sessions) == 0:
        logging.debug('no other modalities sessions, skipping page')
        return

    # Start a new page
    pdf.add_page()

    # Show all session counts
    pdf.set_font('helvetica', size=18)
    pdf.cell(w=7.5, h=0.4, align='C', txt='Other Modalities')
    pdf.ln(0.25)
    draw_counts(pdf, other_sessions)
    pdf.ln(1)

    # Show session counts in date range
    pdf.cell(w=7.5, h=0.4, align='C', txt='Other Modalities')
    pdf.ln(0.25)
    draw_counts(pdf, other_sessions, rangetype='lastmonth')
    pdf.ln(1)

    return pdf


def add_stats_page(pdf, stats, proctype):
    # 4 across, 3 down

    pdf.add_page()
    pdf.set_font('helvetica', size=18)
    pdf.cell(txt=proctype)

    # Limit the data to this proctype
    stats = stats.copy()
    stats = stats[stats.TYPE == proctype]

    # this returns a PIL Image object
    image = plot_stats(stats, proctype)

    # Split the image into chunks that fit on a letter page
    # crop((left, top, right, bottom))
    _img1 = image.crop((0, 0, 1000, 500))
    _img2 = image.crop((1000, 0, 2000, 500))
    _img3 = image.crop((2000, 0, 2500, 500))

    pdf.set_fill_color(114, 172, 77)

    # Draw the images on the pdf
    pdf.image(_img1, x=0.5, y=0.75, h=3.3)
    pdf.image(_img2, x=0.5, y=4, h=3.3)
    pdf.image(_img3, x=0.5, y=7.25, h=3.3)

    return pdf


def add_qa_page(pdf, scandata, assrdata, sesstype):
    scan_image = plot_qa(scandata)
    assr_image = plot_qa(assrdata)

    if not scan_image and not assr_image:
        # skip this page b/c there's nothing to plot
        logging.debug('skipping page, nothing to plot:{}'.format(sesstype))
        return pdf

    pdf.add_page()
    pdf.set_font('helvetica', size=18)
    pdf.ln(0.5)
    pdf.cell(w=5, align='C', txt='Scans by Type ({} Only)'.format(sesstype))

    if scan_image:
        pdf.image(scan_image, x=0.5, y=1.3, w=7.5)
        pdf.ln(4.7)

    if assr_image:
        pdf.cell(w=5, align='C', txt='Assessors by Type ({} Only)'.format(sesstype))
        pdf.image(assr_image, x=0.5, y=6, w=7.5)

    return pdf


def add_timeline_page(pdf, info):
    # Get the data for all
    df = info['sessions'].copy()

    pdf.add_page()
    pdf.set_font('helvetica', size=18)

    # Draw all timeline
    _txt = 'Sessions Timeline (all)'
    pdf.cell(w=7.5, align='C', txt=_txt)
    image = plot_timeline(df)
    pdf.image(image, x=0.5, y=0.75, w=7.5)
    pdf.ln(5)

    # Get the dates of last month
    enddate = date.today().replace(day=1) - timedelta(days=1)
    startdate = date.today().replace(day=1) - timedelta(days=enddate.day)

    # Get the name of last month
    lastmonth = startdate.strftime("%B")

    _txt = 'Sessions Timeline ({})'.format(lastmonth)
    image = plot_timeline(df, startdate=startdate, enddate=enddate)
    pdf.cell(w=7.5, align='C', txt=_txt)
    pdf.image(image, x=0.5, y=5.75, w=7.5)
    pdf.ln()

    return pdf


def add_phantom_page(pdf, info):
    # Get the data for all
    df = info['phantoms'].copy()

    pdf.add_page()
    pdf.set_font('helvetica', size=18)

    # Draw all timeline
    _txt = 'Phantoms (all)'
    pdf.cell(w=7.5, align='C', txt=_txt)
    image = plot_timeline(df)
    pdf.image(image, x=0.5, y=0.75, w=7.5)
    pdf.ln(5)

    # Get the dates of last month
    enddate = date.today().replace(day=1) - timedelta(days=1)
    startdate = date.today().replace(day=1) - timedelta(days=enddate.day)

    # Get the name of last month
    lastmonth = startdate.strftime("%B")

    _txt = 'Phantoms ({})'.format(lastmonth)
    image = plot_timeline(df, startdate=startdate, enddate=enddate)
    pdf.cell(w=7.5, align='C', txt=_txt)
    pdf.image(image, x=0.5, y=5.75, w=7.5)
    pdf.ln()

    return pdf


def add_activity_page(pdf, info):
    # 'index', 'SESSION', 'SUBJECT', 'ASSR', 'JOBDATE', 'QCSTATUS',
    #   'session_ID', 'PROJECT', 'PROCSTATUS', 'xsiType', 'PROCTYPE',
    #   'QCDATE', 'DATE', 'QCBY', 'LABEL', 'CATEGORY', 'STATUS', 'SOURCE',
    #   'DESCRIPTION', 'DATETIME', 'ID'],
    pdf.add_page()
    pdf.set_font('helvetica', size=16)

    df = info['activity'].copy()
    df = df[df.SOURCE == 'qa']
    image = plot_activity(df, 'CATEGORY')
    pdf.image(image, x=1.6, y=0.2, h=3.3)
    pdf.ln(0.5)
    pdf.multi_cell(1.5, 0.3, txt='QA\n')

    df = info['activity'].copy()
    df = df[df.SOURCE == 'dax']
    image = plot_activity(df, 'CATEGORY')
    pdf.image(image, x=1.6, y=3.5, h=3.3)
    pdf.ln(3)
    pdf.multi_cell(1.5, 0.3, txt='Jobs\n')

    df = info['activity'].copy()
    df = df[df.SOURCE == 'ccmutils']
    image = plot_activity(df, 'CATEGORY')
    pdf.image(image, x=1.6, y=7, h=3.3)
    pdf.ln(3)
    pdf.multi_cell(1.5, 0.3, txt='Other\nActivity\n&\nIssues\nthis month')

    return pdf


def plot_qa(dfp):
    # TODO: fix the code in this function b/c it's weird with the pivots/melts

    for col in dfp.columns:
        if col in ('SESSION', 'PROJECT', 'DATE', 'MODALITY'):
            # don't mess with these columns
            continue

        # Change each value from the multiple values in concatenated
        # characters to a single overall status
        dfp[col] = dfp[col].apply(get_metastatus)

    # Initialize a figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Check for empty data
    if len(dfp) == 0:
        logging.debug('dfp empty data')
        return None

    # use pandas melt function to unpivot our pivot table
    df = pd.melt(
        dfp,
        id_vars=(
            'SESSION',
            'PROJECT',
            'DATE',
            'SITE',
            'SESSTYPE',
            'MODALITY'),
        value_name='STATUS')

    # Check for empty data
    if len(df) == 0:
        logging.debug('df empty data')
        return None

    # We use fill_value to replace nan with 0
    dfpp = df.pivot_table(
        index='TYPE',
        columns='STATUS',
        values='SESSION',
        aggfunc='count',
        fill_value=0)

    scan_type = []
    assr_type = []
    for cur_type in dfpp.index:
        # Use a regex to test if name ends with _v and a number, then assr
        if re.search('_v\d+$', cur_type):
            assr_type.append(cur_type)
        else:
            scan_type.append(cur_type)

    newindex = scan_type + assr_type
    dfpp = dfpp.reindex(index=newindex)

    # Draw bar for each status, these will be displayed in order
    # ydata should be the types, xdata should be count of status
    # for each type
    for cur_status, cur_color in QASTATUS2COLOR.items():
        ydata = dfpp.index
        if cur_status not in dfpp:
            xdata = [0] * len(dfpp.index)
        else:
            xdata = dfpp[cur_status]

        cur_name = '{} ({})'.format(cur_status, sum(xdata))

        fig.append_trace(
            go.Bar(
                x=ydata,
                y=xdata,
                name=cur_name,
                marker=dict(color=cur_color),
                opacity=0.9),
            1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=True, width=900)

    # Export figure to image
    _png = fig.to_image(format="png")
    image = Image.open(io.BytesIO(_png))

    return image


def plot_stats(df, proctype):
    box_width = 250
    min_box_count = 4

    logging.info('plot_stats:{}'.format(proctype))

    # Check for empty data
    if len(df) == 0:
        logging.debug('empty data, using empty figure')
        fig = go.Figure()
        _png = fig.to_image(format="png")
        image = Image.open(io.BytesIO(_png))
        return image

    # Filter var list to only include those that have data
    var_list = [x for x in df.columns if not pd.isnull(df[x]).all()]

    # Filter var list to only stats variables
    var_list = [x for x in get_variables() if x in var_list]

    # Determine how many boxplots we're making, depends on how many vars, use
    # minimum so graph doesn't get too small
    box_count = len(var_list)
    if box_count < min_box_count:
        box_count = min_box_count

    graph_width = box_width * box_count

    # Horizontal spacing cannot be greater than (1 / (cols - 1))
    hspacing = 1 / (box_count * 4)

    # Make the figure with 1 row and a column for each var we are plotting
    fig = plotly.subplots.make_subplots(
        rows=1,
        cols=box_count,
        horizontal_spacing=hspacing,
        subplot_titles=var_list)

    # Add box plot for each variable
    for i, var in enumerate(var_list):
        logging.debug('plotting var:{}'.format(var))

        _row = 1
        _col = i + 1
        # Create boxplot for this var and add to figure
        fig.append_trace(
            go.Box(
                y=df[var],
                x=df['SITE'],
                boxpoints='all',
                text=df['assessor_label']),
            _row,
            _col)

        if var.startswith('con_') or var.startswith('inc_'):
            logging.debug('setting beta range:{}'.format(var))
            _yaxis = 'yaxis{}'.format(i + 1)
            fig['layout'][_yaxis].update(range=[-1, 1], autorange=False)
        else:
            logging.debug('setting autorange')

    # Move the subtitles to bottom instead of top of each subplot
    for i in range(len(fig.layout.annotations)):
        fig.layout.annotations[i].update(y=-.15)

    # Customize figure to hide legend and fit the graph
    fig.update_layout(
        showlegend=False,
        autosize=False,
        width=graph_width,
        margin=dict(l=20, r=40, t=40, b=80, pad=0))

    _png = fig.to_image(format="png")
    image = Image.open(io.BytesIO(_png))
    return image


def make_pdf(info, filename):
    logging.debug('making PDF')

    # Initialize a new PDF letter size and shaped
    pdf = blank_letter()
    pdf.set_filename(filename)
    pdf.set_project(info['project'])

    # Add first page showing MRIs
    logging.debug('adding first page')
    add_page1(pdf, info['sessions'])

    # Add other Modalities, counts for each session type
    logging.debug('adding other page')
    add_other_page(pdf, info['sessions'])

    # Timeline
    logging.debug('adding timeline page')
    add_timeline_page(pdf, info)

    # Session type pages - counts per scans, counts per assessor
    logging.debug('adding qa pages')
    for curtype in info['sessions'].SESSTYPE.unique():
        logging.info('add_qa_page:{}'.format(curtype))

        # Get the scan and assr data
        scandf = info['scanqa'].copy()
        assrdf = info['assrqa'].copy()

        # Limit to the current session type
        scandf = scandf[scandf.SESSTYPE == curtype]
        assrdf = assrdf[assrdf.SESSTYPE == curtype]

        # Drop columns that are all empty
        scandf = scandf.dropna(axis=1, how='all')
        assrdf = assrdf.dropna(axis=1, how='all')

        # Add the page for this session type
        add_qa_page(pdf, scandf, assrdf, curtype)

    # Add stats pages
    if info['stats'].empty:
        logging.debug('no stats')
    else:
        for stat in info['stattypes']:
            logging.info('add stats page:{}'.format(stat))
            add_stats_page(pdf, info['stats'], stat)

    # Phantom pages
    if 'phantoms' in info:
        logging.debug('adding phantom page')
        add_phantom_page(pdf, info)

        # QA/Jobs/Issues counts
    add_activity_page(pdf, info)

    # Save to file
    logging.debug('saving PDF to file:{}'.format(pdf.filename))
    try:
        pdf.output(pdf.filename)
    except Exception as err:
        logging.error('error while saving PDF:{}:{}'.format(pdf.filename, err))

    return True


def make_main_report():
    # last week
    # show counts from last week

    # show issue counts

    # previous week

    # previous month

    # previous year

    # Note that all of these can be opened interactively in dashboard

    return


def make_project_report(
    filename,
    project,
    scantypes=[],
    assrtypes=[],
    stattypes=[],
    xsesstypes=[],
    phantom_project=None
):

    results = []

    # stattypes: list of proc types to show stats
    # scantypes: list of scan types to include
    # assrtypes: list of assessor types to include

    info = {}
    info['project'] = project
    info['stattypes'] = stattypes
    info['scantypes'] = scantypes
    info['assrtypes'] = assrtypes

    # Load the data
    info['sessions'] = data.load_session_info(project).sort_values('SESSION')
    info['activity'] = data.load_activity_info(project)
    info['stats'] = data.load_stats(project, stattypes)
    info['scanqa'] = data.load_scanqa_info(project, info['scantypes'])
    info['assrqa'] = data.load_assrqa_info(project, info['assrtypes'])
    if phantom_project:
        info['phantoms'] = data.load_phantom_info(phantom_project)

    # Exclude sessions of specified types
    info['sessions'] = info['sessions'][~info['sessions'].SESSTYPE.isin(
        xsesstypes)]

    # Make the pdf based on loaded info
    success = make_pdf(info, filename)
    if success:
        results.append({
            'result': 'COMPLETE',
            'type': 'update_report',
            'project': project})

    return results

