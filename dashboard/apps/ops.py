import subprocess
from io import StringIO
from datetime import datetime, timedelta
import json
import os
import logging
import math

import humanize
import pandas as pd
import numpy as np
import plotly
import plotly.graph_objs as go
import plotly.subplots
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from dash.dependencies import Input, Output

from dax import XnatUtils

from app import app

from . import utils


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

pd.set_option('display.max_colwidth', None)

ASSR_URI = '/REST/experiments?xsiType=proc:genprocdata\
&columns=\
ID,\
label,\
project,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/jobstartdate,\
proc:genprocdata/memused,\
proc:genprocdata/walltimeused,\
proc:genprocdata/jobid'

ASSR_RENAME = {
    'ID': 'ID',
    'label': 'LABEL',
    'project': 'PROJECT',
    'proc:genprocdata/jobid': 'JOBID',
    'proc:genprocdata/jobstartdate': 'JOBSTARTDATE',
    'proc:genprocdata/memused': 'MEMUSED',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/walltimeused': 'WALLTIMEUSED'}

SQUEUE_USER = ['vuiis_archive_singularity', 'vuiis_daily_singularity']
UPLOAD_DIR = [
    '/scratch/vuiis_archive_singularity/Spider_Upload_Dir',
    '/scratch/vuiis_daily_singularity/Spider_Upload_Dir']
SQUEUE_CMD = 'squeue -u '+','.join(SQUEUE_USER)+' --format="%all"'
DFORMAT = '%Y-%m-%d %H:%M:%S'
XNAT_DFORMAT = '%m/%d/%Y'
TIMEZONE = 'US/Central'
XNAT_USER = 'boydb1'

# we concat diskq status and squeue status to make a single status
# squeue states: CG,F, PR, S, ST
# diskq statuses: JOB_RUNNING, JOB_FAILED, NEED_TO_RUN, COMPLETE,
# UPLOADING, READY_TO_COMPLETE, READY_TO_UPLOAD
STATUS_MAP = {
    'COMPLETENONE': 'COMPLETE',
    'JOB_FAILEDNONE': 'FAILED',
    'JOB_RUNNINGCD': 'RUNNING',
    'JOB_RUNNINGCG': 'RUNNING',
    'JOB_RUNNINGF': 'RUNNING',
    'JOB_RUNNINGR': 'RUNNING',
    'JOB_RUNNINGNONE': 'RUNNING',
    'JOB_RUNNINGPD': 'PENDING',
    'NONENONE': 'WAITING',
    'READY_TO_COMPLETENONE': 'COMPLETE',
    'READY_TO_UPLOADNONE': 'COMPLETE'}

RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELLOW = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_PURPLE = 'rgb(160,106,255)'
RGB_GREY = 'rgb(200,200,200)'

HEX_LBLUE = '#DAEBFF'
HEX_LGREE = '#DCFFDA'
HEX_LYELL = '#FFE4B3'
HEX_LREDD = '#FFDADA'
HEX_LGREY = '#EBEBEB'
HEX_LPURP = '#D1C0E5'

STATUS_LIST = ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN']
COLOR_LIST = [RGB_GREY, RGB_YELLOW, RGB_GREEN, RGB_BLUE, RGB_RED, RGB_PURPLE]
LCOLOR_LIST = [HEX_LGREY, HEX_LYELL, HEX_LGREE, HEX_LBLUE, HEX_LREDD, HEX_LPURP]

STATUS2COLOR = {
    'COMPLETE': 'rgba(0,255,0,0.5)',
    'JOB_FAILED': 'rgba(255,0,0,0.5)',
    'JOB_RUNNING': 'rgba(0,0,255,0.5)',
    'UPLOADING': 'rgba(255,0,255,0.5)'}

DEFAULT_COLOR = 'rgba(0,0,0,0.5)'
LINE_COLOR = 'rgba(50,50,50,0.9)'

JOB_SHOW_COLS = ['LABEL', 'STATUS', 'LASTMOD', 'WALLTIME', 'JOBID']

JOB_TAB_COLS = [
    'LABEL', 'PROJECT', 'STATUS', 'PROCTYPE', 'USER',
    'JOBID', 'TIME', 'WALLTIME', 'LASTMOD']

SQUEUE_COLS = [
    'NAME', 'ST', 'STATE', 'PRIORITY', 'JOBID', 'MIN_MEMORY',
    'TIME', 'SUBMIT_TIME', 'START_TIME', 'TIME_LIMIT', 'TIME_LEFT', 'USER']

# These are the columns to be displayed in the table
TASK_SHOW_COLS = ['LABEL', 'STATUS', 'TIME', 'MEM', 'JOBID']

# These are all the columns in the datatable dataset
TASK_TAB_COLS = [
    'LABEL', 'PROJECT', 'PROCSTATUS', 'PROCTYPE', 'JOBID',
    'JOBSTARTDATE', 'WALLTIMEUSED', 'TIMEUSED', 'DATETIME',
    'TIME', 'MEM']


class DashboardData:
    def __init__(self, xnat):
        self.xnat = xnat
        self.timezone = TIMEZONE
        self.xnat_user = XNAT_USER
        self.updatetime = ''
        self.job_df = self.get_job_data()
        self.task_df = self.get_task_data()
        self.job_refresh_count = 0
        self.task_refresh_count = 0
        self.task_timeframe = 'week'

    def load_data(self):
        # use cache, if cache exists, load it
        if os.path.exists('job.pkl'):
            self.job_df = pd.read_pickle('job.pkl')
        else:
            self.job_df = self.refresh_job_data()

        # use cache, if cache exists, load it
        if os.path.exists('task.pkl'):
            self.task_df = pd.read_pickle('task.pkl')
        else:
            self.task_df = self.refresh_task_data()

    def get_task_data(self, timeframe='3day'):
        nowtime = datetime.now()
        end_date = datetime.strftime(nowtime, XNAT_DFORMAT)
        self.task_timeframe = timeframe

        # Determine the start date to query
        if timeframe == '3day':
            delta = timedelta(days=3)
        elif timeframe == '1day':
            delta = timedelta(days=1)
        elif timeframe == '1week':
            delta = timedelta(days=7)
        elif timeframe == '2week':
            delta = timedelta(days=14)
        else:  # default to 3 days
            delta = timedelta(days=3)

        start_date = datetime.strftime(nowtime - delta, XNAT_DFORMAT)

        # Load tasks in diskq
        logging.debug('loading data:{}-{}'.format(start_date, end_date))
        self.updatetime = nowtime

        df = self.load_proc_data(start_date, end_date)
        df = df[TASK_TAB_COLS].sort_values('LABEL')
        df['STATUS'] = df['PROCSTATUS']

        # Minimize columns
        logging.debug('finishing data')
        return df

    def load_proc_data(self, start_date, end_date):
        date_filter = 'proc:genprocdata/jobstartdate={}-{}'.format(
            start_date, end_date)

        # Build the URI to request
        _uri = ASSR_URI + '&' + date_filter

        # Extract assr data
        _json = self.get_json(_uri)
        df = pd.DataFrame(_json['ResultSet']['Result'])

        # Rename columns
        df.rename(columns=ASSR_RENAME, inplace=True)

        logging.debug('calling clean values')

        df = self.clean_values(df)

        return df

    def task_data(self):
        return self.task_df

    def job_data(self):
        return self.job_df

    def refresh_task_data(self, timeframe='week'):
        self.task_df = self.get_task_data(timeframe)
        self.task_refresh_count += 1

    def refresh_job_data(self, exclude_waiting=True):
        self.job_df = self.get_job_data(exclude_waiting)
        self.job_refresh_count += 1

    def get_job_data(self, exclude_waiting=True):
        # TODO: run each load in separate threads

        # Load tasks in diskq
        logging.debug('loading diskq')
        diskq_df = self.load_diskq_queue()

        # load squeue
        logging.debug('loading squeue')
        squeue_df = self.load_slurm_queue()

        # TODO: load xnat if we want to identify lost jobs in a separate tab

        # merge squeue data into task queue
        logging.debug('merging data')

        if diskq_df.empty and squeue_df.empty:
            print('both empty')
            df = pd.DataFrame(columns=diskq_df.columns.union(squeue_df.columns))
        elif diskq_df.empty:
            print('diskq empty')
            df = squeue_df.reindex(squeue_df.columns.union(diskq_df.columns), axis=1)
        elif squeue_df.empty:
            print('squeue empty')
            df = diskq_df.reindex(diskq_df.columns.union(squeue_df.columns), axis=1)
        else:
            print('merging')
            df = pd.merge(diskq_df, squeue_df, how='outer', on=['LABEL', 'USER'])

        #print(df)
        if not df.empty:
            # assessor label is delimited by "-x-", first element is project,
            # fourth element is processing type
            df['PROJECT'] = df['LABEL'].str.split('-x-', n=1, expand=True)[0]
            df['PROCTYPE'] = df['LABEL'].str.split('-x-', n=4, expand=True)[3]

            # Do this to avoid blanks in the table
            df['JOBID'].fillna('not launched', inplace=True)

            # create a concatenated status that maps to full status
            df['psST'] = df['procstatus'].fillna('NONE') + df['ST'].fillna('NONE')
            df['STATUS'] = df['psST'].map(STATUS_MAP).fillna('UNKNOWN')

            if exclude_waiting:
                df = df[df.STATUS != 'WAITING']

        # Determine how long ago status changed
        # how long has it been running, pending, waiting or complete?

        # Minimize columns
        logging.debug('finishing data')
        df = df.reindex(columns=JOB_TAB_COLS)
        return df.sort_values('LABEL')

    def load_diskq_queue(self, status=None):
        task_list = list()

        for d, u in zip(UPLOAD_DIR, SQUEUE_USER):
            diskq_dir = os.path.join(d, 'DISKQ')
            batch_dir = os.path.join(diskq_dir, 'BATCH')

            for t in os.listdir(batch_dir):
                assr = os.path.splitext(t)[0]
                task = self.load_diskq_task(diskq_dir, assr)
                task['USER'] = u
                task_list.append(task)

        if len(task_list) > 0:
            df = pd.DataFrame(task_list)
        else:
            df = pd.DataFrame(columns=[
                'LABEL', 'procstatus', 'jobid', 'jobnode', 'jobstartdate',
                'memused', 'walltimeused', 'WALLTIME', 'LASTMOD', 'USER'])

        return df

    def load_diskq_task(self, diskq, assr):
        return {
            'LABEL': assr,
            'procstatus': self.get_diskq_attr(diskq, assr, 'procstatus'),
            'jobid': self.get_diskq_attr(diskq, assr, 'jobid'),
            'jobnode': self.get_diskq_attr(diskq, assr, 'jobnode'),
            'jobstartdate': self.get_diskq_attr(diskq, assr, 'jobstartdate'),
            'memused': self.get_diskq_attr(diskq, assr, 'memused'),
            'walltimeused': self.get_diskq_attr(diskq, assr, 'walltimeused'),
            'WALLTIME': self.get_diskq_walltime(diskq, assr),
            'LASTMOD': self.get_diskq_lastmod(diskq, assr)}

    def load_slurm_queue(self):
        try:
            cmd = SQUEUE_CMD
            result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE)
            _data = result.stdout.decode('utf-8')
            df = pd.read_csv(
                StringIO(_data), delimiter='|', usecols=SQUEUE_COLS)
            df['LABEL'] = df['NAME'].str.split('.slurm').str[0]
            return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=SQUEUE_COLS+['LABEL'])

    def get_diskq_walltime(self, diskq, assr):
        COOKIE = "#SBATCH --time="
        walltime = None
        bpath = os.path.join(diskq, 'BATCH', assr + '.slurm')

        try:
            with open(bpath, 'r') as f:
                for line in f:
                    if line.startswith(COOKIE):
                        tmptime = line.split('=')[1].strip('"')
                        walltime = self.humanize_walltime(tmptime)
                        break
        except IOError:
            logging.warn('file does not exist:' + bpath)
            return None
        except PermissionError:
            logging.warn('permission error reading file:' + bpath)
            return None

        return walltime

    def humanize_walltime(self, walltime):
        tmptime = walltime
        days = 0
        hours = 0
        mins = 0

        if '-' in tmptime:
            tmpdays, tmptime = tmptime.split('-', 1)
            days = int(tmpdays)
        if ':' in walltime:
            tmphours, tmptime = tmptime.split(':', 1)
            hours = int(tmphours)
        if ':' in walltime:
            tmpmins = tmptime.split(':', 1)[0]
            mins = int(tmpmins)

        delta = timedelta(days=days, hours=hours, minutes=mins)
        return humanize.naturaldelta(delta)

    def humanize_memused(self, memused):
        return humanize.naturalsize(memused)

    def humanize_minutes(self, minutes):
        return humanize.naturaldelta(timedelta(minutes=minutes))

    def get_diskq_lastmod(self, diskq, assr):

        if os.path.exists(os.path.join(diskq, 'procstatus', assr)):
            apath = os.path.join(diskq, 'procstatus', assr)
        elif os.path.exists(os.path.join(diskq, 'BATCH', assr + '.slurm')):
            apath = os.path.join(diskq, 'BATCH', assr + '.slurm')
        else:
            return None

        updatetime = datetime.fromtimestamp(os.path.getmtime(apath))
        delta = datetime.now() - updatetime
        return humanize.naturaldelta(delta)

    def get_diskq_attr(self, diskq, assr, attr):
        apath = os.path.join(diskq, attr, assr)

        if not os.path.exists(apath):
            return None

        try:
            with open(apath, 'r') as f:
                return f.read().strip()
        except PermissionError:
            return None

    def get_json(self, uri):
        _data = json.loads(self.xnat._exec(uri, 'GET'))
        return _data

    def get_user_projects(self, user):
        uri = '/xapi/users/{}/groups'.format(user)

        # get from xnat and convert to list
        _data = list(self.get_json(uri))

        # format of group name is PROJECT_ROLE,
        # so we split on the underscore
        _data = sorted([x.rsplit('_', 1)[0] for x in _data])

        return _data

    def set_time(self, row):
        if pd.notna(row['SUBMIT_TIME']):
            startdt = datetime.strptime(
                str(row['SUBMIT_TIME']), '%Y-%m-%dT%H:%M:%S')
            row['submitdt'] = datetime.strftime(startdt, DFORMAT)

        row['timeused'] = row['TIME']

        return row

    def clean_values(self, df):

        df['MEM'] = df['MEMUSED'].apply(self.clean_mem)

        # Cleanup wall time used to just be number of minutes
        df['TIMEUSED'] = df['WALLTIMEUSED'].apply(self.clean_timeused)

        df['TIME'] = df['TIMEUSED'].apply(self.clean_time)

        df['STARTDATE'] = df['JOBSTARTDATE'].apply(self.clean_startdate)

        df['TIMEDELTA'] = pd.to_timedelta(df['TIMEUSED'], 'm')

        df['ENDDATE'] = df['STARTDATE'] + df['TIMEDELTA']

        df['DATETIME'] = df['ENDDATE'].apply(self.clean_enddate)

        return df

    def clean_enddate(self, enddate):
        return datetime.strftime(enddate, DFORMAT)

    def clean_startdate(self, jobstartdate):
        return datetime.strptime(jobstartdate, '%Y-%m-%d')

    def clean_mem(self, memused):
        try:
            bytes_used = int(float(memused))*1024
        except ValueError:
            bytes_used = np.nan

        return self.humanize_memused(bytes_used)

    def clean_time(self, timeused):
        return self.humanize_minutes(int(timeused))

    def clean_timeused(self, timeused):
        # Cleanup wall time used to just be number of minutes
        try:
            if '-' in timeused:
                t = datetime.strptime(timeused, '%j-%H:%M:%S')
                delta = timedelta(
                    days=t.day,
                    hours=t.hour, minutes=t.minute, seconds=t.second)
            else:
                t = datetime.strptime(timeused, '%H:%M:%S')
                delta = timedelta(
                    hours=t.hour, minutes=t.minute, seconds=t.second)

            return math.ceil(delta.total_seconds() / 60)
        except ValueError:
            return 1


def update_waiting(waiting):
    modified = False

    if waiting is not None:
        new_waiting = ('WAITING' in waiting)
        if new_waiting != True:
            exclude_waiting = waiting
            modified = True

    return modified


def update_timeframe(timeframe):
    modified = False

    if timeframe is not None and timeframe != dashdata.task_timeframe:
        modified = True

    return modified


def get_job_graph_content(df):
    PIVOTS = ['USER', 'PROJECT', 'PROCTYPE']
    tabs_content = []

    # index are we pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):
        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df, index=pindex, values='LABEL', columns=['STATUS'],
            aggfunc='count', fill_value=0)
        for status, color in zip(STATUS_LIST, COLOR_LIST):
            ydata = sorted(dfp.index)
            if status not in dfp:
                xdata = [0] * len(dfp.index)
            else:
                xdata = dfp[status]

            fig.append_trace(go.Bar(
                x=xdata,
                y=ydata,
                name='{} ({})'.format(status, sum(xdata)),
                marker=dict(color=color),
                opacity=0.9, orientation='h'), 1, 1)

        # Customize figure
        fig['layout'].update(barmode='stack', showlegend=True, width=900)

        # Build the tab
        label = 'By {}'.format(pindex)
        graph = html.Div(dcc.Graph(figure=fig), style={
            'width': '100%', 'display': 'inline-block'})
        tab = dcc.Tab(label=label, value=str(i + 1), children=[graph])

        # Append the tab
        tabs_content.append(tab)

    return tabs_content


def get_task_graph_content(df):
    tabs_content = []
    value = 0

    logging.debug('get_task_figure')

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Check for empty data
    if len(df) == 0:
        logging.debug('empty data')
        return fig

    # Plot trace for each status
    for i in df.PROCSTATUS.unique():
        # Filter data by status
        dft = df[df.PROCSTATUS == i]

        # Match status to main color
        try:
            color = STATUS2COLOR[i]
        except KeyError:
            color = DEFAULT_COLOR

        # Line color
        line = LINE_COLOR

        # Add trace to figure
        fig.append_trace({
            'name': '{} ({})'.format(i, len(dft)),
            'x': dft['DATETIME'],
            'y': dft['TIMEUSED'],
            'text': dft['LABEL'],
            'mode': 'markers',
            'marker': dict(
                color=color, size=10, line=dict(width=1, color=line))
        }, 1, 1)

    # Customize figure
    fig['layout'].update(
        yaxis=dict(type='log', title='minutes used'),
        hovermode='closest', showlegend=True, width=900)

    # Build the tab
    label = 'By {}'.format('TIME')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    value += 1
    tab = dcc.Tab(label=label, value=str(value), children=[graph])

    # Append the tab
    tabs_content.append(tab)

    PIVOTS = ['PROJECT', 'PROCTYPE']

    # index are we pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):

        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df, index=pindex, values='LABEL', columns=['PROCSTATUS'],
            aggfunc='count', fill_value=0)

        status2color = {
            'COMPLETE': RGB_BLUE,
            'JOB_FAILED': RGB_RED,
            'JOB_RUNNING': RGB_GREEN,
            'NEED_INPUTS': RGB_YELLOW,
            'UPLOADING': RGB_DKBLUE}

        for status in df.PROCSTATUS.unique():
            ydata = sorted(dfp.index)
            if status not in dfp:
                xdata = [0] * len(dfp.index)
            else:
                xdata = dfp[status]

            fig.append_trace(go.Bar(
                x=xdata,
                y=ydata,
                name='{} ({})'.format(status, sum(xdata)),
                marker=dict(color=status2color[status]),
                opacity=0.9, orientation='h'), 1, 1)

        # Customize figure
        fig['layout'].update(barmode='stack', showlegend=True, width=900)

        # Build the tab
        label = 'By {}'.format(pindex)
        graph = html.Div(dcc.Graph(figure=fig), style={
            'width': '100%', 'display': 'inline-block'})
        value += 1
        tab = dcc.Tab(label=label, value=str(value), children=[graph])

        # Append the tab
        tabs_content.append(tab)

    # Return the tabs
    return tabs_content


def get_job_content(df):
    df = get_job_data()

    job_graph_content = get_job_graph_content(df)

    job_columns = [{"name": i, "id": i} for i in JOB_SHOW_COLS]

    job_data = df.to_dict('rows')

    job_content = [
        dcc.Loading(id="loading-job", children=[
            html.Div(dcc.Tabs(
                id='tabs-job',
                value='1',
                children=job_graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-job-refresh'),
        dcc.Checklist(
            id='checklist-job-waiting',
            options=[{
                'label': 'exclude WAITING',
                'value': 'WAITING'}],
            value=['WAITING'],
            style={'display': 'inline'}, labelStyle={'display': 'inline'}),
        dcc.Dropdown(
            id='dropdown-job-proj', multi=True,
            placeholder='Select Project(s)'),
        dcc.Dropdown(
            id='dropdown-job-user', multi=True,
            placeholder='Select User(s)'),
        dcc.Dropdown(
            id='dropdown-job-proc', multi=True,
            placeholder='Select Processing Type(s)'),
        dt.DataTable(
                columns=job_columns,
                data=job_data,
                filter_action='native',
                page_action='none',
                sort_action='native',
                id='datatable-job',
                #fixed_rows={'headers': True}, # this behaves weirdly
                #style_table={'overflowY': 'auto', 'overflowX': 'auto'}, # this is weird too
                style_cell={'textAlign': 'left', 'padding': '5px'},
                style_data_conditional=[
                    {'if': {'column_id': 'STATUS'}, 'textAlign': 'center'},
                    {'if': {'filter_query': '{STATUS} = RUNNING'}, 'backgroundColor': HEX_LGREE},
                    {'if': {'filter_query': '{STATUS} = WAITING'}, 'backgroundColor': HEX_LGREY},
                    {'if': {'filter_query': '{STATUS} = "PENDING"'}, 'backgroundColor': HEX_LYELL},
                    {'if': {'filter_query': '{STATUS} = "UNKNOWN"'}, 'backgroundColor': HEX_LPURP},
                    {'if': {'filter_query': '{STATUS} = "FAILED"'}, 'backgroundColor': HEX_LREDD},
                    {'if': {'filter_query': '{STATUS} = "COMPLETE"'}, 'backgroundColor': HEX_LBLUE},
                    {'if': {'column_id': 'STATUS', 'filter_query': '{STATUS} = ""'}, 'backgroundColor': 'white'}
                ],
                style_header={'backgroundColor': 'white', 'fontWeight': 'bold'},
                fill_width=True,
                export_format='xlsx',
                export_headers='names',
                export_columns='visible')]

    return job_content


def get_task_content(df):

    task_graph_content = get_task_graph_content(df)

    task_columns = [{"name": i, "id": i} for i in TASK_SHOW_COLS]

    task_data = df.to_dict('rows')

    task_content = [
        dcc.Loading(id="loading-task", children=[
            html.Div(dcc.Tabs(
                id='tabs-task',
                value='2',
                children=task_graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-task-refresh'),
        dcc.Dropdown(
            id='dropdown-task-time',
            options=[
                {'label': '1 day', 'value': '1day'},
                {'label': '3 days', 'value': '3day'},
                {'label': '1 week', 'value': '1week'},
                {'label': '2 weeks', 'value': '2week'}],
            value='3day'),
        dcc.Dropdown(
            id='dropdown-task-proj', multi=True,
            placeholder='Select Project(s)'),
        dcc.Dropdown(
            id='dropdown-task-proc', multi=True,
            placeholder='Select Processing Type(s)'),
        dt.DataTable(
                columns=task_columns,
                data=task_data,
                filter_action='native',
                page_action='none',
                sort_action='native',
                id='datatable-task',
                #fixed_rows={'headers': True}, # this behaves weirdly
                style_cell={'textAlign': 'left', 'padding': '5px'},
                style_data_conditional=[
                    {'if': {'column_id': 'STATUS'}, 'textAlign': 'center'},
                    {'if': {'filter_query': '{STATUS} = COMPLETE'}, 'backgroundColor': HEX_LGREE},
                    {'if': {'filter_query': '{STATUS} = UNKNOWN'}, 'backgroundColor': HEX_LPURP},
                    {'if': {'filter_query': '{STATUS} = JOB_FAILED'}, 'backgroundColor': HEX_LREDD},
                    {'if': {'filter_query': '{STATUS} = ""'}, 'backgroundColor': 'white'}
                ],
                style_header={'backgroundColor': 'white', 'fontWeight': 'bold'},
                fill_width=True,
                export_format='xlsx',
                export_headers='names',
                export_columns='visible')]

    return task_content


def get_layout():
    logging.debug('get_layout')

    job_content = get_job_content(get_job_data())

    task_content = get_task_content(task_data())

    report_content = [
        html.Div(
            dcc.Tabs(id='tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='Job Queue', value='1', children=job_content),
                dcc.Tab(
                    label='Finished Tasks', value='2', children=task_content)

            ]),
            style={
                'width': '100%', 'display': 'flex',
                'align-items': 'center', 'justify-content': 'center'})]

    footer_content = [
        html.Hr(),
        html.H5('UNKNOWN: status is ambiguous or incomplete'),
        html.H5('FAILED: job has failed, but has not yet been uploaded'),
        html.H5('COMPLETE: job has finished, but not yet been uploaded'),
        html.H5('RUNNING: job is currently running on the cluster'),
        html.H5('PENDING: job has been submitted, but is not yet running'),
        html.H5('WAITING: job has been built, but is not yet submitted')]

    return html.Div([
                html.Div(children=report_content, id='report-content'),
                html.Div(children=footer_content, id='footer-content')])


def get_job_data():
    return dashdata.job_data()


def task_data():
    return dashdata.task_data()


def refresh_task_data():
    return dashdata.refresh_task_data(timeframe='week')


def refresh_job_data():
    return dashdata.refresh_job_data(exclude_waiting=True)


@app.callback(
    [Output('dropdown-task-proc', 'options'),
     Output('dropdown-task-proj', 'options'),
     Output('datatable-task', 'data'),
     Output('tabs-task', 'children')],
    [Input('dropdown-task-proc', 'value'),
     Input('dropdown-task-proj', 'value'),
     Input('dropdown-task-time', 'value'),
     Input('button-task-refresh', 'n_clicks')])
def update_all(selected_proc, selected_proj, selected_time, n_clicks):

    # Update exclude_waiting checkbox  and determine if it was modified
    timeframe_modified = update_timeframe(selected_time)

    # Refresh data if waiting was toggled or refresh button clicked
    if timeframe_modified or (n_clicks is not None and n_clicks > dashdata.task_refresh_count):
        logging.debug('update:refresh:count={},clicks={}'.format(
                dashdata.task_refresh_count, n_clicks))
        refresh_task_data()

    logging.debug('update_all')
    df = task_data()

    # Get the dropdown options
    proc = utils.make_options(df.PROCTYPE.unique())
    proj = utils.make_options(df.PROJECT.unique())

    # Filter by project
    if selected_proj:
        df = df[df['PROJECT'].isin(selected_proj)]

    # Filter by proctype
    if selected_proc:
        df = df[df['PROCTYPE'].isin(selected_proc)]

    tabs = get_task_graph_content(df)

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    records = df.to_dict('records')
    return [proc, proj, records, tabs]


@app.callback(
    [Output('dropdown-job-proc', 'options'),
     Output('dropdown-job-proj', 'options'),
     Output('dropdown-job-user', 'options'),
     Output('datatable-job', 'data'),
     Output('tabs-job', 'children')],
    [Input('dropdown-job-proc', 'value'),
     Input('dropdown-job-proj', 'value'),
     Input('dropdown-job-user', 'value'),
     Input('checklist-job-waiting', 'value'),
     Input('button-job-refresh', 'n_clicks')])
def update_everything(
        selected_proc,
        selected_proj,
        selected_user,
        waiting,
        n_clicks):

    # Update exclude_waiting checkbox  and determine if it was modified
    waiting_modified = update_waiting(waiting)

    # Refresh data if waiting was toggled or refresh button clicked
    if waiting_modified or (n_clicks is not None and n_clicks > dashdata.job_refresh_count):
        logging.debug('update:refresh:count={},clicks={}'.format(
                dashdata.job_refresh_count, n_clicks))
        refresh_job_data()

    # Load stored data
    logging.debug('update:loading data')
    df = get_job_data()

    # Get the dropdown options
    proc = utils.make_options(df.PROCTYPE.unique())
    proj = utils.make_options(df.PROJECT.unique())
    user = utils.make_options(df.USER.unique())

    # Filter by project
    if selected_proj:
        df = df[df['PROJECT'].isin(selected_proj)]

    if selected_user:
        df = df[df['USER'].isin(selected_user)]

    if selected_proc:
        df = df[df['PROCTYPE'].isin(selected_proc)]

    tabs = get_job_graph_content(df)

    # Return table, figure, dropdown options
    logging.debug('update_everything:returning data')
    records = df.to_dict('records')
    return [proc, proj, user, records, tabs]


# Connect to xnat and initialize our data source
logging.debug('DEBUG:connecting to XNAT')
xnat = XnatUtils.get_interface()
dashdata = DashboardData(xnat)

# Build the layout that be used by top level index.py
logging.debug('DEBUG:making the layout')
layout = get_layout()
