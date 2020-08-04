import subprocess
from io import StringIO
from datetime import datetime, timedelta
import json
import os

import humanize
import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from dash.dependencies import Input, Output

from dax import XnatUtils

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

pd.set_option('display.max_colwidth', None)


SQUEUE_USER = ['vuiis_archive_singularity', 'vuiis_daily_singularity']
UPLOAD_DIR = [
    '/scratch/vuiis_archive_singularity/Spider_Upload_Dir',
    '/scratch/vuiis_daily_singularity/Spider_Upload_Dir']
SQUEUE_CMD = 'squeue -u '+','.join(SQUEUE_USER)+' --format="%all"'
DFORMAT = '%Y-%m-%d %H:%M:%S'
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
HEX_LPURP = '#FFD281'

STATUS_LIST = ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN']
COLOR_LIST = [RGB_GREY, RGB_YELLOW, RGB_GREEN, RGB_BLUE, RGB_RED, RGB_PURPLE]
LCOLOR_LIST = [HEX_LGREY, HEX_LYELL, HEX_LGREE, HEX_LBLUE, HEX_LREDD, HEX_LPURP]

SHOW_COLS = ['LABEL', 'STATUS', 'LASTMOD', 'WALLTIME', 'JOBID']

TASK_COLS = [
    'LABEL', 'PROJECT', 'STATUS', 'PROCTYPE', 'USER',
    'JOBID', 'TIME', 'WALLTIME', 'LASTMOD']

SQUEUE_COLS = [
    'NAME', 'ST', 'STATE', 'PRIORITY', 'JOBID', 'MIN_MEMORY',
    'TIME', 'SUBMIT_TIME', 'START_TIME', 'TIME_LIMIT', 'TIME_LEFT']


class DashboardData:
    def __init__(self, xnat):
        self.xnat = xnat
        self.task_df = None
        self.timezone = TIMEZONE
        self.xnat_user = XNAT_USER
        self.updatetime = ''
        self.df = self.get_data()

    def data(self):
        return self.df

    def refresh_data(self, waiting=False):
        self.df = self.get_data(waiting)

    def get_data(self, waiting=False):
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
        df = pd.merge(diskq_df, squeue_df, how='outer', on='LABEL')

        # assessor label is delimited by "-x-", first element is project,
        # fourth element is processing type
        df['PROJECT'] = df['LABEL'].str.split('-x-', n=1, expand=True)[0]
        df['PROCTYPE'] = df['LABEL'].str.split('-x-', n=4, expand=True)[3]

        # Do this to avoid blanks in the table
        df['JOBID'].fillna('not launched', inplace=True)

        # create a concanated status that maps to full status
        df['psST'] = df['procstatus'].fillna('NONE') + df['ST'].fillna('NONE')
        df['STATUS'] = df['psST'].map(STATUS_MAP).fillna('UNKNOWN')

        # for debugging exclude waiting
        if not waiting:
            df = df[df.STATUS != 'WAITING']

        # Determine how long ago status changed
        # how long has it been running, pending, waiting or complete?

        # Minimize columns
        logging.debug('finishing data')
        return df[TASK_COLS].sort_values('LABEL')

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

        df = pd.DataFrame(task_list)
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
            return None

    def get_diskq_walltime(self, diskq, assr):
        COOKIE = "#SBATCH --time="
        walltime = None
        bpath = os.path.join(diskq, 'BATCH', assr + '.slurm')

        if os.path.exists(bpath):
            with open(bpath, 'r') as f:
                for line in f:
                    if line.startswith(COOKIE):
                        walltime = self.humanize_walltime(line.split('=')[1])
                        break

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

        with open(apath, 'r') as f:
            return f.read().strip()

    def get_json(self, xnat, uri):
        _data = json.loads(xnat._exec(uri, 'GET'))
        return _data

    def get_user_projects(self, user):
        uri = '/xapi/users/{}/groups'.format(user)

        # get from xnat and convert to list
        _data = list(self.get_json(self.xnat, uri))

        # format of group name is PROJECT_ROLE,
        # so we split on the underscore
        _data = sorted([x.rsplit('_', 1)[0] for x in _data])

        return _data

    def get_project_names(self):
        if False:
            project_names = self.get_user_projects(self.xnat_user)
        else:
            project_names = [
                'TAYLOR_CAARE', 'TAYLOR_DepMIND', 'REMBRANDT',
                'LesionPilot', 'R21Perfusion', 'NIC', 'PRICE_NSF',
                'PNC_V3', 'BLSA', 'LANDMAN_UPGRAD']

        return project_names

    def set_time(self, row):
        if pd.notna(row['SUBMIT_TIME']):
            startdt = datetime.strptime(
                str(row['SUBMIT_TIME']), '%Y-%m-%dT%H:%M:%S')
            row['submitdt'] = datetime.strftime(startdt, DFORMAT)

        row['timeused'] = row['TIME']

        return row


class DaxDashboard:
    def __init__(self, url_base_pathname=None):
        if False:
            logging.debug('DEBUG:connecting to XNAT')
            self.xnat = XnatUtils.get_interface()
        else:
            self.xnat = None

        self.dashdata = DashboardData(self.xnat)
        self.app = None
        self.url_base_pathname = url_base_pathname
        self.build_app()
        self.update_count = 0
        self.waitin_count = 0

    def make_options(self, values):
        return [{'label': x, 'value': x} for x in sorted(values)]

    def build_app(self):
        # Make the main dash app
        app = dash.Dash(__name__)

        app.css.config.serve_locally = False

        app.css.append_css({
            "external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

        # Note: other css is loaded from assets folder

        app.title = 'DAX Dashboard'
        if self.url_base_pathname:
            app.config.update({
                'url_base_pathname': self.url_base_pathname,
                'routes_pathname_prefix': self.url_base_pathname,
                'requests_pathname_prefix': self.url_base_pathname
            })

        self.app = app

        # By setting layout to the functionn name instead of running the
        # function here, we make the function run on page load and
        # therefore we load the data on page load
        app.layout = self.get_layout

        @app.callback(
            [Output('dropdown-task-proc', 'options'),
             Output('dropdown-task-proj', 'options'),
             Output('dropdown-task-user', 'options'),
             Output('datatable-task', 'data'),
             Output('graph-task', 'figure')],
            [Input('radio-task-groupby', 'value'),
             Input('dropdown-task-proc', 'value'),
             Input('dropdown-task-proj', 'value'),
             Input('dropdown-task-user', 'value'),
             Input('waitin-button', 'n_clicks'),
             Input('update-button', 'n_clicks')])
        def update_everything(
                selected_groupby,
                selected_proc,
                selected_proj,
                selected_user,
                n_clicks_waiting,
                n_clicks):

            if n_clicks is not None and n_clicks > self.update_count:
                self.update_count += 1
                logging.debug('update_everything:refreshing:update_count={},clicks={}'.format(self.update_count, n_clicks))
                self.refresh_data()
            elif n_clicks_waiting is not None and n_clicks_waiting > self.waitin_count:
                self.waitin_count += 1
                logging.debug('update_everything:rewaiting:clicks refresh ={},clicks rewait={}'.format(n_clicks, n_clicks_waiting))
                self.refresh_data(waiting=True)

            logging.debug('update_everything:loading data:update_count={},clicks={}'.format(self.update_count, n_clicks))
            df = self.data()

            # Get the dropdown options
            proc = self.make_options(pd.DataFrame(df).PROCTYPE.unique())
            proj = self.make_options(pd.DataFrame(df).PROJECT.unique())
            user = self.make_options(pd.DataFrame(df).USER.unique())

            # Filter by project
            if selected_proj:
                df = df[df['PROJECT'].isin(selected_proj)]

            if selected_user:
                df = df[df['USER'].isin(selected_user)]

            if selected_proc:
                df = df[df['PROCTYPE'].isin(selected_proc)]

            # Make a 1x1 figure (I dunno why, this is from doing multi plots)
            fig = plotly.subplots.make_subplots(rows=1, cols=1)
            fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

            # What index are we pivoting on to count statuses
            PINDEX = selected_groupby

            # Draw bar for each status, these will be displayed in order
            dfp = pd.pivot_table(
                df, index=PINDEX, values='LABEL', columns=['STATUS'],
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
            fig['layout'].update(barmode='stack', showlegend=True)

            # Return table, figure, dropdown options
            logging.debug('update_everything:returning:update_count={},clicks={}'.format(self.update_count, n_clicks))
            records = df.to_dict('records')
            return [proc, proj, user, records, fig, ]

    def get_layout(self):
        logging.debug('get_layout')

        df = self.data()

        job_columns = [{"name": i, "id": i} for i in SHOW_COLS]
        job_data = df.to_dict('rows')
        #print(job_columns)
        #print(job_data)
        job_tab_content = [
            dcc.Loading(id="loading-task", children=[
                dcc.Graph(id='graph-task'),
                html.Button('Refresh Data', id='update-button'),
                html.Button('Refresh Data including WAITING', id='waitin-button')]),
            dcc.RadioItems(
                options=[
                    {'label': 'By USER', 'value': 'USER'},
                    {'label': 'By PROJECT', 'value': 'PROJECT'},
                    {'label': 'By PROCTYPE', 'value': 'PROCTYPE'}],
                value='USER',
                id='radio-task-groupby',
                labelStyle={'display': 'inline-block'}),
            dcc.Dropdown(
                id='dropdown-task-proj', multi=True,
                placeholder='Select Project(s)'),
            dcc.Dropdown(
                id='dropdown-task-user', multi=True,
                placeholder='Select User(s)'),
            dcc.Dropdown(
                id='dropdown-task-proc', multi=True,
                placeholder='Select Processing Type(s)'),
            dt.DataTable(
                    columns=job_columns,
                    data=job_data,
                    filter_action='native',
                    page_action='none',
                    sort_action='native',
                    id='datatable-task',
                    fixed_rows={'headers': True},
                    style_cell={'textAlign': 'left', 'padding': '5px'},
                    style_cell_conditional=[
                        {'if': {'column_id': 'STATUS'}, 'textAlign': 'center'},
                        {'if': {'filter_query': '{STATUS} = RUNNING'}, 'backgroundColor': HEX_LGREE},
                        {'if': {'filter_query': '{STATUS} = WAITING'}, 'backgroundColor': HEX_LGREY},
                        {'if': {'filter_query': '{STATUS} = PENDING'}, 'backgroundColor': HEX_LYELL},
                        {'if': {'filter_query': '{STATUS} = UNKNOWN'}, 'backgroundColor': HEX_LPURP},
                        {'if': {'filter_query': '{STATUS} = FAILED'}, 'backgroundColor': HEX_LREDD},
                        {'if': {'filter_query': '{STATUS} = COMPLETE'}, 'backgroundColor': HEX_LBLUE},
                        {'if': {'filter_query': '{STATUS} = ""'}, 'backgroundColor': 'white'}],
                    style_header={'backgroundColor': 'white', 'fontWeight': 'bold'},
                    fill_width=True,
                    export_format='xlsx',
                    export_headers='names',
                    export_columns='display')]

        report_content = [
            html.Div(
                dcc.Tabs(id='tabs', value=1, vertical=False, children=[
                    dcc.Tab(
                        label='Job Queue', value=1, children=job_tab_content)
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
            html.H5('WAITING: job has been built, but is not yet submitted'),
            html.Hr(),
            html.Div([
                html.P('DAX Dashboard by BDB', style={'textAlign': 'right'})])]

        top_content = [
            dcc.Location(id='url', refresh=False),
            html.Div([html.H1('DAX Dashboard')])]

        return html.Div([
                    html.Div(children=top_content, id='top-content'),
                    html.Div(children=report_content, id='report-content'),
                    html.Div(children=footer_content, id='footer-content')])

    def data(self):
        return self.dashdata.data()

    def refresh_data(self, waiting=False):
        return self.dashdata.refresh_data(waiting)

    def get_app(self):
        return self.app

    def run(self, host='0.0.0.0'):
        logging.debug('DEBUG:running app on host:' + host)
        self.app.run_server(host=host)


if __name__ == '__main__':
    daxdash = DaxDashboard()
    daxdash.run()
