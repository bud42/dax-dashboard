import subprocess
from io import StringIO
from datetime import datetime
import pytz
import json
import os

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from dax import XnatUtils


pd.set_option('display.max_colwidth', None)


# SQUEUE_CMD = 'ssh sideshowb squeue -u '+SQUEUE_USER+' --format="%all"'
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
# diskq statuses: JOB_RUNNING, JOB_FAILED, NEED_TO_RUN,
# UPLOADING, READY_TO_COMPLETE, READY_TO_UPLOAD
STATUS_MAP = {
    'NONENONE': 'WAITING',
    'JOB_RUNNINGCD': 'RUNNING',
    'JOB_RUNNINGCG': 'RUNNING',
    'JOB_RUNNINGF': 'RUNNING',
    'JOB_RUNNINGR': 'RUNNING',
    'JOB_RUNNINGNONE': 'COMPLETE',
    'JOB_RUNNINGPD': 'PENDING'}

RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELLOW = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_GREY = 'rgb(200,200,200)'

STATUS_LIST = ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'UNKNOWN']
COLOR_LIST = [RGB_GREY, RGB_YELLOW, RGB_GREEN, RGB_BLUE, RGB_RED]

TASK_COLS = [
    'LABEL', 'PROJECT', 'STATUS', 'PROCTYPE', 'USER', 'TIME', 'TIME_LEFT',
    'JOBID']

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

    def load_data(self):
        # Cache update time
        nowtime = datetime.now()

        # Load tasks in diskq
        print('loading diskq queue')
        diskq_df = self.load_diskq_queue()

        # load squeue
        print('loading slurm queue')
        squeue_df = self.load_slurm_queue()

        # TODO: load xnat if we want to identify lost jobs in a separate tab

        print('merging data')
        # merge squeue data into task queue
        df = pd.merge(diskq_df, squeue_df, how='outer', on='LABEL')

        print('cleaning data:parse assessor')
        # assessor label is delimited by "-x-", first element is project,
        # fourth element is processing type
        df['PROJECT'] = df['LABEL'].str.split('-x-', n=1, expand=True)[0]
        df['PROCTYPE'] = df['LABEL'].str.split('-x-', n=4, expand=True)[3]

        print('cleaning data:set status')
        # create a concanated status that maps to full status
        df['psST'] = df['procstatus'].fillna('NONE') + df['ST'].fillna('NONE')
        df['STATUS'] = df['psST'].map(STATUS_MAP).fillna('UNKNOWN')

        print('finishing data')
        # Minimize columns
        self.task_df = df[TASK_COLS].sort_values('LABEL')

        # Store updated time
        self.updatetime = self.formatted_time(nowtime)

    def get_data(self):
        # Load tasks in diskq
        print('loading diskq queue')
        diskq_df = self.load_diskq_queue()

        # load squeue
        print('loading slurm queue')
        squeue_df = self.load_slurm_queue()

        # TODO: load xnat if we want to identify lost jobs in a separate tab

        print('merging data')
        # merge squeue data into task queue
        df = pd.merge(diskq_df, squeue_df, how='outer', on='LABEL')

        print('cleaning data:parse assessor')
        # assessor label is delimited by "-x-", first element is project,
        # fourth element is processing type
        df['PROJECT'] = df['LABEL'].str.split('-x-', n=1, expand=True)[0]
        df['PROCTYPE'] = df['LABEL'].str.split('-x-', n=4, expand=True)[3]

        print('cleaning data:set status')
        # create a concanated status that maps to full status
        df['psST'] = df['procstatus'].fillna('NONE') + df['ST'].fillna('NONE')
        df['STATUS'] = df['psST'].map(STATUS_MAP).fillna('UNKNOWN')

        print('finishing data')
        # Minimize columns
        return df[TASK_COLS].sort_values('LABEL')

    def update_data(self):
        # Reload data
        self.load_data()

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
            'walltimeused': self.get_diskq_attr(diskq, assr, 'walltimeused')}

    def load_slurm_queue(self):
        try:
            cmd = SQUEUE_CMD
            result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE)
            data = result.stdout.decode('utf-8')
            df = pd.read_csv(
                StringIO(data), delimiter='|', usecols=SQUEUE_COLS)
            df['LABEL'] = df['NAME'].str.split('.slurm').str[0]
            return df
        except pd.errors.EmptyDataError:
            return None

    def get_diskq_attr(self, diskq, assr, attr):
        apath = os.path.join(diskq, attr, assr)

        if not os.path.exists(apath):
            return None

        with open(apath, 'r') as f:
            return f.read().strip()

    def get_json(self, xnat, uri):
        data = json.loads(xnat._exec(uri, 'GET'))
        return data

    def get_user_projects(self, user):
        uri = '/xapi/users/{}/groups'.format(user)

        # get from xnat and convert to list
        data = list(self.get_json(self.xnat, uri))

        # format of group name is PROJECT_ROLE,
        # so we split on the underscore
        data = sorted([x.rsplit('_', 1)[0] for x in data])

        return data

    def get_project_names(self):
        if False:
            project_names = self.get_user_projects(self.xnat_user)
        else:
            project_names = [
                'TAYLOR_CAARE', 'TAYLOR_DepMIND', 'REMBRANDT',
                'LesionPilot', 'R21Perfusion', 'NIC', 'PRICE_NSF',
                'PNC_V3', 'BLSA', 'LANDMAN_UPGRAD']

        return project_names

    def updated_datetime(self):
        return datetime.strptime(self.updatetime, DFORMAT)

    def now_formatted(self):
        return datetime.strftime(
            datetime.now(pytz.timezone(self.timezone), DFORMAT))

    def formatted_time(self, curtime):
        return datetime.strftime(curtime, DFORMAT)

    def set_time(self, row):
        if pd.notna(row['SUBMIT_TIME']):
            startdt = datetime.strptime(
                str(row['SUBMIT_TIME']), '%Y-%m-%dT%H:%M:%S')
            row['submitdt'] = datetime.strftime(startdt, DFORMAT)

        row['timeused'] = row['TIME']

        # Make time used to just be number of minutes
        # TODO: optimize this, if we need it
        # if pd.notna(row['TIME']):
        #     try:
        #         if '-' in str(row['TIME']):
        #             t = datetime.strptime(str(row['TIME']), '%j-%H:%M:%S')
        #             delta = timedelta(
        #                 days=t.day,
        #                 hours=t.hour, minutes=t.minute, seconds=t.second)
        #         elif str(row['TIME']).count(':') == 2:
        #             t = datetime.strptime(str(row['TIME']), '%H:%M:%S')
        #             delta = timedelta(
        #                 hours=t.hour, minutes=t.minute, seconds=t.second)
        #         else:
        #             t = datetime.strptime(str(row['TIME']), '%M:%S')
        #             delta = timedelta(
        #                 hours=t.hour, minutes=t.minute, seconds=t.second)

        #         row['timeused(min)'] = math.ceil(delta.total_seconds() / 60)
        #
        #     except ValueError:
        #         print('ValueError:'+row['label'])
        #         row['timeused(min)'] = 1
        return row


class DaxDashboard:
    def __init__(self, url_base_pathname=None):
        if False:
            print('DEBUG:connecting to XNAT')
            self.xnat = XnatUtils.get_interface()
        else:
            self.xnat = None

        self.dashdata = DashboardData(self.xnat)
        self.app = None
        self.url_base_pathname = url_base_pathname
        self.build_app()

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

        # callbacks
        # ===================================================================
        @app.callback(
            Output('datatable-task', 'data'),
            [Input('dropdown-task-proj', 'value'),
             Input('dropdown-task-user', 'value'),
             Input('dropdown-task-proc', 'value'),
             Input('local', 'modified_timestamp')],
            [State('local', 'data')])
        def update_rows(
                 selected_proj, selected_user, selected_proc,
                 modified_timestamp, data):
            print('update_rows_task')

            if data is None:
                print('update_rows', 'PreventUpdate')
                raise PreventUpdate

            df = pd.DataFrame(data)

            # Filter by project
            if selected_proj:
                df = df[df['PROJECT'].isin(selected_proj)]

            if selected_user:
                df = df[df['USER'].isin(selected_user)]

            if selected_proc:
                df = df[df['PROCTYPE'].isin(selected_proc)]

            print('update_rows_task:len=', len(df))

            return df.to_dict('records')

        # @app.callback(
        #     Output('dropdown-task-proj', 'options'),
        #     [Input('local', 'modified_timestamp')],
        #     [State('local', 'data')])
        # def update_dropdown_proj(modified_timestamp, data):

        #     if data is None:
        #         print('update_dropdown_projects', 'PreventUpdate')
        #         raise PreventUpdate

        #     print('update_dropdown_proj')
        #     options = self.make_options(pd.DataFrame(data).PROJECT.unique())
        #     print('update_dropdown_proj', options)
        #     return options

        # @app.callback(
        #     Output('dropdown-task-proc', 'options'),
        #     [Input('local', 'modified_timestamp')],
        #     [State('local', 'data')])
        # def update_dropdown_proc(modified_timestamp, data):

        #     if data is None:
        #         print('update_dropdown_proc', 'PreventUpdate')
        #         raise PreventUpdate

        #     print('update_dropdown_proc')
        #     options = self.make_options(pd.DataFrame(data).PROCTYPE.unique())
        #     print('update_dropdown_proc', options)
        #     return options

        # @app.callback(
        #     Output('dropdown-task-user', 'options'),
        #     [Input('local', 'modified_timestamp')],
        #     [State('local', 'data')])
        # def update_dropdown_user(modified_timestamp, data):

        #     if data is None:
        #         print('update_dropdown_user', 'PreventUpdate')
        #         raise PreventUpdate

        #     print('update_dropdown_user')
        #     options = self.make_options(pd.DataFrame(data).USER.unique())
        #     print('update_dropdown_user', options)
        #     return options

        @app.callback(
            Output('graph-task', 'figure'),
            [Input('datatable-task', 'data'),
             Input('radio-task-groupby', 'value')])
        def update_figure(data, selected_groupby):
            if not data:
                print('update_figure', 'PreventUpdate')
                raise PreventUpdate

            print('update_figure')

            # Make a 1x1 figure (I dunno why, this is from doing multi plots)
            fig = plotly.subplots.make_subplots(rows=1, cols=1)
            fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

            # Load table data into a dataframe for easy manipulation
            df = pd.DataFrame(data)
            print('update_figure:len=', len(df))

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
            return fig

        # add a click to the appropriate store.
        @app.callback(
            Output('local', 'data'),
            [Input('update-button', 'n_clicks')],
            [State('local', 'data')])
        def update_button_click(n_clicks, data):
            if n_clicks is None:
                print('n_clicks PreventUpdate')
                raise PreventUpdate

            print('update_button_click', n_clicks)

            print('calling update_data')
            df = self.get_data()
            print('returning data')

            print('update_button_click:len=', len(df))
            return df.to_dict('records')

    def get_layout(self):
        print('building interface')

        job_show = ['LABEL', 'STATUS', 'TIME', 'JOBID']
        job_columns = [{"name": i, "id": i} for i in job_show]
        job_tab_content = [dcc.Loading(
            id='loading-main',
            type='default',
            style={'backgroundColor': 'transparent'},
            children=[
                dcc.Store(id='local', storage_type='local'),
                dcc.Graph(
                    id='graph-task',
                    figure={'title': 'Job Queue', 'layout': go.Layout(
                        xaxis={
                            'showgrid': False, 'zeroline': False,
                            'showline': False, 'showticklabels': False},
                        yaxis={
                            'showgrid': False, 'zeroline': False,
                            'showline': False, 'showticklabels': False})})]),
                html.Button(
                    'Update',
                    id='update-button',
                    n_clicks=0, style={'margin-right': '25px'}),
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
                    data={},
                    filter_action='native',
                    page_action='none',
                    sort_action='native',
                    id='datatable-task',
                    fixed_rows={'headers': True},
                    style_cell={'textAlign': 'left'},
                    fill_width=False,
                    export_format='xlsx',
                    export_headers='names',
                    export_columns='display')]

        report_content = [html.Div(dcc.Tabs(id='tabs', value=1, children=[
            dcc.Tab(label='Job Queue', value=1, children=job_tab_content)],
            vertical=False))]

        footer_content = [
            html.Hr(),
            html.H5('WAITING: job has been built, but is not yet submitted'),
            html.H5('PENDING: job has been submitted, but is not yet running'),
            html.H5('RUNNING: job is running on the cluster'),
            html.H5('COMPLETE: job has finished and will be uploaded'),
            html.H5('UNKNOWN: status is ambiguous or incomplete'),
            html.Hr(),
            html.Div([
                html.P('DAX Dashboard by BDB', style={'textAlign': 'right'})])]

        top_content = [dcc.Location(id='url', refresh=False), html.Div([
            html.H3(
                'DAX Dashboard',
                style={
                    'margin-right': '100px', 'display': 'inline-block'}),
            ], style={'display': 'inline-block'})]

        return html.Div([
                    html.Div(children=top_content, id='top-content'),
                    html.Div(children=report_content, id='report-content'),
                    html.Div(children=footer_content, id='footer-content')])

    def update_data(self):
        return self.dashdata.update_data()

    def get_data(self):
        return self.dashdata.get_data()

    def get_app(self):
        return self.app

    def run(self, host='0.0.0.0'):
        print('DEBUG:running app on host:' + host)
        self.app.run_server(host=host)


if __name__ == '__main__':
    daxdash = DaxDashboard()
    daxdash.run()
