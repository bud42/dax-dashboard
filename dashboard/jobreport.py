import subprocess
from io import StringIO
import math
from datetime import datetime, timedelta
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
from dash.dependencies import Input, Output
from dax import XnatUtils

# NEXT: radio buttons to select group by User/Project/Processing Type
# load an addition queue (from disk by listing subdirs in the upload dir)
# as the upload queue and display them as
# "actually" uploading vs queued for upload
# maybe add a dropdown to filter by status, so we can limit the table to
# only inlcude specific status (this could be done by filter too, buy this
# would be more convenient)


# Tab #1: jobs that are both in the task queue and slurm queue or are complete
# jobs that complete will use walltimeuse, jobs that are still running will
# use elapsed time
# Tab #2: jobs that are JOB_RUNNING on xnat but missing
# Tab #3: finished jobs from xnat

pd.set_option('display.max_colwidth', None)

#SQUEUE_CMD = 'ssh sideshowb squeue -u vuiis_archive_singularity --format="%all"'
SQUEUE_CMD = 'squeue -u vuiis_archive_singularity --format="%all"'
DFORMAT = '%Y-%m-%d %H:%M:%S'
TIMEZONE = 'US/Central'
XNAT_USER = 'boydb1'
#UPLOAD_DIR = '/Users/boydb1/RESULTS_XNAT_SPIDER'
UPLOAD_DIR = '/scratch/vuiis_archive_singularity/Spider_Upload_Dir'
RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELLOW = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_GREY = 'rgb(200,200,200)'

TASK_COLS = [
    'label', 'project', 'session', 'status', 'procstatus', 'ST',
    'proctype', 'submitdt', 'timeused', 'user']

SQUEUE_COLS = [
    'NAME', 'USER', 'ACCOUNT', 'GROUP',
    'ST', 'STATE', 'PRIORITY', 'JOBID', 'MIN_MEMORY',
    'TIME', 'SUBMIT_TIME', 'START_TIME', 'TIME_LIMIT', 'TIME_LEFT']


class DashboardData:
    def __init__(self, xnat):
        self.xnat = xnat
        self.task_df = None
        self.timezone = TIMEZONE
        self.xnat_user = XNAT_USER
        self.updatetime = ''

        # Determine project list
        proj_list = self.get_project_names()
        print(proj_list)
        self.all_proj_list = proj_list

    def load_data(self):
        # Load tasks in diskq
        print('loading diskq queue')
        diskq_df = self.load_diskq_queue()

        # load squeue
        print('loading slurm queue')
        squeue_df = self.load_slurm_queue()

        # TODO: load xnat if we want to identify lost jobs in a separate tab

        print('merging data')
        # merge squeue data into task queue
        df = pd.merge(diskq_df, squeue_df, how='outer', on='label')

        print('cleaning data')
        # Apply the clean values
        df = df.apply(self.clean_values, axis=1)
        df = df.apply(self.parse_assessor, axis=1)
        df['user'] = 'vuiis_archive_singularity'

        print('finishing data')
        # Minimize columns
        self.task_df = df[TASK_COLS]

    def update_data(self):
        self.updatetime = self.formatted_time(datetime.now())

        # Reload data
        self.load_data()

    def load_diskq_queue(self, status=None):
        diskq_dir = os.path.join(UPLOAD_DIR, 'DISKQ')
        batch_dir = os.path.join(diskq_dir, 'BATCH')
        task_list = list()
        batch_list = os.listdir(batch_dir)

        for t in batch_list:
            assr = os.path.splitext(t)[0]
            task_list.append(self.load_diskq_task(diskq_dir, assr))

        df = pd.DataFrame(task_list)
        return df

    def load_diskq_task(self, diskq, assr):
        return {
            'label': assr,
            'procstatus': self.get_diskq_attr(diskq, assr, 'procstatus'),
            'jobid': self.get_diskq_attr(diskq, assr, 'jobid'),
            'jobnode': self.get_diskq_attr(diskq, assr, 'jobnode'),
            'jobstartdate': self.get_diskq_attr(diskq, assr, 'jobstartdate'),
            'memused': self.get_diskq_attr(diskq, assr, 'memused'),
            'walltimeused': self.get_diskq_attr(diskq, assr, 'walltimeused')}

    def load_slurm_queue(self, data=None):

        if data is None:
            cmd = SQUEUE_CMD
            result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE)
            data = result.stdout.decode('utf-8')

        df = pd.read_csv(StringIO(data), delimiter='|', usecols=SQUEUE_COLS)
        df['label'] = df['NAME'].str.split('.slurm').str[0]
        return df

    def get_diskq_attr(self, diskq, assr, attr):
        apath = os.path.join(diskq, attr, assr)

        if not os.path.exists(apath):
            return None

        with open(apath, 'r') as f:
            return f.read().strip()

    def parse_assessor(self, row):
        labels = row['label'].split("-x-")
        row['project'] = labels[0]
        row['subject'] = labels[1]
        row['session'] = labels[2]
        row['proctype'] = labels[3]
        return row

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

    def clean_values(self, row):
        row['account'] = row['ACCOUNT']

        # Use diskq status and squeue status to make a single status
        # squeue states: CG,F, PR, S, ST
        # diskq statuses: JOB_RUNNING, JOB_FAILED, NEED_TO_RUN,
        # UPLOADING, READY_TO_COMPLETE, READY_TO_UPLOAD
        dstatus = row['procstatus']
        sstatus = row['ST']
        if pd.isna(dstatus) and pd.isna(sstatus):
            row['status'] = 'WAITING'
        elif dstatus == 'JOB_RUNNING' and sstatus in ['R', 'CD', 'CG', 'F']:
            row['status'] = 'RUNNING'
        elif dstatus == 'JOB_RUNNING' and not sstatus:
            row['status'] = 'WAITING'
        elif dstatus == 'JOB_RUNNING' and sstatus == 'PD':
            row['status'] = 'PENDING'
        else:
            row['status'] = 'UNKNOWN'

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
        if pd.notna(row['SUBMIT_TIME']):
            startdt = datetime.strptime(
                str(row['SUBMIT_TIME']), '%Y-%m-%dT%H:%M:%S')
            row['submitdt'] = datetime.strftime(startdt, DFORMAT)

        row['timeused'] = row['TIME']
        return row


class DaxDashboard:
    def __init__(self, url_base_pathname=None):
        print('DEBUG:connecting to XNAT')
        self.xnat = XnatUtils.get_interface()
        self.dashdata = DashboardData(self.xnat)
        self.app = None
        self.url_base_pathname = url_base_pathname
        self.build_app()

    def make_options(self, values):
        return [{'label': x, 'value': x} for x in sorted(values)]

    def build_app(self):
        # Make the main dash app
        app = dash.Dash(__name__)

        #app.css.config.serve_locally = False

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

        # If you are assigning callbacks to components
        # that are generated by other callbacks
        # (and therefore not in the initial layout), then
        # you can suppress this exception by setting
        # app.config['suppress_callback_exceptions'] = True

        # callbacks
        # ===================================================================
        @app.callback(
            Output('datatable-task', 'data'),
            [Input('dropdown-task-proj', 'value')])
        def update_rows(selected_proj):
            print('update_rows_task')
            df = self.dashdata.task_df

            # Filter by project
            if selected_proj:
                df = df[df['project'].isin(selected_proj)]

            return df.to_dict('records')

        @app.callback(
            Output('graph-task', 'figure'),
            [Input('datatable-task', 'data')])
        def update_figure(data):
            print('update_figure')

            selected_groupby = 'none'

            if selected_groupby == 'proctype':
                pass
            elif selected_groupby == 'project':
                df = pd.DataFrame(data)
                yall = sorted(df.project.unique())
                xgree = df[df.status == 'RUNNING'].groupby('project')['label'].count()
                xblue = df[df.status == 'UPLOADING'].groupby('project')['label'].count()
                xredd = df[df.status == 'UNKNOWN'].groupby('project')['label'].count()
                xyell = df[df.status == 'PENDING'].groupby('project')['label'].count()
                xgrey = df[df.status == 'WAITING'].groupby('project')['label'].count()

                # Make a 1x1 figured
                fig = plotly.subplots.make_subplots(rows=1, cols=1)

                # Draw bar for each status, note these will be displayed
                # in order left to right horizontally
                fig.append_trace(go.Bar(
                    y=yall, x=xgrey,
                    name='{} ({})'.format('WAITING', xgrey.sum()),
                    marker=dict(color=RGB_GREY),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xyell, name='PENDING',
                    marker=dict(color=RGB_YELLOW),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xgree, name='RUNNING',
                    marker=dict(color=RGB_GREEN),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xblue, name='UPLOADING',
                    marker=dict(color=RGB_BLUE),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xredd, name='UNKNOWN',
                    marker=dict(color=RGB_RED),
                    opacity=0.9, orientation='h'), 1, 1)

                # Customize figure
                fig['layout'].update(barmode='stack', showlegend=True)

                return fig
            else:
                df = pd.DataFrame(data)
                yall = sorted(df.user.unique())
                print(yall)
                xgree = df[df.status == 'RUNNING'].groupby('user')['label'].count()
                xblue = df[df.status == 'UPLOADING'].groupby('user')['label'].count()
                xredd = df[df.status == 'UNKNOWN'].groupby('user')['label'].count()
                xyell = df[df.status == 'PENDING'].groupby('user')['label'].count()
                xgrey = df[df.status == 'WAITING'].groupby('user')['label'].count()

                # Make a 1x1 figured
                fig = plotly.subplots.make_subplots(rows=1, cols=1)

                # Draw bar for each status, note these will be displayed
                # in order left to right horizontally
                fig.append_trace(go.Bar(
                    y=yall, x=xgrey,
                    name='{} ({})'.format('WAITING', xgrey.sum()),
                    marker=dict(color=RGB_GREY),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xyell,
                    name='{} ({})'.format('PENDING', xyell.sum()),
                    marker=dict(color=RGB_YELLOW),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xgree,
                    name='{} ({})'.format('RUNNING', xgree.sum()),
                    marker=dict(color=RGB_GREEN),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xblue,
                    name='{} ({})'.format('UPLOADING', xblue.sum()),
                    marker=dict(color=RGB_BLUE),
                    opacity=0.9, orientation='h'), 1, 1)

                fig.append_trace(go.Bar(
                    y=yall, x=xredd,
                    name='{} ({})'.format('UNKNOWN', xredd.sum()),
                    marker=dict(color=RGB_RED),
                    opacity=0.9, orientation='h'), 1, 1)

                # Customize figure
                fig['layout'].update(barmode='stack', showlegend=True)

                return fig

        @app.callback(
            Output('update-text', 'children'),
            [Input('update-button', 'n_clicks')])
        def update_button_click(n_clicks):
            print('update_button_click', n_clicks)
            if n_clicks > 0:
                print('INFO:UPDATING DATA')
                self.update_data()

            return ['{}    '.format(self.dashdata.updatetime)]
            #,self.dashdata.updatetime_humanized())]

    def get_layout(self):
        print('building interface')
        self.dashdata.load_data()
        df = self.dashdata.task_df
        proj_options = self.make_options(df.project.unique())
        job_columns = [
            {"name": i, "id": i} for i in self.dashdata.task_df.columns]
        job_data = self.dashdata.task_df.to_dict('rows')

        job_tab_content = [
                html.Div(
                  html.Div([
                    dcc.Graph(
                        id='graph-task'),
                    dcc.Dropdown(
                        id='dropdown-task-proj', multi=True,
                        options=proj_options,
                        placeholder='Select Project(s)'),
                    dt.DataTable(
                        columns=job_columns,
                        data=job_data,
                        filter_action='native',
                        page_action='none',
                        sort_action='native',
                        id='datatable-task'),
                    ], className="container", style={"max-width": "none"})
                ),
                html.Div(dt.DataTable(data=[{}]), style={'display': 'none'})]

        report_content = [html.Div(dcc.Tabs(id='tabs', value=1, children=[
            dcc.Tab(label='Jobs', value=1, children=job_tab_content)],
            vertical=False),)]

        return html.Div([
            dcc.Location(id='url', refresh=False),
            html.Div([
                html.H1(
                    'DAX Dashboard',
                    style={
                        'margin-right': '100px', 'display': 'inline-block'}),
                html.P(children=[
                    'Last updated: ',
                    html.P(
                        children=['{}     '.format(self.dashdata.updatetime)],
                        id='update-text',
                        style={
                            'margin-right': '25px',
                            'display': 'inline-block'}),
                    html.Button(
                        'Update',
                        id='update-button',
                        n_clicks=0, style={'margin-right': '25px'}),
                ], style={'float': 'right', 'display': 'inline-block'}),
            ], style={'display': 'inline-block'}),
            html.Div(children=report_content, id='report-content')])

    def update_data(self):
        self.dashdata.update_data()

    def get_app(self):
        return self.app

    def run(self, host='0.0.0.0'):
        print('DEBUG:running app on host:' + host)
        self.app.run_server(host=host)


if __name__ == '__main__':
    daxdash = DaxDashboard()
    daxdash.run()
