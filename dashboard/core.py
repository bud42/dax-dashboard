import math
from datetime import datetime, timedelta
import pandas as pd
import plotly
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from glob import glob
import yaml
from dax import XnatUtils
import os
import tempfile
from zipfile import BadZipfile
import fileinput
import json


# TODO: update buttons or buttons
# TODO: each tab has button and file that are handled separately BUT assessor
# file is reused by stats tabs
# need to display time "updated: datetime" on each tab
# when app starts it uses cached data or None, then each tab can be updated
# need to make tabs "independent" so they can be included as customized


class DaxDashboard:
    DFORMAT = '%Y-%m-%d %H:%M:%S'
    TASK_IGNORE_LIST = ['NO_DATA', 'NEED_INPUTS', 'Complete']
    TASK_COLS = [
        'label', 'project', 'memused(MB)',
        'procstatus', 'proctype', 'datetime', 'timeused(min)']
    FAV_URI = '/data/archive/projects?favorite=True'

    def __init__(self, config_file, server=None):
        self.config = None

        with open(config_file, 'r') as f:
            self.config = yaml.load(f)

        if not os.path.exists(self.config['data_dir']):
            raise IOError('Dir does not exist:' + self.config['data_dir'])

        self.datadir = self.config['data_dir']
        self.app = None
        self.reset_curtime()
        self.load_data()
        self.server = server
        self.build_app()

    def reset_curtime(self):
        self.starttime = datetime.now()

        # Get the time for naming files
        self.curtime = datetime.strftime(self.starttime, '%Y%m%d-%H%m%S')

        # Get the time for jobs
        self.nowtime = datetime.strftime(self.starttime, self.DFORMAT)

    def extract_scan_data(self, xnat, proj_list, data_file):
        # Build list of scans for all projects
        scan_list = list()
        for proj in proj_list:
            print('DEBUG:extracting scan data for project:' + proj)
            scan_list.extend(xnat.get_project_scans(proj))

        # Convert scan to dataframe and filter it
        scan_df = pd.DataFrame(scan_list)

        # Write scan file
        print('writing:' + data_file)
        scan_df.to_csv(data_file)

    def extract_assr_data(self, xnat, proj_list, data_file):
        # Build list of scans/assrs for all projects
        assr_list = list()
        for proj in proj_list:
            print('DEBUG:extracting assr data for project:' + proj)
            assr_list.extend(XnatUtils.list_project_assessors(xnat, proj))

        # Convert assr to dataframe and filter it
        assr_df = pd.DataFrame(assr_list)

        # Write assr file
        print('writing:' + data_file)
        assr_df.to_csv(data_file)
        return assr_list

    def update_data(self):
        self.reset_curtime()

        # Load data from XNAT
        xnat = XnatUtils.get_interface()

        if self.config['xnat_projects'] == 'favorites':
            # Load projects that have been checked as Favorite for current user
            print('Loading project list from XNAT')
            proj_list = [x['id'] for x in xnat._get_json(self.FAV_URI)]
        elif isinstance(self.config['xnat_projects'], basestring):
            proj_list = [self.config['xnat_projects']]
        else:
            proj_list = self.config['xnat_projects']

        print(proj_list)

        # Write scan file
        scan_file = self.datadir + '/scandata-' + self.curtime + '.csv'
        self.extract_scan_data(xnat, proj_list, scan_file)

        # Write assr file
        assr_file = self.datadir + '/assrdata-' + self.curtime + '.csv'
        assr_list = self.extract_assr_data(xnat, proj_list, assr_file)

        # Write squeue file
        if self.config['use_squeue']:
            _user = self.config['squeue_user']
            _file = self.datadir + '/squeue-' + self.curtime + '.txt'
            cmd = 'squeue -u ' + _user + ' --format="%all" > ' + _file
            print('running:' + cmd)
            os.system(cmd)

        if True:
            self.update_stats_data(xnat, assr_list)

    def update_stats_data(self, xnat, assr_list):
        # LST
        lst_v1_list = [a for a in assr_list if a['proctype'] == 'LST_v1']
        lst_v1_file = self.datadir + '/lst_v1-' + self.curtime + '.csv'
        print('extracting LST_v1 data to:' + lst_v1_file)
        self.extract_stats_data(xnat, lst_v1_list, lst_v1_file)

        # fmri_v3
        fmri_v3_list = [a for a in assr_list if a['proctype'] == 'fMRIQA_v3']
        fmri_v3_file = self.datadir + '/fmriqa_v3-' + self.curtime + '.csv'
        print('extracting fmri_v3 data to:' + fmri_v3_file)
        self.extract_stats_data(xnat, fmri_v3_list, fmri_v3_file)

    def load_keyvalue_data(self, data_file):
        data = dict()

        for line in fileinput.input(data_file):
            line = line.strip()
            if ',' in line:
                stringline = line.split(',')
            else:
                stringline = line.split('=')

            if len(stringline) == 2:
                # add the value in the dictionary
                data[stringline[0]] = stringline[1]
            elif len(stringline) == 3:
                # add the value in the dictionary
                data[stringline[0]] = stringline[2]

        return data

    def load_stats_data(self, res):
        _stats = {}
        _dir = tempfile.mkdtemp()
        print('DEBUG:tmpdir=' + _dir)

        # Download the files and load the data
        try:
            file_list = res.get(_dir, extract=True)
            for f in file_list:
                print('DEBUG:f=' + f)
                _stats.update(self.load_keyvalue_data(f))
        except BadZipfile:
            print('DEBUG:bad zip file')

        return _stats

    def load_assr_data(self, xnat, assr):
        res_name = 'STATS'
        res = XnatUtils.get_full_object(xnat, assr).out_resource(res_name)

        if not res.exists():
            assr['NOTES'] = 'No stats resource'
        else:
            _stats = self.load_stats_data(res)
            assr.update(_stats)

        # Match each assr with a scan and append the scantype
        labels = assr['label'].split('-x-')
        scan_obj = xnat.select(
            '/projects/' + labels[0] +
            '/subjects/' + labels[1] +
            '/experiments/' + labels[2] +
            '/scans/' + labels[3]
        )

        if not scan_obj.exists() and assr['inputs']:
            # Try to interpret scan from inputs field
            assr_inputs = json.loads(assr['inputs'].replace('&quot;', '"'))
            _key, _val = assr_inputs.popitem()
            scan_id = _val.split('/')[-1]
            labels = assr['label'].split('-x-')
            scan_obj = xnat.select(
                '/projects/' + labels[0] +
                '/subjects/' + labels[1] +
                '/experiments/' + labels[2] +
                '/scans/' + scan_id
            )

        if scan_obj.exists():
            assr['scan_type'] = scan_obj.attrs.get('type')
            assr['scan_descrip'] = scan_obj.attrs.get('series_description')
        else:
            print('DEBUG:cannot determine scan id')

        return assr

    def extract_stats_data(self, xnat, assr_list, out_file):
        df = pd.DataFrame()

        # Build row for each
        for assr in assr_list:
            # Append data from stats file
            assr = self.load_assr_data(xnat, assr)
            df = df.append(assr, ignore_index=True)

        # Write the final file
        df.to_csv(out_file, sep=',', index=False)

    def clean_values(self, row):
        # Clean up memory used to be just number of megabytes
        try:
            if str(row['memused']).endswith('mb'):
                row['memused(MB)'] = row['memused'][0:-2]
            else:
                row['memused(MB)'] = math.ceil(float(row['memused']) / 1024)
        except ValueError:
            row['memused(MB)'] = 1

        # Cleanup wall time used to just be number of minutes
        try:
            if '-' in str(row['walltimeused']):
                t = datetime.strptime(str(row['walltimeused']), '%j-%H:%M:%S')
                delta = timedelta(
                    days=t.day,
                    hours=t.hour, minutes=t.minute, seconds=t.second)
            else:
                t = datetime.strptime(str(row['walltimeused']), '%H:%M:%S')
                delta = timedelta(
                    hours=t.hour, minutes=t.minute, seconds=t.second)

            startdate = datetime.strptime(str(row['jobstartdate']), '%Y-%m-%d')
            enddate = startdate + delta
            row['datetime'] = datetime.strftime(enddate, self.DFORMAT)
            row['timeused(min)'] = math.ceil(delta.total_seconds() / 60)
        except ValueError:
            row['timeused(min)'] = 1
            if row['jobstartdate']:
                row['datetime'] = row['jobstartdate']
            else:
                row['datetime'] = self.nowtime

        return row

    def load_data(self):
        # Input files
        datadir = self.datadir
        assr_file = sorted(glob(datadir + '/assrdata*.csv'), reverse=True)[0]
        scan_file = sorted(glob(datadir + '/scandata*.csv'), reverse=True)[0]
        fmri_file = sorted(glob(datadir + '/fmriqa*.csv'), reverse=True)[0]
        lst_file = sorted(glob(datadir + '/lst*.csv'), reverse=True)[0]

        print('FILEDIR=' + datadir)
        print('ASSR_FILE=' + assr_file)
        print('SCAN_FILE=' + scan_file)
        print('FMRI_FILE=' + fmri_file)
        print('LST_FILE=' + lst_file)

        if self.config['use_squeue']:
            _file = sorted(glob(datadir + '/squeue*.txt'), reverse=True)[0]
            print('SQUEUE_FILE=' + _file)

            # Load jobs
            _cols = ['NAME', 'USER', 'TIME', 'ST', 'START_TIME', 'JOBID']
            self.squeue_df = pd.read_csv(_file, delimiter='|', usecols=_cols)
            self.squeue_df.rename(
                columns={
                    'NAME': 'name', 'USER': 'user', 'ST': 'state',
                    'TIME': 'elapsed_time', 'START_TIME': 'start_time',
                    'JOBID': 'jobid'
                }, inplace=True)
        else:
            self.squeue_df = None

        # Load assessors
        _cols = [
            'proctype', 'label', 'qcstatus', 'project_id', 'session_label',
            'procstatus', 'jobstartdate', 'jobid', 'memused', 'version',
            'walltimeused']
        self.assr_df = pd.read_csv(assr_file, usecols=_cols)
        if self.config['assr_types'] != 'all':
            # Filter assr types included
            self.assr_df = self.assr_df[
                self.assr_df['proctype'].isin(self.config['assr_types'])]

        self.assr_df['qcstatus'].replace({
            'Passed': 'P', 'passed': 'P',
            'Questionable': 'P',
            'Failed': 'F',
            'Needs QA': 'Q'},
            inplace=True)
        self.assr_df.loc[
            (self.assr_df['qcstatus'].str.len() > 1), 'qcstatus'] = 'J'
        self.assr_df.rename(columns={
            'project_id': 'project', 'session_label': 'session'}, inplace=True)
        self.assr_df['name'] = self.assr_df['label'] + '.slurm'
        self.assr_dfp = self.assr_df.pivot_table(
            index=('session', 'project'),
            columns='proctype', values='qcstatus',
            aggfunc=lambda q: ''.join(q))
        self.assr_dfp.reset_index(inplace=True)

        # Load fmri
        self.fmri_df = pd.read_csv(fmri_file)
        self.fmri_df.rename(
            columns={
                'project_id': 'project',
                'session_label': 'session',
                'fmriqa_v3_voxel_displacement_median': 'displace_median',
                'fmriqa_v3_voxel_displacement_95prctile': 'displace_95',
                'fmriqa_v3_voxel_displacement_99prctile': 'displace_99',
                'fmriqa_v3_signal_delta_95prctile': 'sig_delta_95',
                'fmriqa_v3_global_timeseries_stddev': 'glob_ts_stddev',
                'fmriqa_v3_tsnr_95prctile': 'tsnr_95',
                'fmriqa_v3_tsnr_median': 'tsnr_median'
            }, inplace=True)

        # Load LST
        self.lst_df = pd.read_csv(lst_file)
        self.lst_df.rename(
            columns={
                'project_id': 'project',
                'session_label': 'session'
            }, inplace=True)

        # Load scan data
        _cols = ['session_label', 'project_id', 'quality', 'type']
        self.scan_df = pd.read_csv(scan_file, usecols=_cols)
        if self.config['scan_types'] != 'all':
            # Filter scan types to include
            self.scan_df = self.scan_df[self.scan_df['type'].isin(
                self.config['scan_types'])]

        self.scan_df['quality'].replace(
            {'usable': 'P', 'unusable': 'F', 'questionable': 'Q'},
            inplace=True)
        self.scan_df.rename(
            columns={
                'project_id': 'project',
                'session_label': 'session'
            }, inplace=True)
        self.scan_dfp = self.scan_df.pivot_table(
            index=('session', 'project'),
            columns='type', values='quality',
            aggfunc=lambda x: ''.join(x))
        self.scan_dfp.reset_index(inplace=True)

        # Merge assr and squeue
        if self.config['use_squeue']:
            self.task_df = pd.merge(
                self.assr_df, self.squeue_df, how='outer', on='name')
        else:
            self.task_df = self.assr_df

        # Filter out tasks we want to ignore
        self.task_df = self.task_df[~self.task_df.procstatus.isin(
            self.TASK_IGNORE_LIST)]

        # Apply the clean values
        self.task_df = self.task_df.apply(self.clean_values, axis=1)

        # Minimize columns
        self.task_df = self.task_df[self.TASK_COLS]

    def build_app(self):
        stats_types = self.config['stats_types']

        assr_proj_list = sorted(self.assr_df.project.unique())
        assr_type_list = sorted(self.assr_df.proctype.unique())
        assr_proj_options = [{'label': x, 'value': x} for x in assr_proj_list]
        assr_type_options = [{'label': x, 'value': x} for x in assr_type_list]
        assr_cols = ['session', 'project'] + list(assr_type_list)

        task_proj_list = sorted(self.task_df.project.unique())
        task_proc_list = sorted(self.task_df.proctype.unique())
        task_proj_options = [{'label': x, 'value': x} for x in task_proj_list]
        task_proc_options = [{'label': x, 'value': x} for x in task_proc_list]

        scan_proj_list = sorted(self.scan_df.project.unique())
        scan_type_list = sorted(self.scan_df.type.unique())
        scan_stat_list = sorted(['Passed', 'Needs QA', 'Failed'])
        scan_proj_options = [{'label': x, 'value': x} for x in scan_proj_list]
        scan_type_options = [{'label': x, 'value': x} for x in scan_type_list]
        scan_stat_options = [{'label': x, 'value': x} for x in scan_stat_list]
        scan_cols = ['session', 'project'] + list(scan_type_list)

        fmri_proj_list = sorted(self.fmri_df.project.unique())
        fmri_type_list = sorted(self.fmri_df.scan_type.unique())
        fmri_stat_list = sorted(self.fmri_df.qcstatus.unique())
        fmri_cols = [
            'label', 'project', 'session', 'qcstatus', 'scan_type',
            'displace_median',
            'displace_95',
            'displace_99',
            'sig_delta_95',
            'tsnr_95',
            'tsnr_median'
        ]
        fmri_proj_options = [{'label': x, 'value': x} for x in fmri_proj_list]
        fmri_type_options = [{'label': x, 'value': x} for x in fmri_type_list]
        fmri_stat_options = [{'label': x, 'value': x} for x in fmri_stat_list]

        lst_cols = [
            'label', 'project', 'session', 'qcstatus', 'wml_volume'
        ]
        lst_proj_list = sorted(self.lst_df.project.unique())
        lst_stat_list = sorted(self.lst_df.qcstatus.unique())
        lst_proj_options = [{'label': x, 'value': x} for x in lst_proj_list]
        lst_stat_options = [{'label': x, 'value': x} for x in lst_stat_list]

        # Make the main dash app
        if self.server:
            app = dash.Dash('DAX Dashboard', server=self.server)
        else:
            app = dash.Dash('DAX Dashboard')

        app.title = 'DAX Dashboard'
        self.app = app

        # If you are assigning callbacks to components
        # that are generated by other callbacks
        # (and therefore not in the initial layout), then
        # you can suppress this exception by setting
        app.config['suppress_callback_exceptions'] = True

        # Build the gui
        app.layout = html.Div([
            html.Div(
                dcc.Tabs(
                    tabs=[
                        {'label': 'Processing', 'value': 1},
                        {'label': 'Jobs', 'value': 2},
                        {'label': 'fMRIQA', 'value': 3},
                        {'label': 'Scans', 'value': 5},
                        {'label': 'LST', 'value': 6},
                    ],
                    value=1,
                    id='tabs',
                    vertical=False
                ),
            ),
            html.Div(id='tab-output'),
            html.Div(dt.DataTable(rows=[{}]), style={'display': 'none'})
        ])

        @app.callback(
            Output('tab-output', 'children'),
            [Input('tabs', 'value')])
        def display_content(value):
            if value == 2:
                return html.Div([
                    dcc.Graph(id='graph-task'),
                    dcc.Dropdown(id='dropdown-task-time', options=[
                        {'label': '1 day', 'value': 0},
                        {'label': '1 week', 'value': 1},
                        {'label': '1 month', 'value': 2},
                        {'label': '1 year', 'value': 3},
                        {'label': 'All time', 'value': 4}], value=2),
                    dcc.Dropdown(
                        id='dropdown-task-proj', multi=True,
                        options=task_proj_options, placeholder='All projects'),
                    dcc.Dropdown(
                        id='dropdown-task-proc', multi=True,
                        options=task_proc_options,
                        placeholder='All processing types'),
                    dt.DataTable(
                        rows=self.task_df.to_dict('records'),
                        columns=self.TASK_COLS,
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='datatable-task'),
                    html.Div(id='selected-indexes')], className="container")
            elif value == 3:
                return html.Div([
                    dcc.Graph(
                        id='graph-fmri'),
                    dcc.Dropdown(
                        id='dropdown-fmri-proj', multi=True,
                        options=fmri_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-fmri-type', multi=True,
                        options=fmri_type_options,
                        placeholder='Select scan type(s)'),
                    dcc.Dropdown(
                        id='dropdown-fmri-stat', multi=True,
                        options=fmri_stat_options,
                        placeholder='Select qc status'),
                    dt.DataTable(
                        rows=self.fmri_df.to_dict('records'),
                        columns=fmri_cols,  # specifies order of columns
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='datatable-fmri'),
                    html.Div(id='selected-indexes-fmri'),
                ], className="container")
            elif value == 5:
                return html.Div([
                    dcc.Graph(
                        id='graph-scan'),
                    dcc.Dropdown(
                        id='dropdown-scan-proj', multi=True,
                        options=scan_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-scan-type', multi=True,
                        options=scan_type_options,
                        placeholder='Select scan type(s)'),
                    dcc.Dropdown(
                        id='dropdown-scan-stat', multi=True,
                        options=scan_stat_options,
                        placeholder='Select status'),
                    dt.DataTable(
                        rows=self.scan_dfp.to_dict('records'),
                        columns=scan_cols,  # specifies order of columns
                        filterable=True,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='datatable-scan'),
                    html.Div(id='selected-indexes-scan'),
                ], className="container")
            elif value == 6 and 'LST_v1' in stats_types:
                return html.Div([
                    dcc.Graph(
                        id='graph-lst'),
                    dcc.Dropdown(
                        id='dropdown-lst-proj', multi=True,
                        options=lst_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-lst-stat', multi=True,
                        options=lst_stat_options,
                        placeholder='Select qc status'),
                    dt.DataTable(
                        rows=self.lst_df.to_dict('records'),
                        columns=lst_cols,  # specifies order of columns
                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='datatable-lst'),
                    html.Div(id='selected-indexes-lst'),
                ], className="container")
            elif value == 1:
                return html.Div([
                    dcc.Graph(
                        id='graph-both'),
                    dcc.Dropdown(
                        id='dropdown-both-proj', multi=True,
                        options=assr_proj_options,
                        placeholder='Select project(s)'),
                    dcc.Dropdown(
                        id='dropdown-both-type', multi=True,
                        options=assr_type_options,
                        placeholder='Select type(s)'),
                    dt.DataTable(
                        rows=self.assr_dfp.to_dict('records'),
                        columns=assr_cols,  # specifies order of columns
                        filterable=False,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='datatable-both')
                ], className="container")

        @app.callback(
            Output('datatable-task', 'rows'),
            [Input('dropdown-task-time', 'value'),
             Input('dropdown-task-proj', 'value'),
             Input('dropdown-task-proc', 'value')])
        def update_rows(selected_time, selected_proj, selected_proc):
            DFORMAT = self.DFORMAT

            # Filter by time
            if selected_time == 0:
                _prevtime = self.starttime - timedelta(days=1)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            elif selected_time == 1:
                _prevtime = self.starttime - timedelta(days=7)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            elif selected_time == 2:
                _prevtime = self.starttime - timedelta(days=30)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            elif selected_time == 3:
                _prevtime = self.starttime - timedelta(days=365)
                fdate = datetime.strftime(_prevtime, DFORMAT)
            else:
                fdate = '1969-12-31'

            dff = self.task_df[(self.task_df['datetime'] > fdate)]

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by proctype
            if selected_proc:
                dff = dff[dff['proctype'].isin(selected_proc)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-task', 'figure'),
            [Input('datatable-task', 'rows'),
             Input('datatable-task', 'selected_row_indices')])
        def update_figure(rows, selected_row_indices):
            # Make dict of colors by status
            stat2color = {
                'COMPLETE': 'rgba(0,255,0,0.5)',
                'JOB_FAILED': 'rgba(255,0,0,0.5)',
                'JOB_RUNNING': 'rgba(0,0,255,0.5)',
                'UPLOADING': 'rgba(255,0,255,0.5)'}

            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Plot trace for each status
            for i in dff.procstatus.unique():
                # Filter data by status
                dft = dff[dff.procstatus == i]

                # Match status to main color
                try:
                    color = stat2color[i]
                except KeyError:
                    color = 'rgba(0,0,0,0.5)'

                # Line color
                line = 'rgba(50,50,50,0.9)'

                # Add trace to figure
                fig.append_trace({
                    'name': '{} ({})'.format(i, len(dft)),
                    'x': dft['datetime'],
                    'y': dft['timeused(min)'],
                    'text': dft['label'],
                    'mode': 'markers',
                    'marker': dict(
                        color=color, size=10, line=dict(width=1, color=line))
                }, 1, 1)

            # Customize figure
            fig['layout'].update(
                yaxis=dict(type='log', title='minutes used'),
                hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('graph-fmri', 'figure'),
            [Input('datatable-fmri', 'rows'),
             Input('datatable-fmri', 'selected_row_indices')])
        def update_figure_fmri(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x4 figure
            fig = plotly.tools.make_subplots(rows=1, cols=4)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Add traces to figure
            fig.append_trace(
                go.Box(
                    y=dff.displace_95,
                    name='displace_95',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 1)

            fig.append_trace(
                go.Box(
                    y=dff.displace_median,
                    name='displace_median',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 2)

            fig.append_trace(
                go.Box(
                    y=dff.displace_99,
                    name='displace_99',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 3)

            fig.append_trace(
                go.Box(
                    y=dff.sig_delta_95,
                    name='sig_95',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 4)

            # Customize figure
            fig['layout'].update(hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('datatable-fmri', 'rows'),
            [Input('dropdown-fmri-proj', 'value'),
             Input('dropdown-fmri-type', 'value'),
             Input('dropdown-fmri-stat', 'value')])
        def update_rows_fmri(selected_proj, selected_type, selected_stat):
            dff = self.fmri_df

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by scan type
            if selected_type:
                dff = dff[dff['scan_type'].isin(selected_type)]

            # Filter by status
            if selected_stat:
                dff = dff[dff['qcstatus'].isin(selected_stat)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-scan', 'figure'),
            [Input('datatable-scan', 'rows'),
             Input('datatable-scan', 'selected_row_indices')])
        def update_figure_scan(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            # Check for empty data
            if len(dff) == 0:
                return fig

            ydata = dff.sort_values(
                'project').groupby('project')['session'].count()

            fig.append_trace(
                go.Bar(
                    x=sorted(dff.project.unique()),
                    y=ydata,
                    name='counts',
                ), 1, 1)

            return fig

        @app.callback(
            Output('datatable-scan', 'rows'),
            [Input('dropdown-scan-proj', 'value'),
             Input('dropdown-scan-type', 'value'),
             Input('dropdown-scan-stat', 'value')])
        def update_rows_scan(selected_proj, selected_type, selected_stat):
            dff = self.scan_dfp

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            if selected_type:
                for t in selected_type:
                    # Filter to include anything with at least one P or Q
                    dff = dff[(dff[t].str.contains(
                        'P', na=False)) | (dff[t].str.contains('Q', na=False))]

            return dff.to_dict('records')

        @app.callback(
            Output('datatable-both', 'rows'),
            [Input('dropdown-both-proj', 'value'),
             Input('dropdown-both-type', 'value')])
        def update_rows_both(selected_proj, selected_type):
            dff = self.assr_dfp

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            return dff.to_dict('records')

        @app.callback(
            Output('graph-both', 'figure'),
            [Input('datatable-both', 'rows'),
             Input('dropdown-both-proj', 'value'),
             Input('dropdown-both-type', 'value')])
        def update_figure_both(rows, selected_proj, selected_type):
            dfp = pd.DataFrame(rows)
            xall = sorted(dfp.project.unique())
            yall = dfp.sort_values(
                'project').groupby('project')['session'].count()
            yred = [0] * len(assr_proj_options)
            ygreen = [0] * len(assr_proj_options)
            ygrey = [0] * len(assr_proj_options)
            yyell = [0] * len(assr_proj_options)

            # Make a 1x1 figured
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            if not selected_type:
                # Draw bar
                fig.append_trace(go.Bar(
                    x=xall, y=yall, name='All',
                    marker=dict(color='rgb(59,89,152)')
                ), 1, 1)

            else:
                # TODO: optimize this cause it's ugly
                for i, proj in enumerate(xall):
                    # Subset for this project
                    dfpp = dfp.loc[dfp['project'] == proj]
                    # Iterate by session
                    for s, sess in dfpp.iterrows():
                        cur = 0
                        for t in selected_type:
                            if not sess[t]:
                                cur = 3
                                break
                            elif 'F' in sess[t] and cur <= 2:
                                cur = 2
                            elif (('Q' in sess[t] or 'J' in sess[t]) and
                                    cur < 1):
                                cur = 1

                        # Record final value across all types
                        if cur == 3:
                            ygrey[i] += 1
                        elif cur == 2:
                            yred[i] += 1
                        elif cur == 1:
                            yyell[i] += 1
                        else:
                            ygreen[i] += 1

                # Draw bar for each status
                fig.append_trace(go.Bar(
                    x=xall, y=ygreen, name='Passed',
                    marker=dict(color='rgb(27,157,5)'),
                    opacity=0.9), 1, 1)

                fig.append_trace(go.Bar(
                    x=xall, y=yyell, name='Needs QA',
                    marker=dict(color='rgb(240,240,30)'),
                    opacity=0.9), 1, 1)

                fig.append_trace(go.Bar(
                    x=xall, y=yred, name='Failed',
                    marker=dict(color='rgb(200,0,0)'),
                    opacity=0.9), 1, 1)

                fig.append_trace(go.Bar(
                    x=xall, y=ygrey, name='None',
                    marker=dict(color='rgb(200,200,200)'),
                    opacity=0.9), 1, 1)

                # Customize figure
                fig['layout'].update(barmode='stack', showlegend=True)

            return fig

        @app.callback(
            Output('graph-lst', 'figure'),
            [Input('datatable-lst', 'rows'),
             Input('datatable-lst', 'selected_row_indices')])
        def update_figure_lst(rows, selected_row_indices):
            # Load data from input
            dff = pd.DataFrame(rows)

            # Make a 1x1 figure
            fig = plotly.tools.make_subplots(rows=1, cols=1)

            # Check for empty data
            if len(dff) == 0:
                return fig

            # Add traces to figure
            fig.append_trace(
                go.Box(
                    y=dff.wml_volume,
                    name='wml_volume',
                    boxpoints='all',
                    text=dff.label,
                ), 1, 1)

            # Customize figure
            fig['layout'].update(hovermode='closest', showlegend=True)

            return fig

        @app.callback(
            Output('datatable-lst', 'rows'),
            [Input('dropdown-lst-proj', 'value'),
             Input('dropdown-lst-stat', 'value')])
        def update_rows_lst(selected_proj, selected_stat):
            dff = self.lst_df

            # Filter by project
            if selected_proj:
                dff = dff[dff['project'].isin(selected_proj)]

            # Filter by status
            if selected_stat:
                dff = dff[dff['qcstatus'].isin(selected_stat)]

            return dff.to_dict('records')

    def get_app(self):
        return self.app

    def run(self, host='0.0.0.0'):
        if self.server:
            self.app.run_server()
            print('DEBUG:running app on server')
        else:
            print('DEBUG:running app on host:' + host)
            self.app.run_server(host=host)
