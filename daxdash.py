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


# pip install dash
# pip install dash-renderer
# pip install dash-html-components
# pip install dash-core-components
# pip install dash-core-components==0.21.0rc1
# pip install dash_table_experiments
# pip install plotly --upgrade
# pip install xlrd

# More Dash stuff
# https://alysivji.github.io/reactive-dashboards-with-dash.html
# https://github.com/plotly/dash-recipes

# Dash Docker:
# https://github.com/TahaSolutions/dash/tree/master/Dash2
# https://github.com/JoaoCarabetta/viz-parallel

# Deplying Dash Apps:
# https://dash.plot.ly/deployment
# https://community.plot.ly/t/how-to-run-dash-on-a-public-ip/4796

# Link in chart
# https://community.plot.ly/t/open-url-in-new-tab-when-chart-is-clicked-on/3576/5

# Purposes
# 1. easily find problems that need manual intervention
# 2. overview of what data we have across projects
# 3. overview of what data needs to be inspected (QA'd)

# TODO: keep cache of data files so we can go back through time
# TODO: eventually have dax keep track of queue status on XNAT
# so we don't have to get directly from cluster
# TODO: per project counts of each type of assessor,
# or can this be on per session page?
# TODO: prearchive, auto-archive counts
# TODO: per session page with columns for scans and assessors with filters
# for project, scan type (passed), assr type (passed)
# TODO: get custom statuses for orphaned jobs, more specific accre statuses
# TODO: make sure all tasks appear somewhere
# TODO: at bottom of jobs report, show list of warnings about jobs that have
# been running for a long time, etc.
# TODO: show when data was updated, later have button to update data
# TODO: selecting rows in table affect graphs


# TODO: scans/assessors: if any are questionable
# count usable and questionable and yellow,
# if no questionable and any are usable, count the usable and green,
# else if all are unusable count the unusable and red
# if questcount > 0, usecount+questcount,yellow
# elif usecount > 0, usecount,green
# else: totcount,red

# update_data.py - periodcially updates data files
# write file with on dax build/update - since we are accessing the same data,
# we can just save it to a file

# Input files
FILEDIR = '/scratch/boydb1/UPLOADQ/DASHDATA'
#FILEDIR = '/Users/boydb1/Desktop/DASHDATA'
SQUEUE_FILE = sorted(glob(FILEDIR + '/squeue*.txt'), reverse=True)[0]
ASSR_FILE = sorted(glob(FILEDIR + '/assrdata*.csv'), reverse=True)[0]
FMRI_FILE = sorted(glob(FILEDIR + '/fmriqa*.csv'), reverse=True)[0]
SCAN_FILE = sorted(glob(FILEDIR + '/scandata*.csv'), reverse=True)[0]
# SETT_FILE = .yaml

print('FILEDIR=' + FILEDIR)
print('SQUEUE_FILE=' + SQUEUE_FILE)
print('ASSR_FILE=' + ASSR_FILE)
print('FMRI_FILE=' + FMRI_FILE)
print('SCAN_FILE=' + SCAN_FILE)

# Load jobs
USE_COLS = ['NAME', 'USER', 'TIME', 'ST', 'START_TIME', 'JOBID']
squeue_df = pd.read_csv(SQUEUE_FILE, delimiter='|', usecols=USE_COLS)
squeue_df.rename(
    columns={
        'NAME': 'name', 'USER': 'user', 'ST': 'state',
        'TIME': 'elapsed_time', 'START_TIME': 'start_time', 'JOBID': 'jobid'
    }, inplace=True)

# Load assessors
ASSR_COLS = [
    'proctype', 'label', 'qcstatus', 'project_id', 'session_label',
    'procstatus', 'jobstartdate', 'jobid', 'memused', 'version', 'walltimeused'
]
assr_df = pd.read_csv(ASSR_FILE, usecols=ASSR_COLS)
assr_df.rename(columns={
    'project_id': 'project', 'session_label': 'session'}, inplace=True)
assr_df['name'] = assr_df['label'] + '.slurm'
assr_dfp = assr_df.pivot_table(
    index=('session', 'project'),
    columns='proctype', values='qcstatus',
    aggfunc=lambda q: ''.join(q))
assr_dfp.reset_index(inplace=True)

ASSR_PROJ_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(assr_df.project.unique())]

ASSR_TYPE_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(assr_df.proctype.unique())]

ASSR_STAT_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(['Passed', 'Needs QA', 'Failed'])]

ASSR_COLS = ['session', 'project'] + list(assr_df.proctype.unique())

# Load fmri
fmri_df = pd.read_csv(FMRI_FILE)
fmri_df.rename(
    columns={
        'fmriqa_v3_voxel_displacement_median': 'displace_median',
        'fmriqa_v3_voxel_displacement_95prctile': 'displace_95',
        'fmriqa_v3_voxel_displacement_99prctile': 'displace_99',
        'fmriqa_v3_signal_delta_95prctile': 'sig_delta_95',
        'fmriqa_v3_global_timeseries_stddev': 'glob_ts_stddev',
        'fmriqa_v3_tsnr_95prctile': 'tsnr_95',
        'fmriqa_v3_tsnr_median': 'tsnr_median'
    }, inplace=True)
FMRI_COLS = [
    'label', 'project_id', 'session_label', 'qcstatus', 'scan_type',
    'displace_median',
    'displace_95',
    'displace_99',
    'sig_delta_95',
    'global_ts_stddev',
    'tsnr_95',
    'tsnr_median'
]
FMRI_PROJ_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(fmri_df.project_id.unique())
]
FMRI_TYPE_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(fmri_df.scan_type.unique())
]
FMRI_STAT_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(fmri_df.qcstatus.unique())
]


# Load scan data
def reduce_count(x):
    if x is None:
        return None
    elif 'Q' in x:
        return x.count('Q') + x.count('P')
    elif 'P' in x:
        return x.count('P')
    else:
        return x.count('F')


def reduce_quality(x):
    if x is None:
        return None
    elif 'Q' in x:
        return 1
    elif 'P' in x:
        return 2
    else:
        return 0


scan_df = pd.read_csv(
    SCAN_FILE, usecols=['session_label', 'project_id', 'quality', 'type'])

scan_dfp = scan_df.pivot_table(
    index=('session_label', 'project_id'), columns='type', values='quality',
    aggfunc=lambda x: ''.join(x))
scan_dfp.reset_index(inplace=True)

SCAN_PROJ_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(scan_df.project_id.unique())]

SCAN_TYPE_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(scan_df.type.unique())]

SCAN_STAT_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(['Passed', 'Needs QA', 'Failed'])]

SCAN_COLS = ['session_label', 'project_id'] + list(scan_df.type.unique())

if False:
    scan_quality = scan_df.applymap(reduce_quality)
    scan_count = scan_df.applymap(reduce_count)

# Make the task table
IGNORE_LIST = ['NO_DATA', 'NEED_INPUTS', 'Complete']


def clean_values(row):
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

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
        row['datetime'] = datetime.strftime(startdate + delta, DATE_FORMAT)
        row['timeused(min)'] = math.ceil(delta.total_seconds() / 60)
    except ValueError:
        row['timeused(min)'] = 1
        if row['jobstartdate']:
            row['datetime'] = row['jobstartdate']
        else:
            row['datetime'] = datetime.strftime(datetime.now(), DATE_FORMAT)

    return row


# Merge assr and squeue
task_df = pd.merge(assr_df, squeue_df, how='outer', on='name')
task_df = task_df[~task_df.procstatus.isin(IGNORE_LIST)]

# Apply the clean values
task_df = task_df.apply(clean_values, axis=1)

# Minimize columns
TASK_COLS = [
    'label', 'project', 'memused(MB)',
    'procstatus', 'proctype', 'datetime', 'timeused(min)'
]
task_df = task_df[TASK_COLS]

PROJ_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(task_df.project.unique())
]
PROC_OPTIONS = [
    {'label': x, 'value': x} for x in sorted(task_df.proctype.unique())
]

# Make the main dash app
app = dash.Dash()
app.title = 'DAX Dashboard'

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
                {'label': 'Assessors', 'value': 4},
                {'label': 'Scans', 'value': 5},
            ],
            value=1,
            id='tabs',
            vertical=False
        ),
    ),
    html.Div(id='tab-output'),
    html.Div(dt.DataTable(rows=[{}]), style={'display': 'none'})
])


@app.callback(Output('tab-output', 'children'), [Input('tabs', 'value')])
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
                options=PROJ_OPTIONS, placeholder='All projects'),
            dcc.Dropdown(
                id='dropdown-task-proc', multi=True, options=PROC_OPTIONS,
                placeholder='All processing types'),
            dt.DataTable(
                rows=task_df.to_dict('records'),
                columns=TASK_COLS,
                row_selectable=True,
                filterable=True,
                sortable=True,
                editable=False,
                selected_row_indices=[],
                id='datatable-task'),
            html.Div(id='selected-indexes')], className="container")
    elif value == 4:
        return html.Div([
            dcc.Graph(
                id='graph-assr'),
            dcc.Dropdown(
                id='dropdown-assr-proj', multi=True,
                options=ASSR_PROJ_OPTIONS,
                placeholder='All project'),
            dcc.Dropdown(
                id='dropdown-assr-type', multi=True,
                options=ASSR_TYPE_OPTIONS,
                placeholder='All processing types'),
            dcc.Dropdown(
                id='dropdown-assr-stat', multi=True,
                options=ASSR_STAT_OPTIONS,
                placeholder='Select status'),
            dt.DataTable(
                rows=assr_dfp.to_dict('records'),
                columns=ASSR_COLS,  # specifies order of columns
                filterable=True,
                sortable=True,
                id='datatable-assr')
        ], className="container")
    elif value == 3:
        return html.Div([
            dcc.Graph(
                id='graph-fmri'),
            dcc.Dropdown(
                id='dropdown-fmri-proj', multi=True,
                options=FMRI_PROJ_OPTIONS,
                placeholder='Select project(s)'),
            dcc.Dropdown(
                id='dropdown-fmri-type', multi=True,
                options=FMRI_TYPE_OPTIONS,
                placeholder='Select scan type(s)'),
            dcc.Dropdown(
                id='dropdown-fmri-stat', multi=True,
                options=FMRI_STAT_OPTIONS,
                placeholder='Select qc status'),
            dt.DataTable(
                rows=fmri_df.to_dict('records'),
                columns=FMRI_COLS,  # specifies order of columns
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
                options=SCAN_PROJ_OPTIONS,
                placeholder='Select project(s)'),
            dcc.Dropdown(
                id='dropdown-scan-type', multi=True,
                options=SCAN_TYPE_OPTIONS,
                placeholder='Select scan type(s)'),
            dcc.Dropdown(
                id='dropdown-scan-stat', multi=True,
                options=SCAN_STAT_OPTIONS,
                placeholder='Select status'),
            dt.DataTable(
                rows=scan_dfp.to_dict('records'),
                columns=SCAN_COLS,  # specifies order of columns
                filterable=True,
                sortable=True,
                editable=False,
                selected_row_indices=[],
                id='datatable-scan'),
            html.Div(id='selected-indexes-scan'),
        ], className="container")
    elif value == 1:
        return html.Div([
            dcc.Graph(
                id='graph-both'),
            dcc.Dropdown(
                id='dropdown-both-proj', multi=True,
                options=ASSR_PROJ_OPTIONS,
                placeholder='Select project(s)'),
            dcc.Dropdown(
                id='dropdown-both-type', multi=True,
                options=ASSR_TYPE_OPTIONS,
                placeholder='Select type(s)'),
            dt.DataTable(
                rows=assr_dfp.to_dict('records'),
                columns=ASSR_COLS,  # specifies order of columns
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
    FORMAT = '%Y-%m-%d %H:%M:%S'

    # Filter by time
    if selected_time == 0:
        fdate = datetime.strftime(datetime.now() - timedelta(days=1), FORMAT)
    elif selected_time == 1:
        fdate = datetime.strftime(datetime.now() - timedelta(days=7), FORMAT)
    elif selected_time == 2:
        fdate = datetime.strftime(datetime.now() - timedelta(days=30), FORMAT)
    elif selected_time == 3:
        fdate = datetime.strftime(datetime.now() - timedelta(days=365), FORMAT)
    else:
        fdate = '1969-12-31'

    dff = task_df[(task_df['datetime'] > fdate)]

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
    dff = fmri_df

    # Filter by project
    if selected_proj:
        dff = dff[dff['project_id'].isin(selected_proj)]

    # Filter by proctype
    if selected_type:
        dff = dff[dff['scan_type'].isin(selected_type)]

    # Filter by status
    if selected_stat:
        dff = dff[dff['qcstatus'].isin(selected_stat)]

    return dff.to_dict('records')


cscale = [
    ['0.0', 'rgb(225,50,50)'],
    ['0.5', 'rgb(235,235,25)'],
    ['1.0', 'rgb(0,125,0)']
]


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
        'project_id').groupby('project_id')['session_label'].count()

    fig.append_trace(
        go.Bar(
            x=sorted(dff.project_id.unique()),
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
    dff = scan_dfp

    # Filter by project
    if selected_proj:
        dff = dff[dff['project_id'].isin(selected_proj)]

    if selected_type:
        for t in selected_type:
            # Filter to include anything with at least one P or Q
            dff = dff[(dff[t].str.contains(
                'P', na=False)) | (dff[t].str.contains('Q', na=False))]

    return dff.to_dict('records')


@app.callback(
    Output('graph-assr', 'figure'),
    [Input('datatable-assr', 'rows')])
def update_figure_assr(rows):
    # Load data from input
    dff = pd.DataFrame(rows)

    # Make a 1x1 figured
    fig = plotly.tools.make_subplots(rows=1, cols=1)

    # Check for empty data
    if len(dff) == 0:
        return fig

    fig.append_trace(
        go.Bar(
            x=sorted(dff.project.unique()),
            y=dff.sort_values('project').groupby('project')['session'].count(),
            name='counts'), 1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack')
    return fig


@app.callback(
    Output('datatable-assr', 'rows'),
    [Input('dropdown-assr-proj', 'value'),
     Input('dropdown-assr-type', 'value'),
     Input('dropdown-assr-stat', 'value')])
def update_rows_assr(selected_proj, selected_type, selected_stat):
    dff = assr_dfp

    # Filter by project
    if selected_proj:
        dff = dff[dff['project'].isin(selected_proj)]

    if selected_type:
        for t in selected_type:
            # Filter to include anything with at least one P or Q
            dff = dff[
                (dff[t].str.contains('P', na=False)) |
                (dff[t].str.contains('Q', na=False))]

    return dff.to_dict('records')


@app.callback(
    Output('datatable-both', 'rows'),
    [Input('dropdown-both-proj', 'value'),
     Input('dropdown-both-type', 'value')])
def update_rows_both(selected_proj, selected_type):
    dff = assr_dfp

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
    yred = [0] * len(ASSR_PROJ_OPTIONS)
    ygreen = [0] * len(ASSR_PROJ_OPTIONS)
    ygrey = [0] * len(ASSR_PROJ_OPTIONS)
    yyell = [0] * len(ASSR_PROJ_OPTIONS)

    # Make a 1x1 figured
    fig = plotly.tools.make_subplots(rows=1, cols=1)

    if not selected_type:
        # Draw bar
        fig.append_trace(go.Bar(
            x=xall, y=yall, name='All',
            marker=dict(color='rgb59,89,152)')
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
                    elif ('Q' in sess[t] or 'J' in sess[t]) and cur < 1:
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
        fig.append_trace(go.Bar(x=xall, y=ygreen, name='Passed', marker=dict(
            color='rgb(27,157,5)'), opacity=0.9), 1, 1)

        fig.append_trace(go.Bar(x=xall, y=yyell, name='Needs QA', marker=dict(
            color='rgb(240,240,30)'), opacity=0.9), 1, 1)

        fig.append_trace(go.Bar(x=xall, y=yred, name='Failed', marker=dict(
            color='rgb(200,0,0)'), opacity=0.9), 1, 1)

        fig.append_trace(go.Bar(x=xall, y=ygrey, name='None', marker=dict(
            color='rgb(200,200,200)'), opacity=0.9), 1, 1)

        # Customize figure
        fig['layout'].update(barmode='stack', showlegend=True)

    return fig


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
