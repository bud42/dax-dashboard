import logging

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from dash.dependencies import Input, Output
import dash

from app import app
from . import opsdata
from . import utils


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

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


def filter_jobs_data(df, projects, proctypes, user):
    print('filtering before size=', len(df))
    print('filtering projects', projects)
    print('filtering proctypes', proctypes)
    print('filtering user', user)

    # Filter by project
    if projects:
        logging.debug('filtering by project:')
        logging.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    if proctypes:
        logging.debug('filtering by proctypes:')
        logging.debug(proctypes)
        df = df[df['PROCTYPE'].isin(proctypes)]

    if user:
        df = df[df['USER'].isin(user)]

    print('filtering after size=', len(df))
    return df


def get_job_graph_content(df):
    PIVOTS = ['USER', 'PROJECT', 'PROCTYPE']
    tabs_content = []

    print('df size=', len(df))

    # index are we pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):
        logging.debug('making graph:' + str(i) + ',' + str(pindex))
        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df, index=pindex, values='LABEL', columns=['STATUS'],
            aggfunc='count', fill_value=0)

        for status, color in zip(STATUS_LIST, COLOR_LIST):
            logging.debug('plotting bar:'+status+','+color)
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
        logging.debug('tab finished')
        tabs_content.append(tab)

    return tabs_content


def get_job_content(df):
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


def get_layout():
    logging.debug('get_layout')

    job_content = get_job_content(load_data())

    report_content = [
        html.Div(
            dcc.Tabs(id='tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='Job Queue', value='1', children=job_content),
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


def load_data():
    return opsdata.load_data()


def refresh_data():
    logging.debug('refresh_data')
    return opsdata.refresh_data()


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


@app.callback(
    [Output('dropdown-job-proc', 'options'),
     Output('dropdown-job-proj', 'options'),
     Output('dropdown-job-user', 'options'),
     Output('datatable-job', 'data'),
     Output('tabs-job', 'children')],
    [Input('dropdown-job-proc', 'value'),
     Input('dropdown-job-proj', 'value'),
     Input('dropdown-job-user', 'value'),
     Input('button-job-refresh', 'n_clicks')])
def update_everything(
        selected_proc,
        selected_proj,
        selected_user,
        n_clicks
):
    logging.debug('update_all')

    # Load the data
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-job-refresh'):
        # Refresh data if refresh button clicked
        logging.debug('refresh:clicks={}'.format(n_clicks))
        df = refresh_data()
    else:
        print('load data')
        df = load_data()

    # Get the dropdown options
    proc = utils.make_options(df.PROCTYPE.unique())
    proj = utils.make_options(df.PROJECT.unique())
    user = utils.make_options(df.USER.unique())

    print(user, proc, proj)

    logging.debug('applying data filters')
    df = filter_jobs_data(
        df,
        selected_proj,
        selected_proc,
        selected_user)

    logging.debug('getting job graph content')
    tabs = get_job_graph_content(df)

    # Extract records from dataframe
    records = df.to_dict('records')

    # Return dropdown options, table, figure
    logging.debug('update_everything:returning data')
    return [proc, proj, user, records, tabs]


# Build the layout that will used by top level index.py
layout = get_layout()
