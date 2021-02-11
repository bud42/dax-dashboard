import logging
from datetime import datetime

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
from . import statsdata
from . import utils


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

# These are used to set colors of graphs
RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELLOW = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_PURPLE = 'rgb(160,106,255)'
RGB_GREY = 'rgb(200,200,200)'

# These can be used to set color of html tables via style argument
HEX_LBLUE = '#DAEBFF'
HEX_LGREE = '#DCFFDA'
HEX_LYELL = '#FFE4B3'
HEX_LREDD = '#FFDADA'
HEX_LGREY = '#EBEBEB'
HEX_LPURP = '#D1C0E5'

# Give each status a color to display
STATUS2COLOR = {
    'PASS': RGB_GREEN,
    'TBD': RGB_YELLOW,
    'FAIL': RGB_RED,
    'NONE': RGB_GREY}

DEFAULT_COLOR = 'rgba(0,0,0,0.5)'

LINE_COLOR = 'rgba(50,50,50,0.9)'


def filter_stats_data(df, projects, proctypes, timeframe, sesstype):
    # Filter by project
    if projects:
        logging.debug('filtering by project:')
        logging.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by proctype
    if proctypes:
        logging.debug('filtering by proctypes:')
        logging.debug(proctypes)
        df = df[df['TYPE'].isin(proctypes)]

    # Filter by timeframe
    if timeframe in ['1day', '7day', '30day', '365day']:
        logging.debug('filtering by ' + timeframe)
        then_datetime = datetime.now() - pd.to_timedelta(timeframe)
        df = df[pd.to_datetime(df.DATE) > then_datetime]
    else:
        # ALL
        logging.debug('not filtering by time')
        pass

    # Filter by sesstype
    if sesstype == 'baseline':
        logging.debug('filtering by baseline only')
        df = df[df['ISBASELINE']]
    elif sesstype == 'followup':
        logging.debug('filtering by followup only')
        df = df[~df['ISBASELINE']]
    else:
        logging.debug('not filtering by sesstype')
        pass

    return df


def get_stats_graph_content(df):
    tabs_content = []
    tab_value = 0

    var_list = ['accuracy', 'RT', 'trials']  # EDATQA
    var_list.extend(['WML'])  # LST
    var_list.extend(['VOXD', 'DVARS'])  # fmriqa
    var_list.extend(['compgm_suvr'])  # amyvidqa

    logging.debug('get_stats_figure')

    # Determine how many boxplots we're making, depends on how many vars
    box_count = len(var_list)

    # Make a figure
    fig = plotly.subplots.make_subplots(rows=1, cols=box_count)

    # Check for empty data
    if len(df) == 0:
        logging.debug('empty data, using empty figure')
        return [fig]

    # box plots
    # each proctype has specific set of fields to plot,
    # vwe need a dictionary listing them
    # then we just do the boxplots for the chosen proctypes (and maybe scan
    # types?, how did we do that for fmri scan type?)

    # Add traces to figure
    for i, var in enumerate(var_list):
        print(i, 'var', var)
        if pd.isnull(df[var]).all():
            continue

        fig.append_trace(go.Box(
            y=df[var],
            name=var,
            boxpoints='all',
            text=df.assessor_label,
            ), 1, i + 1)

    # Customize figure
    fig.update_layout(autosize=True, margin=dict(l=40, r=40, t=40, b=40))

    # Build the tab
    label = 'ALL'
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tab_value += 1

    # Append the tab
    tabs_content.append(tab)

    # Return the tabs
    return tabs_content


def get_stats_content(df):
    stats_graph_content = get_stats_graph_content(df)

    # Get the rows and colums for the table
    stats_columns = [{"name": i, "id": i} for i in df.columns]
    df.reset_index(inplace=True)
    stats_data = df.to_dict('rows')

    stats_content = [
        dcc.Loading(id="loading-stats", children=[
            html.Div(dcc.Tabs(
                id='tabs-stats',
                value='0',
                children=stats_graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-stats-refresh'),
        dcc.Dropdown(
            id='dropdown-stats-time',
            options=[
                {'label': 'all time', 'value': 'ALL'},
                {'label': '1 day', 'value': '1day'},
                {'label': '1 week', 'value': '7day'},
                {'label': '1 month', 'value': '30day'},
                {'label': '1 year', 'value': '365day'}],
            value='ALL'),
        dcc.Dropdown(
            id='dropdown-stats-proj', multi=True,
            placeholder='Select Project(s)'),
        dcc.Dropdown(
            id='dropdown-stats-proc', multi=True,
            placeholder='Select Type(s)'),
        dcc.RadioItems(
            options=[
                {'label': 'All Sessions', 'value': 'all'},
                {'label': 'Baseline Only', 'value': 'baseline'},
                {'label': 'Followup Only', 'value': 'followup'}],
            value='all',
            id='radio-stats-sesstype',
            labelStyle={'display': 'inline-block'}),
        dt.DataTable(
            columns=stats_columns,
            data=stats_data,
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-stats',
            style_table={'overflowY': 'scroll', 'overflowX': 'scroll'},
            style_cell={
                'textAlign': 'left',
                'padding': '5px 5px 0px 5px',
                'width': '30px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'height': 'auto',
                'minWidth': '40',
                'maxWidth': '60'},
            style_header={
                'width': '80px',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            fill_width=True,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible')]

    return stats_content


def get_layout():
    logging.debug('get_layout')

    stats_content = get_stats_content(load_data())

    report_content = [
        html.Div(
            dcc.Tabs(id='tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='STATS', value='1', children=stats_content)
            ]),
            style={
                'width': '100%', 'display': 'flex',
                'align-items': 'center', 'justify-content': 'left'})]

    footer_content = [
        html.Hr(),
        html.H5('F: Failed'),
        html.H5('P: Passed QA'),
        html.H5('Q: To be determined')]

    return html.Div([
        html.Div(children=report_content, id='report-content'),
        html.Div(children=footer_content, id='footer-content')])


def load_data():
    return statsdata.load_data()


def refresh_stats_data():
    logging.debug('refresh_qa_data calling dashdata.refresh_data()')
    return statsdata.refresh_data()


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


# Now we initialize the callbacks for the app

# With more recent dash, we can set multiple inputs AND multiple
# outputs instead of having to create a new callback per output.
# update_all() handles all of the data tab interface

# inputs:
# values from assr proc types dropdown
# values from project dropdown
# values from timeframe dropdown
# number of clicks on refresh button

# returns:
# options for the assessor proc types dropdown
# options for the assessor projects dropdown
# data for the table
# content for the graph tabs
@app.callback(
    [Output('dropdown-stats-proc', 'options'),
     Output('dropdown-stats-proj', 'options'),
     Output('datatable-stats', 'data'),
     Output('datatable-stats', 'columns'),
     Output('tabs-stats', 'children')],
    [Input('dropdown-stats-proc', 'value'),
     Input('dropdown-stats-proj', 'value'),
     Input('dropdown-stats-time', 'value'),
     Input('radio-stats-sesstype', 'value'),
     Input('button-stats-refresh', 'n_clicks')])
def update_all(
    selected_proc,
    selected_proj,
    selected_time,
    selected_sesstype,
    n_clicks
):
    logging.debug('update_all')

    # Load our data
    # This data will already be merged scans and assessors with
    # a row per scan or assessor
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-stats-refresh'):
        # Refresh data if refresh button clicked
        logging.debug('refresh:clicks={}'.format(n_clicks))
        df = refresh_stats_data()
    else:
        df = load_data()

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    proc = utils.make_options(df.TYPE.unique())
    proj = utils.make_options(sorted(df.PROJECT.unique()))

    # Filter data based on dropdown values
    df = filter_stats_data(
        df,
        selected_proj,
        selected_proc,
        selected_time,
        selected_sesstype)

    tabs = get_stats_graph_content(df)

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    records = df.reset_index().to_dict('records')
    columns = [{"name": i, "id": i} for i in df.reset_index().columns]

    return [proc, proj, records, columns, tabs]


# Build the layout that will used by top level index.py
layout = get_layout()
