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
import utils
import stats.data as data


# need to build this dynamically, maybe from the params file or a yaml somehow
# use same method as qaparams.yaml, i.e. statsparams.yaml
# need a default list when you first load the tab and then a full list of
# everything allowed to add

# TODO: by default just use the first variable, then allow list is based
# VAR_LIST in params.py, which can be overriden in statsparams.yaml


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


def get_graph_content(df):
    tabs_content = []
    tab_value = 0
    box_width = 150
    min_box_count = 4

    # Check for empty data
    if len(df) == 0:
        logging.debug('empty data, using empty figure')
        return [plotly.subplots.make_subplots(rows=1, cols=1)]

    # Filter var list to only include those that have data
    # Do we need this filtering?
    #var_list = [x for x in VAR_LIST if x in df and not pd.isnull(df[x]).all()]
    #var_list = [x for x in df.columns if not pd.isnull(df[x]).all()]
    var_list  = df.columns
    print('var_list', var_list)

    logging.debug('get_stats_figure')

    # Determine how many boxplots we're making, depends on how many vars, use
    # minimum so graph doesn't get too small
    box_count = len(var_list)
    if box_count < min_box_count:
        box_count = min_box_count

    graph_width = box_width * box_count

    # Horizontal spacing cannot be greater than (1 / (cols - 1))
    hspacing = 1 / (box_count * 2)

    # Make the figure
    fig = plotly.subplots.make_subplots(
        rows=1, cols=box_count, horizontal_spacing=hspacing)

    # box plots
    # each proctype has specific set of fields to plot,
    # TODO: we need a dictionary listing them
    # then we just do the boxplots for the chosen proctypes (and maybe scan
    # types?, how did we do that for fmri scan type?)

    # Add traces to figure
    for i, var in enumerate(var_list):
        # Create boxplot for this var and add to figure
        fig.append_trace(
            go.Box(
                y=df[var],
                name=var,
                boxpoints='outliers',  # 'all'
                text=df.assessor_label),
            1,
            i + 1)

    # Customize figure
    fig.update_layout(
        showlegend=False,
        autosize=False,
        width=graph_width,
        margin=dict(l=20, r=0, t=40, b=40, pad=0))

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


def get_content():
    df = load_stats()
    stats_graph_content = get_graph_content(df)

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
            fill_width=False,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible')]

    return stats_content


def load_stats(refresh=False):
    return data.load_data(refresh=refresh)


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


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
def update_stats(
    selected_proc,
    selected_proj,
    selected_time,
    selected_sesstype,
    n_clicks
):
    refresh = False

    logging.debug('update_all')

    # Load our data
    # This data will already be merged scans and assessors with
    # a row per scan or assessor
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-stats-refresh'):
        # Refresh data if refresh button clicked
        logging.debug('refresh:clicks={}'.format(n_clicks))
        refresh = True

    # Load data with refresh if requested
    df = load_stats(refresh=refresh)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    proc = utils.make_options(df.TYPE.unique())
    proj = utils.make_options(df.PROJECT.unique())

    # Filter data based on dropdown values
    df = data.filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_time,
        selected_sesstype)

    # Get the graph content in tabs (currently only one tab)
    tabs = get_graph_content(df)

    # Get the table data
    selected_cols = ['assessor_label', 'PROJECT', 'SESSION', 'TYPE']

    if selected_proc:
        #var_list = [x for x in VAR_LIST if not pd.isnull(df[x]).all()]
        var_list = df.columns
        print('var_list', var_list)
        selected_cols += var_list
    else:
        # Nothing selected so grab the first three?
        selected_cols += var_list[0:3]

    columns = utils.make_columns(selected_cols)
    records = df.reset_index().to_dict('records')

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    return [proc, proj, records, columns, tabs]
