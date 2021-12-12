import logging

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash

from app import app
import utils
import stats.data as data


# VAR_LIST in params.py, which can be overriden in statsparams.yaml

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


def get_graph_content(df):
    tabs_content = []
    tab_value = 0
    box_width = 250
    min_box_count = 4

    logging.debug('get_stats_figure')

    # Check for empty data
    if len(df) == 0:
        logging.debug('empty data, using empty figure')
        return [plotly.subplots.make_subplots(rows=1, cols=1)]

    # Filter var list to only include those that have data
    var_list = [x for x in df.columns if not pd.isnull(df[x]).all()]

    # Filter var list to only stats variables
    var_list = [x for x in var_list if x in data.get_variables()]

    # Determine how many boxplots we're making, depends on how many vars, use
    # minimum so graph doesn't get too small
    box_count = len(var_list)
    if box_count < min_box_count:
        box_count = min_box_count

    graph_width = box_width * box_count

    print('box_count', box_count)
    print('graph_width', graph_width)

    # Horizontal spacing cannot be greater than (1 / (cols - 1))
    #hspacing = 1 / (box_count * 2)
    hspacing = 1 / (box_count * 4)
    print('hspacing=', hspacing)

    # Make the figure with 1 row and a column for each var we are plotting
    fig = plotly.subplots.make_subplots(
        rows=1,
        cols=box_count, 
        horizontal_spacing=hspacing,
        subplot_titles=var_list)

    # box plots
    # each proctype has specific set of fields to plot,
    # TODO: we need a dictionary listing them
    # then we just do the boxplots for the chosen proctypes (and maybe scan
    # types?, how did we do that for fmri scan type?)

    # Add traces to figure
    for i, var in enumerate(var_list):
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

        # if it looks lika beta, set beta formatting
        _var_mean = df[var].mean() 
        if _var_mean < 1 and _var_mean > -1:
            print(_var_mean, 'setting beta range')
            #fig.update_layout(yaxis=dict(range=[-1,1], autorange=False))
            fig.update_yaxes(range=[-1,1], autorange=False) 
        else:
            fig.update_yaxes(autorange=True)
            pass

    # Move the subtitles to bottom instead of top of each subplot
    for i in range(len(fig.layout.annotations)):
        fig.layout.annotations[i].update(y=-.15) #, font={'size': 18})

    # Customize figure to hide legend and fit the graph
    fig.update_layout(
        showlegend=False,
        autosize=False,
        width=graph_width,
        margin=dict(l=20, r=40, t=40, b=80, pad=0))

    # Build the tab
    # We set the graph to overflow and then limit the size to 1000px, this
    # makes the graph stay in a scrollable section
    label = 'ALL'
    graph = html.Div(
        dcc.Graph(figure=fig, style={'overflow': 'scroll'}), 
        style={'width': '1000px'})

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
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll', 
                'width': '1000px'},
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

    # if none chose, default to the first proc
    #if not selected_proc:
    #    selected_proc = [(df.TYPE.unique())[0]]
    #    if 'fmri_msit_v2' in df.TYPE.unique():
    #        selected_proc = ['fmri_msit_v2']
    print(selected_proc)

    # Filter data based on dropdown values
    df = data.filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_time,
        selected_sesstype)

    # Get the graph content in tabs (currently only one tab)
    tabs = get_graph_content(df)

    # Determine columns to be included in the table
    _vars = data.get_vars()
    selected_cols = list(data.static_columns())
    selected_cols.extend(
        [x for x in df.columns if (x in _vars and not pd.isnull(df[x]).all())])

    # Get the table data
    columns = utils.make_columns(selected_cols)
    records = df.reset_index().to_dict('records')

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    return [proc, proj, records, columns, tabs]
