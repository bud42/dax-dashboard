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

    # Filter var list to only stats variables, this also helps sort
    # by order in params yaml
    var_list = [x for x in data.get_variables() if x in var_list]

    # Determine how many boxplots we're making, depends on how many vars, use
    # minimum so graph doesn't get too small
    box_count = len(var_list)
    if box_count < min_box_count:
        box_count = min_box_count

    graph_width = box_width * box_count

    #print('box_count', box_count)
    #print('graph_width', graph_width)

    # Horizontal spacing cannot be greater than (1 / (cols - 1))
    #hspacing = 1 / (box_count * 2)
    hspacing = 1 / (box_count * 4)
    #print('hspacing=', hspacing)

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

        if var.startswith('con_') or var.startswith('inc_'):
            print(var, 'setting beta range')
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
    # Load the data
    df = load_stats([], [])

    # Check for empty data
    #if df.empty:
    #    _txt = 'No stats loaded.'
    #    logging.debug(_txt)
        #stats_content = html.Div(
        #    _txt,
        #     style={
        #        'padding-top': '100px',
        #        'padding-bottom': '200px',
        #        'padding-left': '400px',
        #        'padding-right': '400px'}
        #)
        #return stats_content

    # Make the graphs
    stats_graph_content = get_graph_content(df)

    # Get the rows and colums for the table
    stats_columns = [{"name": i, "id": i} for i in df.columns]
    print(df.columns)

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
        dcc.Dropdown(
            id='dropdown-stats-sess', multi=True,
            placeholder='Select Session Type(s)'),
        dcc.RadioItems(
            options=[
                {'label': 'Row per Assessor', 'value': 'assr'},
                #{'label': 'Session', 'value': 'sess'},
                {'label': 'Row per Subject', 'value': 'subj'}],
            value='assr',
            id='radio-stats-pivot',
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
            export_columns='visible'),
        html.Label('0', id='label-rowcount')]

    return stats_content


def load_stats(projects, proctypes, refresh=False):
    return data.load_data(projects, proctypes, refresh=refresh)


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


@app.callback(
    [Output('dropdown-stats-proc', 'options'),
     Output('dropdown-stats-proj', 'options'),
     Output('dropdown-stats-sess', 'options'),
     Output('datatable-stats', 'data'),
     Output('datatable-stats', 'columns'),
     Output('tabs-stats', 'children'),
     Output('label-rowcount', 'children')],
    [Input('dropdown-stats-proc', 'value'),
     Input('dropdown-stats-proj', 'value'),
     Input('dropdown-stats-sess', 'value'),
     Input('dropdown-stats-time', 'value'),
     Input('radio-stats-pivot', 'value'),
     Input('button-stats-refresh', 'n_clicks')])
def update_stats(
    selected_proc,
    selected_proj,
    selected_sess,
    selected_time,
    selected_pivot,
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

    # Load selected data with refresh if requested
    df = load_stats(selected_proj, selected_proc, refresh=refresh)

    # Get options based on redcdap keys file
    proj_options, proc_options = data.load_options(selected_proj, selected_proc)
    proj = utils.make_options(proj_options)
    proc = utils.make_options(proc_options)

    # Get session type in unfiltered data
    if not df.empty:
        sess = utils.make_options(df.SESSTYPE.unique())
    else:
        sess = []

    # Filter data based on dropdown values
    #selected_sesstype = 'all'
    # TODO: don't need to apply proj/proc filters again
    df = data.filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_time,
        selected_sess)

    # Get the graph content in tabs (currently only one tab)
    tabs = get_graph_content(df)

    # TODO: handle multiple of same type for a subject?
    if selected_pivot == 'subj':
        # Pivot to one row per subject

        _index = ['SUBJECT', 'PROJECT', 'AGE', 'SEX', 'DEPRESS', 'SITE']

        _vars = data.get_vars()

        _vars = [x for x in df.columns if (
            x in _vars and not pd.isnull(df[x]).all())]

        _cols = []
        if len(df.SESSTYPE.unique()) > 1:
            # Multiple session types, need prefix to disambiguate
            _cols += ['SESSTYPE']
        if len(df.TYPE.unique()) > 1:
            # Multiple processing types, need prefix to disambiguate
            _cols += ['TYPE']

        # Drop any duplicates found b/c redcap sync module does not prevent
        df = df.drop_duplicates()

        # Make the pivot table based on _index, _cols, _vars
        #print(_index)
        #print(_cols)
        #print(_vars)
        #print(df)
        dfp = df.pivot(index=_index, columns=_cols, values=_vars)

        # Concatenate column levels to get one level with delimiter
        dfp.columns = ['_'.join(reversed(t)) for t in dfp.columns]

        # Clear the index so all columns are named
        dfp = dfp.reset_index()

        columns = utils.make_columns(dfp.columns)
        records = dfp.to_dict('records')
    else:
        # Keep as to one row per assessor

        # Determine columns to be included in the table
        selected_cols = list(data.static_columns())
        _vars = data.get_vars()
        _vars = [x for x in df.columns if (
            x in _vars and not pd.isnull(df[x]).all())]
        selected_cols.extend(_vars)

        # Get the table data as one row per assessor
        columns = utils.make_columns(selected_cols)
        records = df.reset_index().to_dict('records')

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    return [proc, proj, sess, records, columns, tabs, rowcount]
