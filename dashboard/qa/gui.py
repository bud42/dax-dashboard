# make the CHAMP report
# make the D3 report
# make the REMBRANDT report
# make the DepMIND2 report


# TODO:
# show scans acquired during timeframe or assessors created or jobs started
# (later maybe jobs that finished) 
# default filtering should select this month and then select the sessiontypes
# and projects and scans and assessors that are found in this data


# configure default filtering to be:
# time should be:  by acquisition date or processing date or either
# PI: [*]Both []Newhouse []Taylor 
# "last 30 days" and then select the 
# select the assessor types created in those 30 days
# and then the scan types that are inputs to those assessors
# then have a single "reset" button that reselects
# so effectively we set the selected items to active projects
# and active pipeolines
# TODO: use exclude list to autofilter and select those types,
# then try to make an include list based on which scans are used as inputs
# then advise users to use the little x to clear the box and see them all
# then make a Reset or Filter button to apply the auto filtering again if 
# the user has made changes.


# TODO: sessionsbytime should alow last week, this week, last month, this month,
# and the graphs should stack by session type and have hover esp for weekly,
# bins should be by day/week/month depending on what timeframe is selected
# e.g. last week should show each day, weekly should show each monday date
# then last year/this year should be monthly

# TODO: show session notes in a popup?
# TODO: show help in a clickable dialog
# TODO: add new colors for jobs that are NEED_INPUTS, JOB_RUNNING, JOB_FAILED

# highlight session rows based on whether it's:
# "all fail"=RED, "any tbd"=YELLOW, otherwise no color?

# show how long ago the data was updated using humanized time
# and move the refresh button beside that display

# DESCRIPTION:
# the table is by session using a pivottable that aggregates the statuses
# for each scan/assr type. then we have dropdowns to filter by project,
# processing type, scan type, etc.

import logging
import re
import itertools

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash

from app import app
import utils
from shared import QASTATUS2COLOR, RGB_DKBLUE
import qa.data as data


def get_graph_content(dfp, selected_groupby='PROJECT'):
    tabs_content = []
    tab_value = 0

    logging.debug('get_qa_figure')

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Check for empty data
    if len(dfp) == 0:
        logging.debug('empty data, using empty figure')
        return [fig]

    # First we copy the dfp and then replace the values in each
    # scan/proc type column with a metastatus,
    # that gives us a high level status of the type for that session

    # TODO: should we just make a different pivot table here going back to
    # the original df? yes, later
    dfp_copy = dfp.copy()
    for col in dfp_copy.columns:
        if col in ('SESSION', 'PROJECT', 'DATE'):
            # don't mess with these columns
            # TODO: do we need this if we haven't reindexed yet?
            continue

        # Change each value from the multiple values in concatenated
        # characters to a single overall status
        dfp_copy[col] = dfp_copy[col].apply(get_metastatus)

    # The pivot table for the graph is a pivot of the pivot table, instead
    # of having a row per session, this pivot table has a row per
    # pivot_type, we can pivot by type to get counts of each status for each
    # scan/proc type, or we can pivot by project to get counts of sessions
    # for each project
    # The result will be a table with one row per TYPE (index=TYPE),
    # and we'll have a column for each STATUS (so columns=STATUS),
    # and we'll count how many sessions (values='SESSION') we find for each
    # cell
    # get a copy so it's defragmented
    dfp_copy = dfp_copy.reset_index().copy()

    # don't need subject
    dfp_copy = dfp_copy.drop(columns=['SUBJECT'])

    # use pandas melt function to unpivot our pivot table
    df = pd.melt(
        dfp_copy,
        id_vars=(
            'SESSION',
            'PROJECT',
            'DATE',
            'SITE',
            'SESSTYPE',
            'MODALITY'),
        value_name='STATUS')

    # We use fill_value to replace nan with 0
    dfpp = df.pivot_table(
        index='TYPE',
        columns='STATUS',
        values='SESSION',
        aggfunc='count',
        fill_value=0)

    # sort so scans are first, then assessor
    scan_type = []
    assr_type = []
    for cur_type in dfpp.index:
        # Use a regex to test if name ends with _v and a number, then assr
        if re.search('_v\d+$', cur_type):
            assr_type.append(cur_type)
        else:
            scan_type.append(cur_type)

    newindex = scan_type + assr_type
    dfpp = dfpp.reindex(index=newindex)

    # Draw bar for each status, these will be displayed in order
    # ydata should be the types, xdata should be count of status
    # for each type
    for cur_status, cur_color in QASTATUS2COLOR.items():
        ydata = dfpp.index
        if cur_status not in dfpp:
            xdata = [0] * len(dfpp.index)
        else:
            xdata = dfpp[cur_status]

        cur_name = '{} ({})'.format(cur_status, sum(xdata))

        fig.append_trace(
            go.Bar(
                x=ydata,
                y=xdata,
                name=cur_name,
                marker=dict(color=cur_color),
                opacity=0.9),
            1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=True, width=900)

    # Build the tab
    label = 'By {}'.format('TYPE')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # We also want a tab for By Project, so we can ask e.g. how many
    # sessions for each project, and then ask
    # which projects have a T1 and a good FS6_v1
    # later combine with other pivot
    # table and loop on pivot type
    dfpp = df.pivot_table(
        index='PROJECT',
        values='SESSION',
        aggfunc=pd.Series.nunique,
        fill_value=0)

    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    ydata = dfpp.index
    xdata = dfpp.SESSION

    cur_name = '{} ({})'.format('ALL', sum(xdata))
    cur_color = RGB_DKBLUE

    fig.append_trace(
        go.Bar(
            x=ydata,
            y=xdata,
            text=xdata,
            name=cur_name,
            marker=dict(color=cur_color),
            opacity=0.9),
        1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=True, width=900)

    # Build the tab
    label = 'By {}'.format('PROJECT')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # Append the by-time graph (this was added later with separate function)
    dfs = df[['PROJECT', 'DATE', 'SESSION', 'SESSTYPE', 'SITE', 'MODALITY']].drop_duplicates()
    fig = sessionsbytime_figure(dfs, selected_groupby)
    label = 'By {}'.format('TIME')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # TODO: write a "graph in a tab" function to wrap each figure above
    # in a graph in a tab, b/c DRY

    # Return the tabs
    return tabs_content


def sessionsbytime_figure(df, selected_groupby):
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # TODO: if weekly is chosen, show the actual session name instead of a dot

    # TODO: use different shapes for PET vs MR

    # TODO: try to connect baseline with followup with arc line or something
    # or could have "by subject" choice that has a subject per y value

    # Customize figure
    #fig['layout'].update(xaxis={'automargin': True}, yaxis={'automargin': True})

    from itertools import cycle
    import plotly.express as px
    palette = cycle(px.colors.qualitative.Plotly)
    #palette = cycle(px.colors.qualitative.Vivid)
    #palette = cycle(px.colors.qualitative.Bold)

    for mod, sesstype in itertools.product(df.MODALITY.unique(), df.SESSTYPE.unique()):

        # Get subset for this session type
        dfs = df[(df.SESSTYPE == sesstype) & (df.MODALITY == mod)]

        # Nothing to plot so go to next session type
        if dfs.empty:
            continue

        # Plot base on view
        view = 'default'

        if view == "month":
            # TBD
            pass

        elif view == 'all':

            # Let's do this for the all time view to see histograms by year
            # or quarter or whatever fits well

            # Plot this session type
            fig.append_trace(
                go.Histogram(
                    hovertext=dfs['SESSION'],
                    name='{} ({})'.format(sesstype, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs['PROJECT'],
                    ),
                _row,
                _col)

         
        elif view == 'weekly':
            # Let's do this only for the weekly view and customize it specifically
            # for Mon thru Fri and allow you to choose this week and last week

            dfs['ONE'] = 1

            # Plot this session type
            fig.append_trace(
                go.Bar(
                    hovertext=dfs['SESSION'],
                    name='{} ({})'.format(sesstype, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs['ONE'],
                    ),
                _row,
                _col)

            # width function of number of days being plotted
            #@width = 

            fig.update_layout(
                barmode='stack',
                width=900,
                #bargroupgap=0,
                #wbidth=100,
                bargap=0.1)
        else:
            # Create boxplot for this var and add to figure
            # Default to the jittered boxplot with no boxes

            # markers symbols, see https://plotly.com/python/marker-style/
            if mod == 'MR':
                symb = 'circle-dot'
            elif mod == 'PET':
                symb = 'diamond-wide-dot'
            else:
                symb = 'diamond-tall-dot'

            _color = next(palette)

            # Convert hex to rgba with alpha of 0.5
            if _color.startswith('#'):
                _rgba = 'rgba({},{},{},{})'.format(
                    int(_color[1:3], 16),
                    int(_color[3:5], 16),
                    int(_color[5:7], 16),
                    0.7)
            else:
                _r,_g,_b = _color[4:-1].split(',')
                _a = 0.7
                _rgba = 'rgba({},{},{},{})'.format(_r, _g, _b, _a)

            # Plot this session type
            _row = 1
            _col = 1
            fig.append_trace(
                go.Box(
                    name='{} {} ({})'.format(sesstype, mod, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs[selected_groupby],
                    boxpoints='all',
                    jitter=0.7,
                    text=dfs['SESSION'],
                    pointpos=0.5,
                    orientation='h',
                    marker={
                        'symbol': symb,
                        'color': _rgba,
                        'size': 12,
                        'line': dict(width=2, color=_color)
                    },
                    line={'color': 'rgba(0,0,0,0)'},
                    fillcolor='rgba(0,0,0,0)',
                    hoveron='points',
                ),
                _row,
                _col)

            # show lines so we can better distinguish categories
            fig.update_yaxes(showgrid=True)

            #fig.update_xaxes(range=[])
            #full_fig = fig.full_figure_for_development()
            x_mins = []
            x_maxs = []
            for trace_data in fig.data:
                x_mins.append(min(trace_data.x))
                x_maxs.append(max(trace_data.x))

            x_min = min(x_mins)
            x_max = max(x_maxs)

            if x_min == '2021-11-01' or x_min == '2021-11-10':
                fig.update_xaxes(
                    range=('2021-10-31', '2021-12-01'),
                    tickvals=[
                        '2021-11-01',
                        '2021-11-08',
                        '2021-11-15',
                        '2021-11-22',
                        '2021-11-29'])

            fig.update_layout(width=900)


    return fig


def get_content():
    # The data will be pivoted by session to show a row per session and
    # a column per scan/assessor type,
    # the values in the column a string of characters
    # that represent the status of one scan or assesor,
    # the number of characters is the number of scans or assessors
    # the columns will be the merged
    # status column with harmonized values to be red/yellow/green/blue
    df = data.load_data(hidetypes=True)

    dfp = qa_pivot(df)

    qa_graph_content = get_graph_content(dfp)

    # Get the rows and colums for the table
    qa_columns = [{"name": i, "id": i} for i in dfp.index.names]
    dfp.reset_index(inplace=True)
    qa_data = dfp.to_dict('records')

    qa_content = [
        dcc.Loading(id="loading-qa", children=[
            html.Div(dcc.Tabs(
                id='tabs-qa',
                value='1',
                children=qa_graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-qa-refresh'),
        dcc.Dropdown(
            id='dropdown-qa-time',
            # Change filters to "Today", "this week", "last week",
            #"this month", "last month", "YTD", "past year", "last year"
            options=[
                {'label': 'all time', 'value': 'ALL'},
                {'label': '1 day', 'value': '1day'},
                {'label': '1 week', 'value': '7day'},
                {'label': '1 month', 'value': '30day'},
                #{'label': 'this week', 'value': 'thisweek'},
                #{'label': 'this month', 'value': 'thismonth'},
                {'label': 'last month', 'value': 'lastmonth'},
                {'label': '1 year', 'value': '365day'}],
            value='ALL'),
        dcc.RadioItems(
            options=[
                {'label': 'Group by Project', 'value': 'PROJECT'},
                {'label': 'Group by Site', 'value': 'SITE'}],
            value='PROJECT',
            id='radio-qa-groupby',
            labelStyle={'display': 'inline-block'}),
        dcc.Dropdown(
            id='dropdown-qa-proj', multi=True,
            placeholder='Select Project(s)'),
        dcc.Dropdown(
            id='dropdown-qa-sess', multi=True,
            placeholder='Select Session Type(s)'),
        dcc.RadioItems(
            options=[
                {'label': 'Hide Unused Types', 'value': 'HIDE'},
                {'label': 'Show All Types', 'value': 'SHOW'}],
            value='HIDE',
            id='radio-qa-hidetypes',
            labelStyle={'display': 'inline-block'}),
        dcc.Dropdown(
            id='dropdown-qa-proc', multi=True,
            placeholder='Select Processing Type(s)'),
        dcc.Dropdown(
            id='dropdown-qa-scan', multi=True,
            placeholder='Select Scan Type(s)'),
        dt.DataTable(
            columns=qa_columns,
            data=qa_data,
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-qa',
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
                #'width': '80px',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            fill_width=False,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible'),
        html.Label('0', id='label-qa-rowcount'),
        ]

    return qa_content


def get_metastatus(status):

    if status != status:
        # empty so it's none
        metastatus = 'NONE'
    elif not status or pd.isnull(status):  # np.isnan(status):
        # empty so it's none
        metastatus = 'NONE'
    elif 'P' in status:
        # at least one passed, so PASSED
        metastatus = 'PASS'
    elif 'Q' in status:
        # any are still needs qa, then 'NEEDS_QA', or 'TBD'
        metastatus = 'TBD'
    elif 'J' in status:
        # if any jobs are still running, then 'TBD'
        metastatus = 'TBD'
    elif 'F' in status:
        # at this point if one failed, then they all failed, so 'FAILED'
        metastatus = 'FAIL'
    elif 'X' in status:
        metastatus = 'JOBF'
    elif 'R' in status:
        metastatus = 'JOBR'
    else:
        # whatever else is UNKNOWN, grey
        metastatus = 'NONE'

    return metastatus


def qa_pivot(df):
    dfp = df.pivot_table(
        index=(
            'SESSION', 'SUBJECT', 'PROJECT',
            #'AGE', 'SEX', 'DEPRESS',
            'DATE', 'SESSTYPE', 'SITE', 'MODALITY'),
        columns='TYPE',
        values='STATUS',
        aggfunc=lambda x: ''.join(x))

    # and return our pivot table
    return dfp


# This is where the data gets initialized
def load_data(refresh=False, hidetypes=True):
    return data.load_data(refresh=refresh, hidetypes=hidetypes)


def load_proj_options():
    return data.load_proj_options()


def load_sess_options(proj_filter=None):
    return data.load_sess_options(proj_filter)


def load_scan_options(proj_filter=None):
    return data.load_scan_options(proj_filter)


def load_proc_options(proj_filter=None):
    return data.load_proc_options(proj_filter)


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
# options for the assessor scans dropdown
# options for the assessor sessions dropdown
# data for the table
# content for the graph tabs
@app.callback(
    [Output('dropdown-qa-proc', 'options'),
     Output('dropdown-qa-scan', 'options'),
     Output('dropdown-qa-sess', 'options'),
     Output('dropdown-qa-proj', 'options'),
     Output('datatable-qa', 'data'),
     Output('datatable-qa', 'columns'),
     Output('tabs-qa', 'children'),
     Output('label-qa-rowcount', 'children'),
     ],
    [Input('dropdown-qa-proc', 'value'),
     Input('dropdown-qa-scan', 'value'),
     Input('dropdown-qa-sess', 'value'),
     Input('dropdown-qa-proj', 'value'),
     Input('dropdown-qa-time', 'value'),
     Input('radio-qa-groupby', 'value'),
     Input('radio-qa-hidetypes', 'value'),
     Input('button-qa-refresh', 'n_clicks')])
def update_all(
    selected_proc,
    selected_scan,
    selected_sess,
    selected_proj,
    selected_time,
    selected_groupby,
    selected_hidetypes,
    n_clicks
):
    refresh = False

    logging.debug('update_all')

    # Load our data
    # This data will already be merged scans and assessors with
    # a row per scan or assessor
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-qa-refresh'):
        # Refresh data if refresh button clicked
        logging.debug('refresh:clicks={}'.format(n_clicks))
        refresh = True

    logging.debug('loading data')
    hidetypes = (selected_hidetypes == 'HIDE')
    df = load_data(refresh=refresh, hidetypes=hidetypes)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    proj = utils.make_options(load_proj_options())
    scan = utils.make_options(load_scan_options(selected_proj))
    sess = utils.make_options(load_sess_options(selected_proj))
    proc = utils.make_options(load_proc_options(selected_proj))

    # Filter data based on dropdown values
    df = data.filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_scan,
        selected_time,
        selected_sess)

    #if selected_hidetypes == 'HIDE':
    #    df = data.filter_types(df)

    # Get the qa pivot from the filtered data
    dfp = qa_pivot(df)

    tabs = get_graph_content(dfp, selected_groupby)

    # Get the table data
    selected_cols = [
        'SESSION', 'SUBJECT', 'PROJECT',
        #'AGE', 'SEX', 'DEPRESS', 
        'DATE', 'SESSTYPE', 'SITE']

    if selected_proc:
        selected_cols += selected_proc

    if selected_scan:
        selected_cols += selected_scan

    columns = utils.make_columns(selected_cols)
    records = dfp.reset_index().to_dict('records')

    # TODO: should we only include data for selected columns here,
    # to reduce amount of data sent?

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    return [proc, scan, sess, proj, records, columns, tabs, rowcount]
