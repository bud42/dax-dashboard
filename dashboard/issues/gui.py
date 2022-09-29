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
from shared import STATUS2HEX
from shared import RGB_RED, RGB_GREEN, RGB_YELLOW, RGB_GREY, RGB_BLUE
import issues.data as data


STATUS2RGB = {
    'FAIL': RGB_RED,
    'COMPLETE': RGB_BLUE,
    'PASS': RGB_GREEN,
    'UNKNOWN': RGB_GREY,
    'TBD': RGB_YELLOW}


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


def _get_graph_content(df):
    PIVOTS = ['PROJECT', 'CATEGORY', 'SOURCE']
    status2rgb = STATUS2RGB
    tabs_content = []

    # index we are pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):
        #print('making pivot', i, pindex)
        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df, index=pindex, values='LABEL', columns=['STATUS'],
            aggfunc='count', fill_value=0)

        for status, color in status2rgb.items():
            #print('making bar', status, color)
            ydata = sorted(dfp.index)
            if status not in dfp:
                xdata = [0] * len(dfp.index)
            else:
                xdata = dfp[status]

            fig.append_trace(go.Bar(
                x=ydata,
                y=xdata,
                name='{} ({})'.format(status, sum(xdata)),
                marker=dict(color=color),
                opacity=0.9), 1, 1)

        # Customize figure
        fig['layout'].update(barmode='stack', showlegend=True, width=900)

        # Build the tab
        label = 'By {}'.format(pindex)
        graph = html.Div(dcc.Graph(figure=fig), style={
            'width': '100%', 'display': 'inline-block'})
        tab = dcc.Tab(label=label, value=str(i + 1), children=[graph])

        # Append the tab
        tabs_content.append(tab)

    return tabs_content


def get_content():
    ISSUES_SHOW_COLS = ['ID', 'CATEGORY', 'DATETIME', 'PROJECT', 'SUBJECT', 'EVENT', 'FIELD', 'DESCRIPTION']

    df = load_issues()
    issues_graph_content = _get_graph_content(df)

    # Get the rows and colums for the table
    issues_columns = [{"name": i, "id": i} for i in ISSUES_SHOW_COLS]
    df.reset_index(inplace=True)
    issues_data = df.to_dict('records')

    issues_content = [
        dcc.Loading(id="loading-issues", children=[
            html.Div(dcc.Tabs(
                id='tabs-issues',
                value='1',
                children=issues_graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-issues-refresh'),
        dcc.Dropdown(
            id='dropdown-issues-project', multi=True,
            placeholder='Select Projects'),
        dcc.Dropdown(
            id='dropdown-issues-category', multi=True,
            placeholder='Select Categories'),
        dcc.Dropdown(
            id='dropdown-issues-source', multi=True,
            placeholder='Select Sources'),
        dt.DataTable(
            columns=issues_columns,
            data=issues_data,
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-issues',
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll',
                'width': '1000px',
            },
            style_cell={
                'textAlign': 'left',
                'padding': '5px 5px 0px 5px',
                'width': '30px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'height': 'auto',
                'minWidth': '40',
                'maxWidth': '60'},
            style_data_conditional=[
                {'if': {'column_id': 'STATUS'}, 'textAlign': 'center'},
                {'if': {'filter_query': '{STATUS} = "PASS"'},  'backgroundColor': STATUS2HEX['RUNNING']},
                {'if': {'filter_query': '{STATUS} = "UNKNOWN"'},  'backgroundColor': STATUS2HEX['WAITING']},
                {'if': {'filter_query': '{STATUS} = "TBD"'},  'backgroundColor': STATUS2HEX['PENDING']},
                {'if': {'filter_query': '{STATUS} = "UNKNOWN"'},  'backgroundColor': STATUS2HEX['UNKNOWN']},
                {'if': {'filter_query': '{STATUS} = "FAIL"'},   'backgroundColor': STATUS2HEX['FAILED']},
                {'if': {'filter_query': '{STATUS} = "COMPLETE"'}, 'backgroundColor': STATUS2HEX['COMPLETE']},
                {'if': {'column_id': 'STATUS', 'filter_query': '{STATUS} = ""'}, 'backgroundColor': 'white'}
            ],
            style_header={
                #'width': '80px',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            fill_width=False,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible')]

    return issues_content


def load_issues(refresh=False):
    return data.load_data(refresh=refresh)


def load_category_options():
    return data.load_category_options()


def load_project_options():
    return data.load_project_options()


def load_source_options():
    return data.load_source_options()


def filter_data(df, selected_project, selected_category, selected_source):
    return data.filter_data(
        df, selected_project, selected_category, selected_source)


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


@app.callback(
    [Output('dropdown-issues-category', 'options'),
     Output('dropdown-issues-project', 'options'),
     Output('dropdown-issues-source', 'options'),
     Output('datatable-issues', 'data'),
     Output('datatable-issues', 'columns'),
     Output('tabs-issues', 'children')],
    [Input('dropdown-issues-category', 'value'),
     Input('dropdown-issues-project', 'value'),
     Input('dropdown-issues-source', 'value'),
     Input('button-issues-refresh', 'n_clicks')])
def update_issues(
    selected_category,
    selected_project,
    selected_source,
    n_clicks
):
    refresh = False

    logging.debug('update_issues')

    # Load issues data
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-issues-refresh'):
        # Refresh data if refresh button clicked
        logging.debug('issues refresh:clicks={}'.format(n_clicks))
        refresh = True

    logging.debug('loading issues data')
    df = load_issues(refresh=refresh)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    projects = utils.make_options(load_project_options())
    categories = utils.make_options(load_category_options())
    sources = utils.make_options(load_source_options())

    # Filter data based on dropdown values
    df = filter_data(
        df,
        selected_project,
        selected_category,
        selected_source)

    tabs = _get_graph_content(df)

    # Get the table data
    selected_cols = ['ID', 'CATEGORY', 'DATETIME', 'PROJECT', 'SUBJECT', 'EVENT', 'FIELD', 'DESCRIPTION']
    columns = utils.make_columns(selected_cols)
    records = df.reset_index().to_dict('records')

    # Return table, figure, dropdown options
    logging.debug('update_issues:returning data')

    return [categories, projects, sources, records, columns, tabs]
