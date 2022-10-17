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
import activity.data as data


STATUS2RGB = {
    'FAIL': RGB_RED,
    'COMPLETE': RGB_BLUE,
    'PASS': RGB_GREEN,
    'UNKNOWN': RGB_GREY,
    'TBD': RGB_YELLOW}


def get_graph_content(df):
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
    ACTIVITY_SHOW_COLS = ['ID', 'DESCRIPTION']

    df = load_activity()
    activity_graph_content = get_graph_content(df)

    # Get the rows and colums for the table
    activity_columns = [{"name": i, "id": i} for i in ACTIVITY_SHOW_COLS]
    df.reset_index(inplace=True)
    activity_data = df.to_dict('records')

    activity_content = [
        dcc.Loading(id="loading-activity", children=[
            html.Div(dcc.Tabs(
                id='tabs-activity',
                value='1',
                children=activity_graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-activity-refresh'),
        dcc.Dropdown(
            id='dropdown-activity-project', multi=True,
            placeholder='Select Projects'),
        dcc.Dropdown(
            id='dropdown-activity-category', multi=True,
            placeholder='Select Categories'),
        dcc.Dropdown(
            id='dropdown-activity-source', multi=True,
            placeholder='Select Sources'),
        dt.DataTable(
            columns=activity_columns,
            data=activity_data,
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-activity',
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

    return activity_content


def load_activity(refresh=False):
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
    [Output('dropdown-activity-category', 'options'),
     Output('dropdown-activity-project', 'options'),
     Output('dropdown-activity-source', 'options'),
     Output('datatable-activity', 'data'),
     Output('datatable-activity', 'columns'),
     Output('tabs-activity', 'children')],
    [Input('dropdown-activity-category', 'value'),
     Input('dropdown-activity-project', 'value'),
     Input('dropdown-activity-source', 'value'),
     Input('button-activity-refresh', 'n_clicks')])
def update_activity(
    selected_category,
    selected_project,
    selected_source,
    n_clicks
):
    refresh = False

    logging.debug('update_activity')

    # Load activity data
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-activity-refresh'):
        # Refresh data if refresh button clicked
        logging.debug('activity refresh:clicks={}'.format(n_clicks))
        refresh = True

    logging.debug('loading activity data')
    df = load_activity(refresh=refresh)

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

    tabs = get_graph_content(df)

    # Get the table data
    selected_cols = ['ID', 'DATETIME', 'DESCRIPTION']
    columns = utils.make_columns(selected_cols)
    records = df.reset_index().to_dict('records')

    # Return table, figure, dropdown options
    logging.debug('update_activity:returning data')

    return [categories, projects, sources, records, columns, tabs]
