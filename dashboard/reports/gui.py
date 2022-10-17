import sys
import logging
import io
import re
import itertools
import os
import os.path
from datetime import datetime
import time

import humanize
import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash
import plotly.express as px

from app import app
import reports.data as data


try:
    os.mkdir('assets')
    os.mkdir('assets/progress')
    os.mkdir('assets/double')
except Exception:
    pass


def get_content():
    df = data.load_data()

    graph_content = get_graph_content(df)

    reports_content = [
        dcc.Loading(id="loading-reports", children=[
            dcc.Tabs(
                id='tabs-reports',
                value='0',
                children=graph_content,
                vertical=True
                )]),
        html.Button('Refresh', id='button-reports-refresh'),
        ]

    return reports_content


def was_triggered(callback_ctx, button_id):
    _bid = callback_ctx.triggered[0]['prop_id'].split('.')[0]
    result = (callback_ctx.triggered and _bid == button_id)

    return result


def get_graph_content(df):
    # We are not currently using the data in the dataframe df
    print(df)

    progress_content = []
    double_content = []
    pdf_style = {
        'padding-top': '10px',
        'padding-bottom': '10px',
        'padding-left': '50px'}

    p_style = { 
        'padding-top': '30px',
        'padding-bottom': '20px',
        'padding-left': '200px',
        'padding-right': ' 200px'}

    _time = time.ctime(os.path.getmtime('assets/progress'))
    _time = datetime.strptime(_time, "%a %b %d %H:%M:%S %Y")
    _txt = 'Last Updated {} @ {}'.format(humanize.naturaltime(_time), _time)
    progress_content.append(html.P(_txt, style=p_style))

    report_list = os.listdir('assets/progress')
    report_list = sorted(report_list)
    report_list = [x for x in report_list if x.endswith('.pdf')]
    for r in report_list:
        # Add a link to project PDF
        progress_content.append(html.Div(html.A(
            r , download=r, href=f'assets/progress/{r}'), style=pdf_style))

    # Add some space
    progress_content.append(html.Br())

    # Wrap in a tab
    tab0 = dcc.Tab(
        label='Monthly',
        value='0',
        children=[html.Div(progress_content)],
        #style={'width': '900px'})
        )

    # Build the double content
    _time = time.ctime(os.path.getmtime('assets/double'))
    _time = datetime.strptime(_time, "%a %b %d %H:%M:%S %Y")
    _txt = 'Last Updated {} @ {}'.format(humanize.naturaltime(_time), _time)
    double_content.append(html.P(_txt, style=p_style))

    report_list = os.listdir('assets/double')
    report_list = sorted(report_list)
    report_list = [x for x in report_list if x.endswith('.pdf')]
    for r in report_list:
        # Add a link to project PDF
        double_content.append(html.Div(html.A(r, download=r, href=f'assets/double/{r}'), style=pdf_style))

    # Add some space
    double_content.append(html.Br())

    # Wrap in a tab
    tab1 = dcc.Tab(
        label='Double',
        value='1',
        children=[html.Div(double_content)],
        )
        #style={'width': '900px'})

    # Concat the tabs
    tabs = [tab0, tab1]

    # Return the tabs
    return tabs


# =============================================================================
# Callbacks for the app

# inputs:
# number of clicks on button

# returns:
# content for the tabs
@app.callback(
    [Output('tabs-reports', 'children')],
    [Input('button-reports-refresh', 'n_clicks')])
def update_all(n_clicks_refresh):
    refresh = False

    # Handle refresh
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-reports-refresh'):
        # Refresh reports if refresh button clicked
        logging.info('refresh:clicks={}'.format(n_clicks_refresh))
        refresh = True

    # load reports from redcap
    df = data.load_data(refresh)

    # Return result
    tabs = get_graph_content(df)
    logging.debug('update_all:returning data')

    return [tabs]
