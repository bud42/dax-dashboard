import sys
import logging
import io
import re
import itertools
from datetime import datetime
import time
from pathlib import Path

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
import admin.data as data
import utils

# TODO: show log of crontab stuff and runs by user here

# when redcap progress report runs in redcap use or create a record
# for this month. the current record matches the report in dashboard, it get's
# overwritten until the month changes. same for dataentry report.

# selected button
# dropdowns where we select what to run
# options for what to run:
#    types:  data entry, progress, audits, etc., actions,
# image03(current should be renamed to save it)
#    projects: []

# on start up get the most recent reports from redcap and save locally, then
# run will create a new report and replace the local copy


def print_results(results):
    # Print summary to screen
    import pprint
    pprint.pprint(results)


def get_content():
    admin_graph_content = get_graph_content()

    admin_content = [
        dcc.Loading(id="loading-admin", children=[
            dcc.Tabs(
                id='tabs-admin',
                value='0',
                children=admin_graph_content,
                vertical=True
                )]),
        html.Button('Run Selected', id='button-admin-run'),
        dcc.Dropdown(
            id='dropdown-admin-projects', multi=False,
            placeholder='Select Projects'),
        dcc.Dropdown(
            id='dropdown-admin-types', multi=True,
            placeholder='Select Types'),
        ]

    return admin_content


def was_triggered(callback_ctx, button_id):
    _bid = callback_ctx.triggered[0]['prop_id'].split('.')[0]
    result = (callback_ctx.triggered and _bid == button_id)

    return result


def load_log():
    txt = ''

    try:
        with open(Path.home().joinpath('log.txt'), 'r') as f:
            txt = f.read()
    except FileNotFoundError:
        pass

    return txt


def get_graph_content():
    graph_content = []

    txt = load_log()

    graph_content.append(html.P(
        txt,
        style={
            'padding-top': '30px',
            'padding-bottom': '20px',
            'padding-left': '200px',
            'padding-right': ' 200px'}))

    # Add some space
    graph_content.append(html.Br())


# =============================================================================
# Callbacks for the app

# inputs:
# number of clicks on button

# returns:
@app.callback(
    [
    Output('dropdown-admin-projects', 'options'),
    Output('dropdown-admin-types', 'options'),
    ],
    [
    Input('button-admin-run', 'n_clicks'),
    Input('dropdown-admin-projects', 'value'),
    Input('dropdown-admin-types', 'value'),
    ])
def update_all(
    n_clicks_run,
    selected_projects,
    selected_types,
):
    logging.info(f'updating:{selected_types}:{selected_projects}')

    # Handle run
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-admin-run'):
        # Run reports if button clicked
        logging.info('run:clicks={}'.format(n_clicks_run))

        if 'Old Reports' in selected_types:
            logging.info(f'updating old reports:{selected_projects}')
            update_old_reports(refresh=True)

        if 'Double Entry Report'  in selected_types:
            logging.info(f'updating double reports:{selected_projects}')
            data.update_double_reports(selected_projects)

        if 'Monthly Progress Report'  in selected_types:
            logging.debug(f'updating progress reports:{selected_projects}')
            data.update_redcap_reports(selected_projects)

        if  'Check Issues' in selected_types:
            logging.debug('running audits to check issues')

    # Return result
    logging.debug('update_all:returning data')

    # Update lists of possible options for dropdowns
    projects = utils.make_options(['DepMIND2'])
    types = utils.make_options([
        'Old Reports',
        'Double Entry Report',
        'Monthly Progress Report',
        'Check Issues'
        ])

    return [projects, types]
