import logging
from pathlib import Path

from dash import dcc, html
from dash.dependencies import Input, Output
import dash

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


def get_content():
    admin_graph_content = _get_graph_content()

    admin_content = [
        dcc.Loading(
            id='loading-admin',
            children=admin_graph_content,
            style={'height': '200px', 'width': '900px'}),
        dcc.Dropdown(
            id='dropdown-admin-projects',
            multi=False,
            placeholder='Select Projects'),
        dcc.Dropdown(
            id='dropdown-admin-types',
            multi=True,
            placeholder='Select Types'),
        html.Button(
            'Run Selected',
            id='button-admin-run'),
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


def _get_graph_content():
    graph_content = []

    txt = 'LOG\n' + load_log()

    graph_content.append(html.P(
        txt,
        style={
            'padding-top': '30px',
            'padding-bottom': '20px',
            'padding-left': '20px',
            'padding-right': ' 20px'}))

    # Add some space
    graph_content.append(html.Br())

    return graph_content


# =============================================================================
# Callbacks for the app

# inputs:
# number of clicks on button

# returns:
@app.callback(
    [
        Output('dropdown-admin-projects', 'options'),
        Output('dropdown-admin-types', 'options'),
        Output('loading-admin', 'children'),
    ],
    [
        Input('button-admin-run', 'n_clicks'),
        Input('dropdown-admin-projects', 'value'),
        Input('dropdown-admin-types', 'value'),
    ])
def update_admin(
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

        if 'Double Entry Report' in selected_types:
            logging.info(f'updating double reports:{selected_projects}')
            data.update_double_reports(selected_projects)

        if 'Monthly Progress Report' in selected_types:
            logging.debug(f'updating progress reports:{selected_projects}')
            data.update_redcap_reports(selected_projects)

        if 'Check Issues' in selected_types:
            logging.debug('running audits to check issues')
            data.check_issues(selected_projects)

    # Return result
    logging.debug('update_all:returning data')

    # Update lists of possible options for dropdowns
    projects = utils.make_options(data.get_projects())
    types = utils.make_options([
        'Double Entry Report',
        'Monthly Progress Report',
        'Check Issues',
    ])

    graphs = _get_graph_content()

    return [projects, types, graphs]
