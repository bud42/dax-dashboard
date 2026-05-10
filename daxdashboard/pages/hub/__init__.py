"""dashboard home"""
import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import Input, Output, callback, dcc, html, dash_table as dt
import dash_bootstrap_components as dbc

from ...log import logger
from .. import utils
from ..shared import STATUS2RGB
from . import data
from .. import queue


# Project table with Processing, Automations, Reports

# Bar graph of Queue
# Bar graph of Issues
# Bar graph of Activity

# Analyses graph by status? or list of active?

COMPLETE2EMO = {'0': '🔴', '1': '🟡', '2': '🟢'}


def _processing_graph(df):

    df['STATUS'] = df['COMPLETE'].map(COMPLETE2EMO).fillna('')

    dfp = df.pivot_table(
        index='TYPE',
        values='STATUS',
        columns=['PROJECT'],
        aggfunc=lambda x: x.mode().iat[0],
        fill_value='')

    dfp = dfp.reset_index()
    columns = dfp.columns
    records = dfp.to_dict('records')

    return [
        dt.DataTable(
            columns=utils.make_columns(columns),
            data=records,
            filter_action='none',
            page_action='none',
            sort_action='none',
            id='datatable-hub-processing',
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll',
            },
            style_cell={
                'textAlign': 'center',
                'width': '10px',
                'height': 'auto',
            },
            style_header={
                'fontWeight': 'bold',
                'padding': '1px 1px 0px 1px',
            },
            fill_width=False,
        )]


def _queue_graph(df):
    if df.empty:
        return [html.P('Nothing in the queue for selected projects.', className='text-center')]

    status2rgb = {k: STATUS2RGB[k] for k in queue.STATUSES}

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)

    dfp = pd.pivot_table(
        df,
        index='PROCTYPE',
        values='LABEL',
        columns=['STATUS'],
        aggfunc='count',
        fill_value=0)

    for status, color in status2rgb.items():
        ydata = sorted(dfp.index)
        if status not in dfp:
            continue
        else:
            xdata = dfp[status]

        fig.append_trace(
            go.Bar(
                x=xdata,
                y=ydata,
                name='{} ({})'.format(status, sum(xdata)),
                marker=dict(color=color),
                opacity=0.9, orientation='h'),
            1,
            1
        )

    fig['layout'].update(barmode='stack', showlegend=True)

    graph = dcc.Graph(figure=fig)

    return [graph]


def get_content():
    '''Get page content.'''

    # We use the dbc grid layout with rows and columns, rows are 12 units wide
    content = [
        dbc.Row([
            dbc.Col(dbc.Button('Refresh', id='button-hub-refresh')),
        ]),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-hub-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
        ),
        dbc.Spinner([
            dbc.Row([               
                dbc.Col(html.H5('Queue', className='text-center'), width=6),
            ]),
            dbc.Row([
                dbc.Col(
                    html.Div(id='div-hub-queue', children=[]), width=6,
                ),
            ]),
        ]),
        dbc.Row([dbc.Col(html.H5('Processing'))]),
        dbc.Row([
            dbc.Col(
                html.Div(
                    id='div-hub-processing',
                    children=[],
                    style={'margin-bottom': '2em'},
                ),
                width=12
            )
        ]),
    ]

    return content


@callback(
    [
     Output('dropdown-hub-proj', 'options'),
     Output('div-hub-processing', 'children'),
     Output('div-hub-queue', 'children'),
     ],
    [
     Input('button-hub-refresh', 'n_clicks'),
     Input('dropdown-hub-proj', 'value'),
    ],
)
def update_hub(n_clicks, selected_proj):
    refresh = False

    logger.debug('update_hub')

    if utils.was_triggered('button-hub-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh-hub:clicks={}'.format(n_clicks))
        refresh = True

    # Load datas
    queue_data = data.get_queue_data(refresh=refresh)
    proc_data = data.get_processors_data(refresh=refresh)

    # Get options for dropdowns berfore filtering
    proj_options = data.load_options(proc_data)
    proj = utils.make_options(proj_options)

    # Filter data
    if selected_proj:
        proc_data = proc_data[proc_data.PROJECT.isin(selected_proj)]
        queue_data = queue_data[queue_data.PROJECT.isin(selected_proj)]

    # Make graphs/tables
    queue_graph = _queue_graph(queue_data)
    proc_graph = _processing_graph(proc_data)

    # Return table, figure, dropdown options
    logger.debug('update_hub:returning data')

    return [proj, proc_graph, queue_graph]
