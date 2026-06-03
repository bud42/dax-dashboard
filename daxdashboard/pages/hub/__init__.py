"""dashboard home"""
import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import Input, Output, callback, dcc, html
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

from ...log import logger
from .. import utils
from ..shared import STATUS2RGB
from . import data
from .. import queue
from .. import analyses


# For more on ag grid cell borders:
# https://dash.plotly.com/dash-ag-grid/styling-borders
# https://www.ag-grid.com/archive/35.2.0/javascript-data-grid/theming-borders/


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

    columnDefs = [{
        'headerName': x,
        'field': x, 
        "headerClass": 'hub-header',
        'width': 40,
        } for x in columns]

    for c in columnDefs:
        if c['field'] == 'TYPE':
            c['headerClass'] = 'hub-header-first'
            c['minWidth'] = 200
            c['cellStyle'] = {"textAlign": "left"}

    return [
        dag.AgGrid(
            id='ag-hub-processing',
            columnDefs=columnDefs,
            columnSize='sizeToFit',
            rowData=records,
            dashGridOptions={
                "theme": {"function": 'themeAlpine.withPart(agGrid.colorSchemeDark).withParams({columnBorder: true, rowBorder: true, wrapperBorder: true})'},
                "headerHeight": 200,
                "rowHeight": 40,
            },
            defaultColDef={
                "wrapHeaderText": True,
                "wrapText": True,
                "sortable": False,
                "filter": False,
                "floatingFilter": False,
                "resizable": True,
                'cellStyle': {"textAlign": "center"},  
            },
            className="no-padding-grid",
        ),
    ]


def _queue_graph(df):
    if df.empty:
        return [html.P('Nothing in the queue for selected projects.', className='text-center')]

    status2rgb = {k: STATUS2RGB[k] for k in queue.STATUSES}

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)

    dfp = pd.pivot_table(
        df,
        index=['PROJECT', 'PROCTYPE'],
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


def _analyses_graph(df):
    if df.empty:
        return [html.P('No active analyses for selected projects.', className='text-center')]

    status2rgb = {k: STATUS2RGB[k] for k in queue.STATUSES}

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)

    dfp = pd.pivot_table(
        df,
        index=['PROJECT'],
        values='ID',
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
                dbc.Col(html.H5('Task Queue', className='text-center'), width=6),
                dbc.Col(html.H5('Analyses', className='text-center'), width=6),
            ]),
            dbc.Row([
                dbc.Col(
                    html.Div(id='div-hub-queue', children=[]), width=6,
                ),
                dbc.Col(
                    html.Div(id='div-hub-analyses', children=[]), width=6,
                ),
            ]),
        ]),
        dbc.Row([dbc.Col(html.H5('Processing', className='text-center'))]),
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
     Output('div-hub-analyses', 'children'),
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
    analyses_data = data.get_analyses_data(refresh=refresh)

    # Get options for dropdowns berfore filtering
    proj_options = data.load_options(proc_data)
    proj = utils.make_options(proj_options)

    # Filter data
    if selected_proj:
        proc_data = proc_data[proc_data.PROJECT.isin(selected_proj)]
        queue_data = queue_data[queue_data.PROJECT.isin(selected_proj)]
        analyses_data = analyses_data[analyses_data.PROJECT.isin(selected_proj)]

    # Make graphs/tables
    queue_graph = _queue_graph(queue_data)
    proc_graph = _processing_graph(proc_data)
    analyses_graph = _analyses_graph(analyses_data)

    # Return table, figure, dropdown options
    logger.debug('update_hub:returning data')

    return [proj, proc_graph, queue_graph, analyses_graph]
