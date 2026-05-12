from dash import dcc, html, dash_table as dt, Input, Output, callback
import dash_bootstrap_components as dbc

from ...log import logger
from .. import utils
from . import data

 
COLUMNS = ['ID', 'PROJECT', 'TYPE', 'EDIT', 'FILE', 'FILTER', 'ARGS']


def get_content():
    columns = utils.make_columns(COLUMNS)

    # Format columns with links as markdown text
    for i, c in enumerate(columns):
        if c['name'] == 'EDIT':
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

    content = [
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-processors-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
        ),
        dbc.Spinner(id="loading-processors-table", children=[
            dbc.Label('Loading...', id='label-processors-rowcount1'),
        ]),
        dt.DataTable(
            columns=columns,
            data=[],
            page_action='none',
            sort_action='native',
            id='datatable-processors',
            style_cell={
                'textAlign': 'center',
                'maxWidth': '200px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'whiteSpace': 'nowrap',
            },
            style_header={
                'fontWeight': 'bold',
            },
            style_cell_conditional=[
                {'if': {'column_id': 'ARGS'}, 'textAlign': 'left'},
                {'if': {'column_id': 'FILTER'}, 'textAlign': 'left'},
            ],
            # Aligns the markdown in OUTPUT, both vertical and horizontal
            css=[dict(selector="p", rule="margin: 0; text-align: center")],
        ),
        html.Label('0', id='label-processors-rowcount2')]

    return content


def load_processors(projects=[]):

    if projects is None:
        projects = []

    return data.load_data(projects, refresh=True)


@callback(
    [
     Output('dropdown-processors-proj', 'options'),
     Output('datatable-processors', 'data'),
     Output('label-processors-rowcount1', 'children'),
     Output('label-processors-rowcount2', 'children'),
    ],
    [
     Input('dropdown-processors-proj', 'value'),
    ])
def update_processors(
    selected_proj,
):
    logger.debug('update_all')

    # Load selected data with refresh if requested
    df = load_processors(selected_proj)

    # Get options based on selected projects, only show proc for those projects
    proj_options = data.project_names()

    logger.debug(f'loaded options:{proj_options}')

    proj = utils.make_options(proj_options)

    # Filter data based on dropdown values
    df = data.filter_data(df)

    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        if not r['EDIT']:
            continue
        if 'redcap' in r['EDIT']:
            _link = r['EDIT']
            _text = 'edit'
            r['EDIT'] = f'[{_text}]({_link})'
        else:
            r['EDIT'] = r['EDIT']

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    return [proj, records, rowcount, rowcount]
