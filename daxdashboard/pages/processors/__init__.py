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
                dbc.Button(
                    'Refresh Data',
                    id='button-processors-refresh',
                    outline=True,
                    color='primary',
                    size='sm',
                ),
            ),
        ),
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


def load_processors(refresh=False):
    return data.load_data(refresh=refresh)


@callback(
    [
     Output('dropdown-processors-proj', 'options'),
     Output('datatable-processors', 'data'),
     Output('label-processors-rowcount1', 'children'),
     Output('label-processors-rowcount2', 'children'),
    ],
    [
     Input('button-processors-refresh', 'n_clicks'),
     Input('dropdown-processors-proj', 'value'),
    ])
def update_processors(
    n_clicks,
    selected_proj,
):
    refresh = False

    logger.debug('update_all')

    # Load selected data with refresh if requested
    if utils.was_triggered('button-processors-refresh'):
        logger.debug(f'processors refresh:clicks={n_clicks}')
        refresh = True

    # Load all processors
    df = load_processors(refresh=refresh)

    proj_options = df.PROJECT.unique()

    logger.debug(f'loaded options:{proj_options}')

    proj = utils.make_options(proj_options)

    # Filter data based on dropdown values
    df = data.filter_data(df, selected_proj)

    # Get the table data as list of records
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
