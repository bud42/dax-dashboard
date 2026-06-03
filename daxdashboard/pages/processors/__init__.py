from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

from ...log import logger
from .. import utils
from . import data

 
COLUMNS = ['ID', 'PROJECT', 'TYPE', 'EDIT', 'FILE', 'FILTER', 'ARGS']


def get_content():
    columnDefs = [{'headerName': x, 'field': x} for x in COLUMNS]

    # Format columns
    for c in columnDefs:
        if c['field'] == 'EDIT':
            c['cellRenderer'] = 'markdown'
    
        if c['field'] in ['NOTES', 'ARGS']:
            # Make column fill extra space
            c["flex"] = 1

        if c['field'] in ['ID', 'EDIT']:
            c['maxWidth'] = 100
            #c["cellStyle"] = {"display": "flex", 'textAlign': 'center'}

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
        dag.AgGrid(
            id='ag-processors',
            columnDefs=columnDefs,
            columnSize='responsiveSizeToFit',
            rowData=[],
            dashGridOptions={
                "theme": {"function": "themeAlpine.withPart(agGrid.colorSchemeDark)"},
            },
            defaultColDef={
                "sortable": True,
                "filter": True,
                "floatingFilter": True,
                "resizable": True,
                "headerClass": "ag-header-cell-center",
            },
        ),
        html.Label('0', id='label-processors-rowcount2')]

    return content


def load_processors(refresh=False):
    return data.load_data(refresh=refresh)


@callback(
    [
     Output('dropdown-processors-proj', 'options'),
     Output('label-processors-rowcount1', 'children'),
     Output('label-processors-rowcount2', 'children'),
     Output('ag-processors', 'rowData'),
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

    return [proj, rowcount, rowcount, records]
