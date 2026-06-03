import pandas as pd
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import dash_ag_grid as dag

from ...log import logger
from .. import utils
from . import data


COLUMNS = [
    'ID',
    'NAME',
    'STATUS',
    'PBS',
    'PDF',
    'LOG',
    'OUTPUT',
    'PROCESSOR',
    'COVARS',
    'SUBJECTS',
    'NOTES'
]


STATUS2EMO = {
    'READY': '🟩',
    'Q': '🔷',
    'QUEUED': '🔷',
    'COMPLETE': '🔷',
    'COMPLETED': '🔷',
    'JOB_FAILED': '🩷',
    'DEVEL': '🟡',
    'RUNNING': '🔷',
}


def get_content():
    columnDefs = [{'headerName': x, 'field': x} for x in COLUMNS]

    for i, c in enumerate(columnDefs):
        if c['field'] in ['OUTPUT', 'EDIT', 'INPUT', 'DATA', 'PROCESSOR', 'LOG', 'PDF', 'PBS', 'COVARS', 'ID']:
            #columns[i]['type'] = 'text'
            columnDefs[i]['cellRenderer'] = 'markdown'

        # Make NOTES column fill extra space
        if c['field'] == 'NOTES':
            c["flex"] = 1

        # TODO: fix centering, not quite working
        if c['field'] in ['STATUS', 'OUTPUT', 'COVARS', 'PBS', 'LOG', 'PDF', 'PROCESSOR']:
            c['maxWidth'] = 100
            c["cellStyle"] = {"display": "flex", "justifyContent": "center", 'textAlign': 'center'}


    content = [
        dbc.Row([
            dbc.Col(
                dbc.Button(
                    'Refresh Data',
                    id='button-analyses-refresh',
                    outline=True,
                    color='primary',
                    size='sm',
                ),
            ),
        ]),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-analyses-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
        ),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-analyses-lead',
                    multi=True,
                    placeholder='Select Lead Investigator(s)',
                ),
                width=3,
            ),
        ),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-analyses-status',
                    multi=True,
                    placeholder='Select Status',
                ),
                width=3,
            ),
        ),
        dbc.Spinner(id="loading-analyses-table", children=[
            dbc.Label('Loading...', id='label-analyses-rowcount1'),
        ]),
        dcc.Download(id="download-covars"),
        dag.AgGrid(
            id='ag-analyses',
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
        html.Label('0', id='label-analyses-rowcount2'),
    ]

    return content


def load_analyses(refresh=False):
    return data.load_data(refresh=refresh)


@callback(
    [
     Output('dropdown-analyses-proj', 'options'),
     Output('dropdown-analyses-lead', 'options'),
     Output('dropdown-analyses-status', 'options'),
     Output('label-analyses-rowcount1', 'children'),
     Output('label-analyses-rowcount2', 'children'),
     Output('ag-analyses', 'rowData'),
    ],
    [
     Input('button-analyses-refresh', 'n_clicks'),
     Input('dropdown-analyses-proj', 'value'),
     Input('dropdown-analyses-lead', 'value'),
     Input('dropdown-analyses-status', 'value'),
    ])
def update_analyses(
    n_clicks,
    selected_proj,
    selected_lead,
    selected_status,
):
    refresh = False

    logger.debug('update_analyses')

    # Load selected data with refresh if requested
    if utils.was_triggered('button-analyses-refresh'):
        logger.debug(f'analyses refresh:clicks={n_clicks}')
        refresh = True

    # Load all analyses
    df = load_analyses(refresh=refresh)

    # Truncate NOTES
    if 'NOTES' in df:
        df['NOTES'] = df['NOTES'].str.slice(0, 20)

    # Count SUBJECTS list
    _mask = df['SUBJECTS'].notna() & (df['SUBJECTS'].str.len() > 0)
    df.loc[_mask, 'SUBJECTS'] = (df.loc[_mask, 'SUBJECTS'].str.split(r'[,\n\s]+').apply(lambda x: f"n={len([i for i in x if i])}"))

    # Change blanks to asterisk
    df.loc[df['SUBJECTS'].str.len() == 0, 'SUBJECTS'] = '*'

    # Get project options
    proj_options = df.PROJECT.unique()

    # Filter by project before loading inv and status options
    df = data.filter_data(df, projects=selected_proj)

    lead_options = sorted(df['INVESTIGATOR'].unique())
    status_options = sorted(df['STATUS'].unique())
    proj = utils.make_options(proj_options)
    lead = utils.make_options(lead_options)
    status = utils.make_options(status_options)

    logger.debug(f'loaded options:{proj_options}:{lead_options}')

    # Filter data based on dropdown values
    df = data.filter_data(
        df,
        leads=selected_lead, 
        statuses=selected_status)

    # Map statuses to emoji
    df['STATUS'] = df['STATUS'].map(STATUS2EMO).fillna('?')
 
    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        # Make edit a link
        _link = r['EDIT']
        _text = 'edit'
        r['EDIT'] = f'[{_text}]({_link})'

        _id = r['ID']
        r['ID'] = f'[{_id}]({_link})'

         # Make log a link
        _link = r['LOGLINK']
        r['LOG'] = f'[📄]({_link})'

        if r['STATUS'] == 'READY':
             # Make pdf a link
            _link = r['PDFLINK']
            r['PDF'] = f'[📊]({_link})'

            # Make pbs a link
            _link = r['PBSLINK']
            r['PBS'] = f'[📋]({_link})'
        else:
            r['PDF'] = ''
            r['PBS'] = ''

        # Make a link
        if not r['OUTPUT']:
            pass
        elif r['OUTPUTLINK']:
            _link = r['OUTPUTLINK']
            #_text = r['OUTPUT']
            #r['OUTPUT'] = f'[{_text}]({_link})'
            r['OUTPUT'] = f'[📁]({_link})'
        elif '/' in r['OUTPUT']:
            _link = r['OUTPUT']
            #_text = r['OUTPUT'].rsplit('/', 2)[1]
            #r['OUTPUT'] = f'[{_text}]({_link})'
            r['OUTPUT'] = f'[📁]({_link})'

        # Make covars a link
        if not r['COVARS']:
            pass
        else:
            #_link = r['COVARS']
            #_text = r['COVARS']
            #r['COVARS'] = f'[📗]({_link})'
            r['COVARS'] = '📗'

        # Make a link
        if not r['PROCESSOR']:
            pass
        elif '/' in r['PROCESSOR']:
            try:
                p = r['PROCESSOR'].replace(':', '/').split('/')
                if len(p) == 4:
                    _link = f'https://github.com/{p[0]}/{p[1]}/tree/{p[2]}/processors/{p[3]}'
                elif len(p) == 3:
                    _link = f'https://github.com/{p[0]}/{p[1]}/tree/{p[2]}'
                else:
                    _link = f'https://github.com/{p}'

                _text = r['PROCESSOR']
                #r['PROCESSOR'] = f'[{_text}]({_link})'
                r['PROCESSOR'] = f'[⚙️]({_link})'
            except Exception as err:
                logger.error(f'failed to parse processor:{r["PROCESSOR"]}')

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    return [proj, lead, status, rowcount, rowcount, records]


@callback(
    Output("download-covars", "data"),
    Input("datatable-analyses", "active_cell"),
    Input("datatable-analyses", "data"),
    )
def get_file(active_cell, rows):
    if not active_cell:
        raise PreventUpdate

    if active_cell['column_id'] not in ['COVARS']:
        raise PreventUpdate

    row_id = active_cell['row']
    project_id = rows[row_id]['PROJECT']
    repeat_id = rows[row_id]['REPEATID']

    if active_cell['column_id'] == 'COVARS':
        content, headers = data.export_covar_file(project_id, repeat_id)
    elif active_cell['column_id'] == 'COVARS':
        content, headers = data.export_pdf_file(project_id, repeat_id)
    else:
        raise Exception('invalid click')

    filename = headers.get('name', 'covariates.csv')
    filename = f'{project_id}_{repeat_id}-{filename}'

    return dcc.send_bytes(content, filename)


if __name__ == "__main__":
    app.run(debug=True)
