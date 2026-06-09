import pandas as pd
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import dash_ag_grid as dag

from ...log import logger
from .. import utils
from .data  import load_data, filter_data
from .data  import export_report_file, export_covar_file, export_batch_file, export_stats_file, export_log_file


COLUMNS = [
    'ID',
    'NAME',
    'STATUS',
    'BATCH',
    'REPORT',
    'LOGFILE',
    'COVARS',
    'OUTPUT',
    'PROCESSOR',
    'SUBJECTS',
    'NOTES'
]


STATUS2EMO = {
    'READY': '✅',
    'Q': '🔷',
    'QUEUED': '🔷',
    'COMPLETE': '🔷',
    'COMPLETED': '🔷',
    'FAILED': '🩷',
    'JOB_FAILED': '🩷',
    'JOB_CANCELLED': '❌',
    'DEVEL': '⚠️',
    'RUNNING': '🔷',
    'DEV': '⚠️',
}


def get_content():
    columnDefs = [{'headerName': x, 'field': x} for x in COLUMNS]

    for i, c in enumerate(columnDefs):
        if c['field'] in ['EDIT', 'INPUT', 'DATA', 'PROCESSOR', 'LOGFILE', 'REPORT', 'BATCH', 'COVARS', 'ID', 'NAME']:
            #columns[i]['type'] = 'text'
            columnDefs[i]['cellRenderer'] = 'markdown'
            columnDefs[i]["linkTarget"] = "_blank"

        # Make NOTES column fill extra space
        if c['field'] == 'NOTES':
            c["flex"] = 1

        if c['field'] in ['STATUS', 'COVARS', 'BATCH', 'LOGFILE', 'REPORT', 'PROCESSOR']:
            c['maxWidth'] = 40
            c["cellStyle"] = {"display": "flex", "justifyContent": "center", 'textAlign': 'center'}
            c["headerClass"] = 'emo-header'
            c["sortable"] = False
            c["filter"] = False

        if c['field'] in ['SUBJECTS']:
            c["cellStyle"] = {"display": "flex", "justifyContent": "center", 'textAlign': 'center'}
            c["sortable"] = False
            c["filter"] = False
            c["headerClass"] = "emo-header-not-center"

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
        dcc.Download(id="download-file"),
        dag.AgGrid(
            id='ag-analyses',
            columnDefs=columnDefs,
            columnSize='responsiveSizeToFit',
            rowData=[],
            dashGridOptions={
                "headerHeight": 90,
                "floatingFiltersHeight": 40,
                "rowHeight": 40,
                "theme": {"function": 'themeAlpine.withPart(agGrid.colorSchemeDark).withParams({columnBorder: true, rowBorder: true, wrapperBorder: true})'},
            },
            defaultColDef={
                "sortable": True,
                "filter": True,
                "floatingFilter": True,
                "resizable": True,
                "headerClass": "emo-header-not ag-header-cell-center",
            },
            className="no-padding-grid",
        ),
        html.Label('0', id='label-analyses-rowcount2'),
    ]

    return content


def load_analyses(refresh=False):
    return load_data(refresh=refresh)


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
    #if 'NOTES' in df:
    #    df['NOTES'] = df['NOTES'].str.slice(0, 20)

    # Count SUBJECTS list
    _mask = df['SUBJECTS'].notna() & (df['SUBJECTS'].str.len() > 0)
    df.loc[_mask, 'SUBJECTS'] = (df.loc[_mask, 'SUBJECTS'].str.split(r'[,\n\s]+').apply(lambda x: f"n={len([i for i in x if i])}"))

    # Change blanks to asterisk
    df.loc[df['SUBJECTS'].str.len() == 0, 'SUBJECTS'] = '*'

    # Get project options
    proj_options = df.PROJECT.unique()

    # Filter by project before loading inv and status options
    df = filter_data(df, projects=selected_proj)

    lead_options = sorted(df['INVESTIGATOR'].unique())
    status_options = sorted(df['STATUS'].unique())
    proj = utils.make_options(proj_options)
    lead = utils.make_options(lead_options)
    status = utils.make_options(status_options)

    logger.debug(f'loaded options:{proj_options}:{lead_options}')

    # Filter data based on dropdown values
    df = filter_data(
        df,
        leads=selected_lead, 
        statuses=selected_status)

    # Map statuses to emoji
    df['STATUS'] = df['STATUS'].map(STATUS2EMO).fillna('?')
 
    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        _link = r['EDIT']

        _id = r['ID']
        r['ID'] = f'[{_id}]({_link})'

        _name = r['NAME']
        r['NAME'] = f'[{_name}]({_link})'

        if r['REPORT']:
            r['REPORT'] = '📊'

        if r['BATCH']:
            r['BATCH'] = '📋'

        if r['LOGFILE']:
            r['LOGFILE'] = '📄'

        if r['COVARS']:
            r['COVARS'] = '📗'

        # Make a link
        #if not r['OUTPUTS']:
        #    pass
        #elif r['OUTPUTLINK']:
        #    _link = r['OUTPUTLINK']
        #    r['OUTPUTS'] = f'[📁]({_link})'
        #elif '/' in r['OUTPUTS']:
        #    _link = r['OUTPUTS']
        #    r['OUTPUT'] = f'[📁]({_link})'

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
    Output("download-file", "data"),
    Input("ag-analyses", "cellClicked"),
    Input("ag-analyses", "rowData"),
    )
def get_file(active_cell, rows):

    if not active_cell:
        raise PreventUpdate

    print(active_cell)

    col_id = active_cell['colId']
  
    if col_id not in ['COVARS', 'REPORT', 'LOGFILE', 'BATCH']:
        raise PreventUpdate

    row_id = active_cell['rowIndex']
    project_id = rows[row_id]['PROJECT']
    repeat_id = rows[row_id]['REPEATID']
    output_id = rows[row_id]['OUTPUT']

    print(col_id, row_id, project_id, repeat_id)

    if col_id == 'COVARS':
        content, headers = export_covar_file(project_id, repeat_id)
        filename = headers.get('name', 'covariates.csv')
    elif col_id == 'REPORT':
        content, headers = export_report_file(project_id, repeat_id)
        filename = headers.get('name', 'report.pdf')
    elif col_id == 'BATCH':
        content, headers = export_batch_file(project_id, repeat_id)
        filename = headers.get('name', 'batch.slurm')
    elif col_id == 'LOGFILE':
        content, headers = export_log_file(project_id, repeat_id)
        filename = headers.get('name', 'log.txt')
    elif col_id == 'STATS':
        content, headers = export_stat_file(project_id, repeat_id)
        filename = headers.get('name', 'stats.csv')
    else:
        raise Exception('invalid click')

    #filename = f'{project_id}_{repeat_id}-{filename}'
    filename = f'{output_id}-{filename}'

    return dcc.send_bytes(content, filename)


if __name__ == "__main__":
    app.run(debug=True)
