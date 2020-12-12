import logging

from dash.exceptions import PreventUpdate
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State


from app import app

from . import qadata
from . import utils

# opens a form to set projects,
# button to save and load types for these projects
# then select types, click button to save. this will initialize a new
# pickle file that will be used as possible values going forward, i.e.
# the level 1 filter


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


def get_settings_content():
    content = [
        dcc.Loading(id="loading-settings", children=[
            dcc.Dropdown(
                id='dropdown-settings-proj', multi=True,
                placeholder='Select Project(s)'),
            dcc.Dropdown(
                id='dropdown-settings-ptype', multi=True,
                placeholder='Select Processing Type(s)'),
            dcc.Dropdown(
                id='dropdown-settings-stype', multi=True,
                placeholder='Select Scan Type(s)'),
            html.Button('Load Options', id='button-settings-load'),
            html.Button('Save', type='submit', id='button-settings-save')])]

    return content


def get_layout():
    logging.debug('get_layout')

    settings_content = get_settings_content()

    report_content = [
        html.Div(
            dcc.Tabs(id='settings-tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='Settings', value='1', children=settings_content),
            ]),
            style={
                'width': '100%', 'display': 'flex',
                'align-items': 'center', 'justify-content': 'center'})]

    return html.Div(children=report_content, id='settings-report-content')


@app.callback(
    [Output('dropdown-settings-proj', 'options'),
     Output('dropdown-settings-ptype', 'options'),
     Output('dropdown-settings-stype', 'options'),
     Output('dropdown-settings-proj', 'value'),
     Output('dropdown-settings-ptype', 'value'),
     Output('dropdown-settings-stype', 'value')],
    [Input('button-settings-load', 'n_clicks')])
def update_settings(load_n_clicks):
    logging.debug('update_settings')
    print('load_n_clicks=', load_n_clicks)

    selected_proj = []
    selected_ptype = []
    selected_stype = []
    proj_options = []
    ptype_options = []
    stype_options = []

    # this should always be called so that it is current b/c it's quickish
    proj_list = qadata.get_user_projects()

    # Load the data
    df = qadata.load_data()

    # Determine which projects are in the current data
    selected_proj = df.PROJECT.unique()

    # Load all possible types for currently selected projects only
    # ptype_list = get_ptypes(xnat, selected_proj) # this is too slow
    stype_list = sorted(qadata.get_stypes(selected_proj))

    # Limit selected to those in the QA file
    type_set = set(df.TYPE.unique())
    selected_stype = list(set(stype_list).intersection(type_set))
    selected_ptype = list(type_set - set(selected_stype))

    ptype_list = selected_ptype

    # Get the dropdown options
    proj_options = utils.make_options(proj_list)
    stype_options = utils.make_options(stype_list)
    ptype_options = utils.make_options(ptype_list)

    #print(proj_list)
    #print(ptype_list)
    #print(stype_list)
    #print(selected_proj)
    #print(selected_ptype)
    #print(selected_stype)

    # Return table, figure, dropdown options
    logging.debug('update_all:returning data')
    return [proj_options, ptype_options, stype_options,
            selected_proj, selected_ptype, selected_stype]


@app.callback(
    [Output('settings-report-content', 'children')],
    [Input('button-settings-save', 'n_clicks')],
    [State('dropdown-settings-proj', 'value'),
     State('dropdown-settings-ptype', 'value'),
     State('dropdown-settings-stype', 'value')])
def save_settings(save_n_clicks, proj_value, ptype_value, stype_value):
    logging.debug('save_settings')
    print('save_n_clicks=', save_n_clicks)

    if not save_n_clicks:
        print('preventupdate')
        raise PreventUpdate("No data changed!")

    print('Setting new filters')
    df = qadata.set_data(proj_value, stype_value, ptype_value)

    print('Loading new page')

    # Display message to user
    msg = 'Settings have been saved! {} records loaded'.format(len(df))
    save_content = [
            html.H3(msg),
            dcc.Link(html.Button('View data'), href='/qa'),
            html.Br()]

    return [save_content]


# Build the layout that be used by top level index.py
logging.debug('DEBUG:making the layout')
layout = get_layout()
