import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_auth
from flask import request

from app import app
from apps import qa, ops, settings, stats
from secrets import VALID_USERNAME_PASSWORD_PAIRS, USER_ACCESS

# Styling for the links to different pages
link_style = {
    'padding': '10px 14px',
    'display': 'inline-block',
    'text-decoration': 'none',
    'text-align': 'center'}

# This loads a css template maintained by the Dash developer
app.css.config.serve_locally = False
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

# Use very basic authentication
# viewers cannot create their own account and cannot change their password
auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

# Make the main app layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='menu-content', style={'float': 'right'}),
    html.Div([html.H1('DAX Dashboard')]),
    html.Div(id='page-content')])


# Make the callback for the links to load app pages
@app.callback(
    [Output('page-content', 'children'),
     Output('menu-content', 'children')],
    [Input('url', 'pathname')])
def display_page(pathname):
    menu_content = []
    username = request.authorization['username']
    layout = ''

    # Determine what pages this user can access
    cur_access = USER_ACCESS.get(username, [])

    if pathname[1:] not in cur_access:
        print('page not accessible to user')
        # nope, no access to that page, but everybody can access ops
        pathname = '/ops'

        # check for a better default page
        if 'qa' in cur_access:
            pathname = '/qa'
        elif 'stats' in cur_access:
            pathname = '/stats'

        print('rerouted to:' + pathname)

    # Now that we know the path, get the content
    if pathname == '/settings':
        print('display_page:settings')
        layout = settings.layout
    elif pathname == '/qa':
        print('display_page:qa')
        layout = qa.layout
    elif pathname == '/ops':
        # return ops.layout
        print('display_page:ops')
        layout = ops.layout
    elif pathname == '/stats':
        print('display_page:stats')
        layout = stats.layout
    else:
        # we shouldn't be here
        pass

    layout = html.Div([
        layout,
        html.Hr(),
        html.P(
            'Hi {}, thanks for using DAX Dashboard!'.format(username),
            style={'textAlign': 'right'})])

    # Build the top menu based on access to pages
    if len(cur_access) > 1:
        for i in cur_access:
            print('adding to menu:' + i)
            menu_content.append(
                dcc.Link(i, href='/' + i, style=link_style, target='_blank'))

    return [layout, menu_content]


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')  # , debug=True)
