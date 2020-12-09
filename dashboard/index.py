import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_auth

from app import app

from apps import qa, ops, settings

from secrets import VALID_USERNAME_PASSWORD_PAIRS

# Styling for the links to different pages
link_style = {
    'padding': '10px 14px',
    'display': 'inline-block',
    'text-decoration': 'none',
    'text-align': 'center'}

# Make the main app layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        dcc.Link('qa', href='/qa', style=link_style),
        dcc.Link('ops', href='/ops', style=link_style),
        dcc.Link('settings', href='/settings', style=link_style)],
        style={'float': 'right'}),
    html.Div([html.H1('DAX Dashboard')]),
    html.Div(id='page-content')])

#footer_content = [
#        html.Hr(),
#        html.Hr(),
#        html.Div([
#            html.P('DAX Dashboard', style={'textAlign': 'right'})])]

# This loads a css template maintained by the Dash developer
app.css.config.serve_locally = False
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

# Use very basic authentication
auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)


# Make the callback for the links to load app pages
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/settings':
        print('display_page:settings')
        return settings.layout
    elif pathname == '/qa':
        print('display_page:qa')
        return qa.layout
    elif pathname == '/ops':
        # return ops.layout
        print('display_page:ops')
        return ops.layout
    else:
        print('display_page:')
        return qa.layout


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')  # , debug=True)
