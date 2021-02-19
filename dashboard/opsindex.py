import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_auth

from app import app

from apps import ops

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
    if pathname == '/ops':
        # return ops.layout
        print('display_page:ops')
        return ops.layout
    else:
        print('display_page:')
        return ops.layout


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')  # , debug=True)
