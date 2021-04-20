import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from app import app
from qa import gui as qa
from stats import gui as stats


server = app.server  # for gunicorn to work correctly

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
    layout = ''

    if pathname == '/stats':
        layout = stats.layout
    else:
        layout = qa.layout

    # Wrap the layout with a footer
    layout = html.Div([
        layout,
        html.Hr(),
        html.P(
            'Hi, thanks for using DAX Dashboard!',
            style={'textAlign': 'right'})])

    # Build the top menu
    cur_access = ['qa', 'stats']
    for i in cur_access:
        menu_content.append(
            dcc.Link(i, href='/' + i, style=link_style, target='_blank'))

    return [layout, menu_content]


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
