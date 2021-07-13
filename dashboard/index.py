import os

import dash_core_components as dcc
import dash_html_components as html

from app import app
from qa import gui as qa
from activity import gui as activity
from stats import gui as stats


def get_layout():
    qa_content = qa.get_content()
    activity_content = activity.get_content()
    stats_content = stats.get_content()

    report_content = [
        html.Div(
            dcc.Tabs(id='tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='QA', value='1', children=qa_content),
                dcc.Tab(
                    label='Activity', value='2', children=activity_content),
                dcc.Tab(
                    label='Stats', value='3', children=stats_content),
            ]),
            #style={
            #    'width': '100%', 'display': 'flex',
            #    'align-items': 'center', 'justify-content': 'left'}
            style={
                'width': '90%', 'display': 'flex',
                'align-items': 'center', 'justify-content': 'center'}
            )]

    footer_content = [
        html.Hr(),
        html.H5('F: Failed'),
        html.H5('P: Passed QA'),
        html.H5('Q: To be determined')]

    return html.Div([
        html.Div(children=report_content, id='report-content'),
        html.Div(children=footer_content, id='footer-content')])


# For gunicorn to work correctly
server = app.server  

# This loads a css template maintained by the Dash developer
app.css.config.serve_locally = False
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

# Check for user passwords file
if os.path.exists('/opt/dashboard/dashboardsecrets.py'):
    # Use very basic authentication
    import dash_auth
    from dashboardsecrets import VALID_USERNAME_PASSWORD_PAIRS
    auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

# Set the content
app.layout = get_layout()


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
