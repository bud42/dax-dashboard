from pathlib import Path

from dash import dcc, html
import yaml
import dash_auth

from app import app
from qa import gui as qa
from activity import gui as activity
from stats import gui as stats
from reports import gui as reports
from issues import gui as issues
from admin import gui as admin


def get_layout():
    qa_content = qa.get_content()
    activity_content = activity.get_content()
    stats_content = stats.get_content()
    reports_content = reports.get_content()
    issues_content = issues.get_content()
    admin_content = admin.get_content()

    report_content = [
        html.Div(
            dcc.Tabs(id='tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='QA', value='1', children=qa_content),
                dcc.Tab(
                    label='Activity', value='2', children=activity_content),
                dcc.Tab(
                    label='Issues', value='3', children=issues_content),
                dcc.Tab(
                    label='Stats', value='4', children=stats_content),
                dcc.Tab(
                    label='Reports', value='5', children=reports_content),
                dcc.Tab(
                    label='Admin', value='6', children=admin_content),
            ]),
            style={
                'width': '90%', 'display': 'flex',
                'align-items': 'center', 'justify-content': 'center'},
        )
    ]

    footer_content = [
        html.Hr(),
        html.H5('F: Failed'),
        html.H5('P: Passed QA'),
        html.H5('Q: To be determined')]

    # Make the main app layout
    main_content = html.Div([
        html.Div([html.H1('DAX Dashboard')]),
        html.Div(children=report_content, id='report-content'),
        html.Div(children=footer_content, id='footer-content')])

    return main_content


# For gunicorn to work correctly
server = app.server

# This loads a css template maintained by the Dash developer
app.css.config.serve_locally = False
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

# Load user passwords to use dash's basic authentication
with open(Path.home().joinpath('dashboardsecrets.yaml'), 'rt') as file:
    _data = yaml.load(file, yaml.SafeLoader)
    auth = dash_auth.BasicAuth(app, _data['VALID_USERNAME_PASSWORD_PAIRS'])

# Set the content
app.layout = get_layout()


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
