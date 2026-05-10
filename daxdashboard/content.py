"""dash index page."""
import logging

from dash import html
import dash_bootstrap_components as dbc

from .app import app
from .pages import hub, qa, queue, processors, analyses
from .log import logger


def _footer_content():
    content = []

    content.append(html.Hr())

    content.append(
        html.Div([
            dbc.Row([
                dbc.Col(
                ),
                dbc.Col(
                    html.A('xnat', href='https://xnat.vanderbilt.edu/xnat'),
                ),
                dbc.Col(
                    html.A('logout', href='../logout'),
                ),
            ]),
            ],
            style={'textAlign': 'center'},
        )
    )

    return content


def get_content():
    tabs = ''
    content = ''
    footer_content = ''

    tabs = dbc.Tabs([
        dbc.Tab(
            label='HOME',
            tab_id='tab-hub',
            children=hub.get_content(),
        ),
        dbc.Tab(
            label='QA',
            tab_id='tab-qa',
            children=qa.get_content(),
        ),
        dbc.Tab(
            label='Queue',
            tab_id='tab-queue',
            children=queue.get_content(),
        ),
        dbc.Tab(
            label='Processors',
            tab_id='tab-processors',
            children=processors.get_content(),
        ),
        dbc.Tab(
            label='Analyses',
            tab_id='tab-analyses',
            children=analyses.get_content(),
        )],
        active_tab="tab-hub",
    )    

    footer_content = _footer_content()

    content = html.Div(
        className='dbc',
        style={'marginLeft': '20px', 'marginRight': '20px'},
        children=[
            html.Div(id='report-content', children=[tabs]),
            html.Div(id='footer-content', children=footer_content)
    ])

    return content
