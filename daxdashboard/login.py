import os

from flask import Flask, request, redirect, session, jsonify, url_for, render_template
from flask_login import login_user, LoginManager, UserMixin, logout_user, current_user
from cryptography.fernet import Fernet
import dash
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

from .extensions import cache
from . import content
from .log import logger
from .utils import encrypt_key, get_xnat_alias, get_redcap_info


# Load custom templates
templates = os.path.join(os.path.dirname(__file__), 'templates')

# Connect to an underlying flask server
server = Flask(__name__, template_folder=templates)

@server.before_request
def check_login():
    logger.debug('checking login')
    if request.method == 'GET':

        if request.path in ['/login', '/logout']:
            # nothing to check here
            return

        if is_authenticated():
            logger.debug(f'user is authenticated:{current_user.id}')
            return

        # nothing to check so user must log in
        return redirect(url_for('login'))
    else:
        if current_user:
            if request.path == '/login' or is_authenticated():
                return

        logout_user()
        return


def is_authenticated():
    return current_user and current_user.is_authenticated


@server.route('/login', methods=['POST', 'GET'])
def login(message=""):
    try:
        if request.method == 'POST':
            if request.form:
                rc_host = request.form['rchost']
                rc_key = request.form['rckey']
                xnat_host = request.form['xnathost']
                xnat_user = request.form['xnatuser']
                _xnat_pass = request.form['xnatpass']

                # get xnat alias
                if xnat_user == 'demo':
                    xnat_alias = 'demo'
                    xnat_token = 'demo'
                else:
                    try:
                        (xnat_alias, xnat_token) = get_xnat_alias(
                            xnat_host, xnat_user, _xnat_pass)
                    except Exception as err:
                        print('XNAT connection failed')
                        logger.debug(f'redirecting to home')
                        return redirect(url)

                if rc_host and rc_key:
                    try:
                        redcap_info = get_redcap_info(rc_host, rc_key)
                        session['rc_version'] = redcap_info['redcap_version']
                        session['rc_pid'] = redcap_info['redcap_pid']
                    except Exception as err:
                        print('REDCap connection failed')
                        logger.debug(f'redirecting to home')
                        return redirect(url)

                # Now we log the user into our app
                try:
                    login_user(User(xnat_user))

                    if xnat_alias and xnat_token:
                        fernet = Fernet(server.config['SECRET_KEY'])
                        session['xnat_host'] = xnat_host
                        session['xnat_alias'] = encrypt_key(fernet, xnat_alias)
                        session['xnat_token'] = encrypt_key(fernet, xnat_token)
                        session['rc_host'] = rc_host
                        if rc_key:
                            session['rc_key'] = encrypt_key(fernet, rc_key)


                    if session.get('url', False):
                        # redirect to original target
                        url = session['url']
                        logger.debug(f'redirecting to target url:{url}')
                        session['url'] = None
                        return redirect(url)
                    else:
                        # redirect to home
                        logger.debug('redirecting to home')
                        return redirect('/')
                except Exception as err:
                    logger.debug(f'login failed:{err}')
                    message = 'login failed, try again'

        else:
            if current_user:
                if current_user.is_authenticated:
                    try:
                        logger.debug('redirecting to /')
                        return redirect('/')
                    except Exception as err:
                        logger.debug(f'cannot log in, try again:{err}')
                        message = 'login failed, try again'

    except Exception as err:
        logger.error(f'login error, route to logout:{err}')
        return logout()

    logger.debug('rendering login.html')
    return render_template('login.html', message=message)


@server.route('/logout', methods=['GET'])
def logout():
    if current_user:
        if current_user.is_authenticated:
            logout_user()
    return render_template('login.html', message="you have been logged out")

# Prep the configs for the app
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
assets_path = os.path.join(os.path.dirname(__file__), 'assets')
stylesheets = [dbc.themes.DARKLY, dbc_css]
load_figure_template("darkly")

# Build the dash app with the configs
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=stylesheets,
    assets_folder=assets_path,
    #suppress_callback_exceptions=True,
)

# Set the title to appear on web pages
app.title = 'dashboard'

# Make a key we can use for encryption
server.config.update(SECRET_KEY=Fernet.generate_key())

# Login manager object used to login / logout users
login_manager = LoginManager()
login_manager.init_app(server)
login_manager.login_view = "/login"

# Make a cache to save query results
cache.init_app(app.server)


# Create the User class that sets id as xnat username
class User(UserMixin):
    def __init__(self, xnat_user):
        self.id = xnat_user


@login_manager.user_loader
def load_user(username):
    """This function loads the user by user id."""
    logger.debug(f'loading user:{username}')
    return User(username)

# Set the main content
app.layout = content.get_content()

logger.debug(f'{app.layout}')

if __name__ == "__main__":
    app.run(debug=True)
