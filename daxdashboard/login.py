import os
import hashlib

from flask import Flask, request, redirect, session, jsonify, url_for, render_template
from flask_login import login_user, LoginManager, UserMixin, logout_user, current_user
from cryptography.fernet import Fernet
import dash
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

from .extensions import cache
from . import content
from .log import logger
from .data import get_xnat_alias, get_redcap_info, encrypt_key, init_data


# Load custom templates, this helps app find our login.html
templates = os.path.join(os.path.dirname(__file__), 'templates')

# Connect to an underlying flask server
server = Flask(__name__, template_folder=templates)

# Make a key we can use for encryption
server.config.update(SECRET_KEY=Fernet.generate_key())


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
    xnat_enabled = False
    rc_enabled = False

    try:
        if request.method == 'POST':
            if request.form:
                # Get an encrypter
                fernet = Fernet(server.config['SECRET_KEY'])

                # Load credentials from form
                rc_host = request.form['rchost']
                rc_key = request.form['rckey']
                xnat_host = request.form['xnathost']
                xnat_user = request.form['xnatuser']
                _xnat_pass = request.form['xnatpass']
                session['xnat_host'] = xnat_host
                session['rc_host'] = rc_host

                # get xnat connection params
                if xnat_host and xnat_user and _xnat_pass:
                    print(f'logging into xnat:{xnat_host=}:{xnat_user=}')

                    try:
                        (xnat_alias, xnat_token) = get_xnat_alias(
                            xnat_host, xnat_user, _xnat_pass)
                        xnat_enabled = True
                    except Exception as err:
                        logger.debug('XNAT failed, redirecting to home')
                        return redirect('/')

                # get redcap connection params
                if rc_host and rc_key:
                    print(f'logging into redcap:{rc_host=}')

                    try:
                        session['rc_key'] = encrypt_key(fernet, rc_key)

                        rc_info = get_redcap_info(rc_host, rc_key)
                        session['rc_version'] = rc_info['redcap_version']
                        session['rc_pid'] = rc_info['redcap_pid']
                        rc_enabled = True
                    except Exception as err:
                        logger.error('REDCap failed, redirecting to home')
                        return redirect('/')

                print(f'{xnat_enabled=}:{rc_enabled=}')

                # Now we log the user into our app based on what is connected
                try:
                    if xnat_enabled:
                        # Login in with xnat username and projects
                        print(f'logging in with xnat_user:{xnat_user}')
                        login_user(User(xnat_user))
                        session['xnat_alias'] = encrypt_key(fernet, xnat_alias)
                        session['xnat_token'] = encrypt_key(fernet, xnat_token)
                    elif rc_enabled:
                        # Make up redcap user 
                        rc_user = hashlib.sha256(rc_key.encode()).hexdigest()
                        rc_user = f"rc:{rc_user}"

                        # Log in with made up name
                        print(f'logging in with rc_user')
                        login_user(User(rc_user))
                        session['rc_user'] = rc_user
                    else:
                        print('neither logged in, raising exception')
                        raise Exception('cannot log in to XNAT or REDCap')

                    init_data()

                    # We are logged in now so handle request
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


# Create the User class that sets id as xnat username
class User(UserMixin):
    def __init__(self, xnat_user):
        self.id = xnat_user

# Login manager object used to login / logout users
login_manager = LoginManager()
login_manager.init_app(server)
login_manager.login_view = "/login"

@login_manager.user_loader
def load_user(username):
    """This function loads the user by user id."""
    logger.debug(f'loading user:{username}')
    return User(username)

# Configure appearance
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
stylesheets = [dbc.themes.DARKLY, dbc_css]
load_figure_template("darkly")

# Build the dash app with the configs
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=stylesheets,
    title='dashboard',
)

# Make a cache to save query results
cache.init_app(app.server)

# Set the main content
app.layout = content.get_content()

logger.debug(f'{app.layout}')


def run_dev():
    app.run(debug=True)


if __name__ == "__main__":
    run_dev()
