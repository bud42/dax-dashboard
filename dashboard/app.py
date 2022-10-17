import dash


app = dash.Dash(__name__)

server = app.server
app.config.suppress_callback_exceptions = True

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')

#import warnings
#warnings.simplefilter(action='ignore', category=FutureWarning)
