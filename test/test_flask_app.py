import sys
import os
import argparse
import flask
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dashboard import DaxDashboard

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    args = parser.parse_args()
    print(args)

    config_file = args.config

    print(config_file)

    server = flask.Flask("DAX Dashboard")

    daxdash = DaxDashboard(config_file, server=server)

    print(daxdash.config)

    daxdash.run()
