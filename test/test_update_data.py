import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dashboard import DaxDashboard
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    args = parser.parse_args()
    print(args)

    config_file = args.config

    print(config_file)

    daxdash = DaxDashboard(config_file)

    print(daxdash.config)

    print('DEBUG:updating data')
    daxdash.update_data()
