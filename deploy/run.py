#! /usr/bin/env python

from dashboard import DaxDashboard

config_file = '/var/www/dashboard/config.yaml'
daxdash = DaxDashboard(config_file)
app = daxdash.get_app()

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', threaded=True)
