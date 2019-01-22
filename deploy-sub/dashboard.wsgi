from dashboard import DaxDashboard

config_file = '/var/www/dashboard/config.yaml'
daxdash = DaxDashboard(config_file, url_base_pathname='/dashboard/')
application = daxdash.app.server
