import sys
sys.path.insert(0, "/var/www/dashboard")

from run import app

server = app.server
application = server
