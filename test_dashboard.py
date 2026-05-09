import webbrowser

from daxdashboard.login import app

url = 'http://localhost:8050'

# Open URL in a new tab, if a browser window is already open.
webbrowser.open_new_tab(url)

# start up a dashboard app
app.run(host='0.0.0.0')
