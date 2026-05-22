import sys
import socket
import logging
import time

#import webview

from daxdashboard.login import app
from daxdashboard.serv import ServerThread


print("START")
print(sys.executable)
print(sys.frozen if hasattr(sys, "frozen") else "not frozen")


def get_free_port():
    s = socket.socket()
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def main():
    port = get_free_port()
    flask_app = app.server

    server = ServerThread(flask_app, port)
    server.daemon = True
    server.start()

    import webbrowser
    #import webview.platforms.edgechromium as edge
    #edge._state['debug'] = False
    #webview.settings['ALLOW_DOWNLOADS'] = True


    print('sleeping to wait for server...')
    time.sleep(15)


    url = "https://www.google.com"
    webbrowser.open_new_tab(url)

    
    url = f"http://localhost:{port}"
    print(f'{url=}')

    #webview.start(gui="edgechromium", debug=True)
    webbrowser.open_new_tab(url)


if __name__ == "__main__":
    try:
        print('calling main')
        main()
        print('main finished')
    except Exception as err:
        print(err)
        logging.exception("Fatal crash")
        sys.exit(1)
