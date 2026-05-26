import sys
import socket
import logging
import platform

import webview

from daxdashboard.login import app
from daxdashboard.serv import ServerThread


def get_free_port():
    s = socket.socket()
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def main():
    port = get_free_port()
    url = f"http://localhost:{port}"
    print(f'{url=}')

    # Start our flask web server in separate thread
    server = ServerThread(app.server, port)
    server.daemon = True
    server.start()

    if platform.system() == 'Windows':
        os.system(f'start {url}')
    else:
        webview.create_window('DAXdashboard', url, width=1000, height=800)
        webview.start()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.exception('crash:{err}')
        sys.exit(1)
