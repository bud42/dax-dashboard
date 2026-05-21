import sys
import socket
import logging

import webview

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

    url = f"http://127.0.0.1:{port}"

    print(f'{url=}')

    webview.create_window('daxdashboard', url)

    webview.start(gui='edgechromium')


if __name__ == "__main__":
    try:
        print('calling main')
        main()
        print('main finished')
    except Exception as err:
        print(err)
        logging.exception("Fatal crash")
        sys.exit(1)
