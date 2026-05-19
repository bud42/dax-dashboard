import threading
from werkzeug.serving import make_server

class ServerThread(threading.Thread):
    def __init__(self, flask_app, port):
        threading.Thread.__init__(self)
        self.server = make_server("127.0.0.1", port, flask_app)
        self.ctx = flask_app.app_context()
        self.ctx.push()

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()
