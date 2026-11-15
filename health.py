"""The health endpoint xhost checks on worker processes (edges, render workers)."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def start_health_server(port):
    """Serve 200 on every path so the xhost platform health check passes.

    The bot is a worker with no web surface, but xhost fails a deploy if nothing is
    listening on $PORT or / returns non-2xx. Started before the bot boots so the check
    succeeds regardless of how long startup takes.
    """
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'ok')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    threading.Thread(target=server.serve_forever, name='Health', daemon=True).start()
    return server
