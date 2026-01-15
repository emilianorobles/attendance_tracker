#!/usr/bin/env python3
"""
Simple test server for the attendance tracker.
Serves the HTML and proxies API calls to the FastAPI app.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from fastapi.testclient import TestClient
from app.main import app
import json
import urllib.parse
import os

class TestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            client = TestClient(app)

            if self.path == '/':
                # Serve the main page
                response = client.get('/')
                self.send_response(response.status_code)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(response.content)
            elif self.path.startswith('/attendance'):
                # Proxy attendance API calls
                query = urllib.parse.urlparse(self.path).query
                response = client.get(f'/attendance?{query}')
                self.send_response(response.status_code)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(response.content)
            elif self.path.startswith('/static/'):
                # Try to serve static files
                try:
                    filepath = '.' + self.path
                    if os.path.exists(filepath):
                        with open(filepath, 'rb') as f:
                            content = f.read()
                        self.send_response(200)
                        if self.path.endswith('.js'):
                            self.send_header('Content-type', 'application/javascript')
                        elif self.path.endswith('.json'):
                            self.send_header('Content-type', 'application/json')
                        elif self.path.endswith('.css'):
                            self.send_header('Content-type', 'text/css')
                        else:
                            self.send_header('Content-type', 'text/plain')
                        self.end_headers()
                        self.wfile.write(content)
                    else:
                        self.send_response(404)
                        self.end_headers()
                except Exception as e:
                    print(f"Error serving static file: {e}")
                    self.send_response(500)
                    self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            print(f"Error in do_GET: {e}")
            self.send_response(500)
            self.end_headers()

    def do_POST(self):
        try:
            client = TestClient(app)

            if self.path == '/attendance/justify':
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))

                response = client.post('/attendance/justify', json=data)
                self.send_response(response.status_code)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(response.content)
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            print(f"Error in do_POST: {e}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress log messages
        pass

if __name__ == '__main__':
    print("Starting test server on http://localhost:8080")
    print("Open your browser to http://localhost:8080")
    print("Use Ctrl+C to stop")
    server = HTTPServer(('localhost', 8080), TestHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped")
        server.server_close()