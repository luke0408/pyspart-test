from http.server import BaseHTTPRequestHandler, HTTPServer
import os


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        self.send_response(404)
        self.end_headers()


def main() -> None:
    port = int(os.getenv("API_PORT", "8000"))
    server = HTTPServer(("", port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
