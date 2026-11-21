"""Loopback HTTP fixtures for the configured providers."""

import argparse
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

TOKEN = "sift-gate-integration-key"
MODEL = "fixture/model"
PROVIDERS = {"openrouter", "routerai", "polza", "proxyapi", "aitunnel"}


class ProviderHandler(BaseHTTPRequestHandler):
    def route(self):
        path = self.path.strip("/").split("/")
        if len(path) < 4 or path[0] not in PROVIDERS or path[1:3] != ["api", "v1"]:
            self.reply(404, {"error": "unknown endpoint"})
            return None
        provider, endpoint = path[0], "/".join(path[3:])
        if provider == "routerai" and endpoint == "messages":
            authorized = self.headers.get("x-api-key") == TOKEN
            authorized &= self.headers.get("Authorization") is None
        else:
            authorized = self.headers.get("Authorization") == f"Bearer {TOKEN}"
        if endpoint == "messages":
            authorized &= self.headers.get("anthropic-version") == "2023-06-01"
        if not authorized:
            self.reply(401, {"error": "incorrect authentication headers"})
            return None
        return provider, endpoint

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/public/aitunnel/models/chat":
            if self.headers.get("Authorization") or self.headers.get("x-api-key"):
                self.reply(400, {"error": "public catalog should not receive credentials"})
            else:
                self.reply(200, {MODEL: {"prompt_cost": 20, "completion_cost": 30}})
            return
        if self.path == "/health":
            self.reply(200, {"status": "ok"})
            return
        route = self.route()
        if route is None:
            return
        provider, endpoint = route
        if endpoint != "models":
            self.reply(404, {"error": "unknown catalog"})
            return
        model = {"id": MODEL}
        if provider == "polza":
            model["top_provider"] = {"pricing": {
                "currency": "RUB", "prompt_per_million": "420", "completion_per_million": "600"
            }}
        elif provider == "routerai":
            model["pricing"] = {"prompt": "0.0001", "completion": 0.0002}
        elif provider == "proxyapi":
            model["object"] = "model"
        else:
            model["pricing"] = {"prompt": "0.000003", "completion": "0.000004"}
        self.reply(200, {"data": [model]})

    def do_POST(self):
        route = self.route()
        if route is None:
            return
        provider, endpoint = route
        if endpoint not in {"chat/completions", "messages"}:
            self.reply(404, {"error": "unknown inference endpoint"})
            return
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if body.get("model") != MODEL or not body.get("messages"):
            self.reply(400, {"error": "expected mapped model and messages"})
            return
        scenario = body["messages"][0]["content"]
        if provider == "routerai" and scenario == "no-response":
            # Keep the connection open without writing headers; bound the fixture lifetime.
            time.sleep(2)
            return
        if provider == "routerai" and scenario == "slow":
            time.sleep(0.25)
        if provider == "routerai" and scenario == "rate-limit":
            self.reply(429, {"error": {"type": "rate_limit_error"}})
            return

        if endpoint == "messages":
            message = {
                "id": f"msg-{provider}", "type": "message", "role": "assistant",
                "model": MODEL, "content": [{"type": "text", "text": provider}],
                "stop_reason": "end_turn", "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
            if body.get("stream"):
                frames = [
                    ("message_start", {"message": dict(message, content=[], stop_reason=None)}),
                    ("content_block_start", {"index": 0, "content_block": {"type": "text", "text": ""}}),
                    ("content_block_delta", {"index": 0, "delta": {"type": "text_delta", "text": provider}}),
                    ("content_block_stop", {"index": 0}),
                    ("message_delta", {"delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 1}}),
                    ("message_stop", {}),
                ]
                self.send_sse([
                    f"event: {kind}\r\ndata: {json.dumps(dict(data, type=kind))}\r\n\r\n"
                    for kind, data in frames
                ])
            else:
                self.reply(200, message)
            return

        if body.get("stream"):
            def chunk(delta, finish_reason=None):
                return json.dumps({
                    "id": f"chat-{provider}", "object": "chat.completion.chunk", "model": MODEL,
                    "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
                })

            frames = [
                f"data: {chunk({'role': 'assistant'})}\r\n\r\n",
                f"data: {chunk({'content': provider})}\r\n\r\n",
                f"data: {chunk({}, 'stop')}\r\n\r\n",
                "data: [DONE]\r\n\r\n",
            ]
            if provider == "routerai" and scenario == "fail-before-sse":
                frames = ['data: {"error":{"code":503}}\r\n\r\n']
            if provider == "routerai" and scenario == "disconnect-after-sse":
                frames = frames[:2]
            self.send_sse(frames)
        else:
            self.reply(200, {
                "id": f"chat-{provider}", "object": "chat.completion", "model": MODEL,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": provider},
                             "finish_reason": "stop"}],
            })

    def send_sse(self, frames):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.end_headers()
        self.wfile.write(b": heartbeat\r\n\r\n")
        for frame in frames:
            self.wfile.write(frame.encode())
            self.wfile.flush()

    def log_message(self, *args):
        pass


def run_tests(url, offline):
    env = os.environ | {"SIFT_GATE_MOCK_URL": url, "SIFT_GATE_MOCK_KEY": TOKEN}
    command = ["cargo", "test", "--test", "http_providers"]
    if offline:
        command.append("--offline")
    command += ["--", "--ignored"]
    return subprocess.call(command, env=env, cwd=Path(__file__).resolve().parents[2])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--url", help="Use an existing mock server, e.g. from Docker Compose")
    parser.add_argument("--offline", action="store_true", help="Run Cargo offline")
    args = parser.parse_args()
    if args.url and args.serve:
        parser.error("--url and --serve are mutually exclusive")
    if args.url:
        return run_tests(args.url, args.offline)
    with ThreadingHTTPServer(("0.0.0.0", 8080) if args.serve else ("127.0.0.1", 0),
                             ProviderHandler) as server:
        if args.serve:
            server.serve_forever()
        else:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                return run_tests(f"http://127.0.0.1:{server.server_port}", args.offline)
            finally:
                server.shutdown()
                worker.join()


if __name__ == "__main__":
    raise SystemExit(main())
