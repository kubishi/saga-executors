"""Direct transport (Slurm, default): node-local scratch + pull from a per-machine data server.

Needs no shared filesystem, ssh or scp, only the Python standard library.

  send(object, data)  write <scratch>/<run>/<task>.<object> on this machine
                      (temp file + rename) -> handoffSeconds. A local disk write,
                      wherever the consumer is.
  recv(peer)          the producer's machine is in FID_INPUT_NODES. On the same
                      machine: read the local file. On another machine: HTTP GET
                      from that machine's data server, into memory
                      -> inputReadSeconds (includes the network transfer).
  leaf                writes <scratch>/<run>/<task> = b"done"

The driver starts a consumer once its producers' steps have COMPLETED (their
outputs are written by then), so a cross-machine transfer is pulled by the consumer
after it starts.

Data server: one per machine, its own Slurm step for the whole job:
    python task.pyz serve --run R --node M --scratch S [--peers a,b] [--bind ADDR] [--port 0]
(token from FID_TOKEN). It creates <scratch>/<run>, binds to the address of the
interface that reaches the other machines (or --bind) on a free port, prints one
JSON line {"fid_server": M, "host", "port", ...}, serves GET /obj/<run>/<key>
(sendfile, many requests at once, X-Fid-Token required) and GET /status/<run>
(the transfers it has served: object, bytes, client address and, on this
machine's clock, received / start / body_start / end), and on POST /shutdown/<run> or SIGTERM deletes
<scratch>/<run> (and --cleanup files) and exits.

Env (task side, besides the core's FID_*): FID_TRANSPORT=direct, FID_RUN, FID_TASK,
FID_NODE, FID_SCRATCH (node-local root), FID_SERVERS ("machine=host:port,..."),
FID_INPUT_NODES ("producer.object=machine,..."), FID_TOKEN.
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import platform
import re
import secrets
import shutil
import signal
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fidelity_core import PhaseClock, parse_map, where, write_payload

TOKEN_HEADER = "X-Fid-Token"
CHUNK = 1 << 20
FETCH_TIMEOUT = 600
KEY_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.+:-]{0,200}$")


def valid_key(s: str) -> bool:
    """Run ids, task names and object keys become path components and URL segments."""
    return bool(KEY_RE.match(s)) and ".." not in s


def parse_servers(s: str) -> dict[str, tuple[str, int]]:
    out = {}
    for m, addr in parse_map(s, "=").items():
        host, _, port = addr.rpartition(":")
        out[m] = (host, int(port))
    return out


def request(addr: tuple[str, int], method: str, path: str, token: str, body: bytes | None = None,
            timeout: float = 30.0) -> tuple[int, bytes]:
    """One HTTP request to a data server; returns (status, body)."""
    conn = http.client.HTTPConnection(addr[0], addr[1], timeout=timeout, blocksize=CHUNK)
    try:
        conn.request(method, path, body=body, headers={TOKEN_HEADER: token})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.tmp.{secrets.token_hex(6)}")
    try:
        with open(tmp, "wb") as f:
            write_payload(f, data)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Task side
# ---------------------------------------------------------------------------

class DirectTransport:
    def __init__(self, env=None) -> None:
        env = os.environ if env is None else env
        self.clock = PhaseClock()
        self.run_id = env["FID_RUN"]
        self.name = env["FID_TASK"]
        self.node = env["FID_NODE"]
        self.dir = Path(env["FID_SCRATCH"]) / self.run_id
        self.dir.mkdir(parents=True, exist_ok=True)        # normally made by the server
        self.token = env.get("FID_TOKEN", "")
        self.servers = parse_servers(env.get("FID_SERVERS", ""))
        self.input_nodes = parse_map(env.get("FID_INPUT_NODES", ""), "=")
        self.pulled = 0
        self.last_recv_info: dict = {}       # measurement only, read by the core's timeline

    def recv(self, peer: str) -> bytes:
        self.clock.recv_begin()
        src = self.input_nodes.get(peer, self.node)
        self.last_recv_info = {"sourceNode": src, "via": "local" if src == self.node else "pull"}
        if src == self.node:
            with open(self.dir / peer, "rb") as f:
                data = f.read()
        else:
            if src not in self.servers:
                raise RuntimeError(f"no data server for {src} (input {peer})")
            status, data = request(self.servers[src], "GET", f"/obj/{self.run_id}/{peer}", self.token,
                                   timeout=FETCH_TIMEOUT)
            if status != 200:
                raise RuntimeError(f"pull {peer} from {src}: {status} {data[:200]!r}")
            self.pulled += len(data)
        self.clock.recv_end(len(data))
        return data

    def send(self, output: str | None, data: bytes) -> None:
        self.clock.send_begin()
        write_atomic(self.dir / (self.name if output is None else f"{self.name}.{output}"), data)
        self.clock.send_end(len(data))

    def close(self) -> dict:
        rec = self.clock.record()
        rec.update(where())
        rec.update({"task": self.name, "node": self.node, "bytesPulled": self.pulled})
        return rec


# ---------------------------------------------------------------------------
# Data server (one per machine)
# ---------------------------------------------------------------------------

class DataServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, run_id: str, node: str, scratch: str | Path, token: str, host: str = "127.0.0.1",
                 port: int = 0) -> None:
        if not valid_key(run_id):
            raise ValueError(f"bad run id {run_id!r}")
        self.run_id, self.node, self.token = run_id, node, token
        self.root = Path(scratch) / run_id
        self.root.mkdir(parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.lock = threading.Lock()
        self.served: list[dict] = []
        self.cleanup_paths: list[Path] = []
        super().__init__((host, port), _Handler)

    def status(self) -> dict:
        with self.lock:
            return {"node": self.node, "run": self.run_id, "unix": time.time(), "served": list(self.served)}

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)
        for p in self.cleanup_paths:
            try:
                p.unlink()
            except OSError:
                pass


class _Handler(BaseHTTPRequestHandler):
    server_version = "fid-server"
    protocol_version = "HTTP/1.0"                    # one request per connection
    server: DataServer

    def log_message(self, fmt, *args):               # quiet; errors are logged explicitly
        pass

    def _reply(self, code: int, body: bytes | str | dict = b"") -> None:
        ctype = "text/plain"
        if isinstance(body, dict):
            body, ctype = json.dumps(body).encode(), "application/json"
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _route(self, verb: str) -> str | None:
        """Check the token and parse /<verb>/<run>[/<key>]; returns key ("" if none) or None."""
        if not secrets.compare_digest(self.headers.get(TOKEN_HEADER, ""), self.server.token):
            self._reply(403, "bad token")
            return None
        parts = self.path.split("?", 1)[0].strip("/").split("/")
        if len(parts) not in (2, 3) or parts[0] != verb or parts[1] != self.server.run_id:
            self._reply(404, "unknown path")
            return None
        key = parts[2] if len(parts) == 3 else ""
        if key and not valid_key(key):
            self._reply(400, "bad key")
            return None
        return key

    def do_GET(self):
        received = time.time()                       # measurement only (served[].received)
        if self.path == "/healthz":
            return self._reply(200, "ok")
        if self.path.startswith("/status/"):
            if self._route("status") is not None:
                self._reply(200, self.server.status())
            return
        key = self._route("obj")
        if not key:
            return None if key is None else self._reply(400, "missing key")
        path = self.server.root / key
        start = time.time()
        try:
            f = open(path, "rb")
        except FileNotFoundError:
            return self._reply(404, f"{key} not here")
        with f:
            size = os.fstat(f.fileno()).st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            self.wfile.flush()
            body_start = time.time()                 # measurement only: headers sent, body begins
            self.connection.sendfile(f)
        with self.server.lock:
            self.server.served.append({"object": key, "bytes": size, "to": self.client_address[0],
                                       "start": start, "end": time.time(), "received": received,
                                       "body_start": body_start, "server_node": self.server.node})

    def do_POST(self):
        if self._route("shutdown") is not None:
            self._reply(200, "bye")
            threading.Thread(target=self.server.shutdown, daemon=True).start()


def bind_address(node: str, peers: list[str]) -> str:
    """This machine's address on the interface that reaches the other machines
    (the cluster network), found by a UDP connect (no packet is sent). With no
    other machine, loopback."""
    others = [p for p in peers if p not in (node, socket.gethostname())]
    for peer in others:
        try:
            ip = socket.gethostbyname(peer)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect((ip, 9))
                addr = s.getsockname()[0]
            if not addr.startswith("127."):
                return addr
        except OSError:
            continue
    if others:
        raise RuntimeError(f"cannot find a non-loopback address towards {others}; pass --bind")
    return "127.0.0.1"


def _log(msg: str) -> None:
    print(f"[fid-server {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def serve_main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(prog="task.pyz serve")
    ap.add_argument("--run", required=True)
    ap.add_argument("--node", required=True)
    ap.add_argument("--scratch", required=True)
    ap.add_argument("--peers", default="", help="comma-separated machines of the job")
    ap.add_argument("--bind", default="", help="address to bind (default: towards the peers)")
    ap.add_argument("--port", type=int, default=0, help="0 = any free port (reported)")
    ap.add_argument("--cleanup", action="append", default=[], help="extra file to delete on exit")
    a = ap.parse_args(argv)
    token = os.environ.get("FID_TOKEN", "")
    if not token:
        sys.exit("FID_TOKEN must be set")
    host = a.bind or bind_address(a.node, [p for p in a.peers.split(",") if p])
    server = DataServer(a.run, a.node, a.scratch, token, host, a.port)
    server.cleanup_paths = [Path(p) for p in a.cleanup]

    def on_term(signum, _frame):
        _log(f"signal {signum}: shutting down")
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    print(json.dumps({"fid_server": a.node, "host": host, "port": server.server_address[1],
                      "hostname": socket.gethostname(), "pid": os.getpid(),
                      "python": platform.python_version(), "scratch": str(server.root)}), flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        server.cleanup()
        _log("stopped, scratch removed")
