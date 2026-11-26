#!/usr/bin/env python3
"""
TCP vs UDP Live Lab
===================
Sends the same numbered messages over real TCP and real UDP sockets, through a
proxy that makes the network behave badly (loss, delay, jitter), and shows
what each protocol does about it in a live browser dashboard.

    sender --UDP--> [udp proxy :9001] --UDP--> [udp receiver :9002]
    sender --TCP--> [tcp proxy :9003] --TCP--> [tcp receiver :9004]
                          |                             |
                    impairment knobs              metrics -> SSE -> browser

Pure Python standard library. Run:  python app.py   then open http://127.0.0.1:8080
"""
import argparse
import asyncio
import json
import random
import socket
import struct
import time
import webbrowser
from collections import deque
from pathlib import Path

HOST = "127.0.0.1"
UDP_PROXY, UDP_RECV = 9001, 9002
TCP_PROXY, TCP_RECV = 9003, 9004

HDR = struct.Struct("!IId")          # epoch, sequence number, send time
RECORD = 64                          # every message is a fixed 64 bytes
GRACE = 3.0                          # seconds before an unseen UDP packet counts as lost
STRIP = 80                           # packets shown in the live strip

now = time.perf_counter

# Live network conditions, changed from the dashboard.
cfg = {"loss": 5.0, "delay": 50.0, "jitter": 20.0, "rate": 20, "running": False}
LIMITS = {"loss": (0, 40), "delay": (0, 300), "jitter": (0, 150), "rate": (1, 100)}

epoch = 0          # bumped on reset so stale in-flight packets are ignored
seq_counter = 0


class Stats:
    """Per-protocol counters, filled in by the receiver side."""

    def __init__(self, name, reliable):
        self.name = name
        self.reliable = reliable
        self.reset()

    def reset(self):
        self.sent = self.received = self.lost = self.reordered = 0
        self.max_seq = -1
        self.lat_sum = 0.0
        self.jitter = 0.0
        self.last_lat = None
        self.recent = deque(maxlen=500)   # latencies for percentiles
        self.win = []                     # latencies since last snapshot
        self.status = {}                  # seq -> p/o/r/l
        self.pending = deque()            # (seq, send_time) awaiting arrival

    def on_send(self, seq, ts):
        self.sent += 1
        self.status[seq] = "p"
        self.pending.append((seq, ts))

    def on_recv(self, seq, ts):
        if self.status.get(seq) in ("o", "r"):
            return
        lat = (now() - ts) * 1000
        self.received += 1
        if seq < self.max_seq:
            self.reordered += 1
            self.status[seq] = "r"
        else:
            self.max_seq = seq
            self.status[seq] = "o"
        self.lat_sum += lat
        self.recent.append(lat)
        self.win.append(lat)
        if self.last_lat is not None:     # RFC 3550 style smoothed jitter
            self.jitter += (abs(lat - self.last_lat) - self.jitter) / 16
        self.last_lat = lat

    def expire(self):
        """UDP never retransmits: anything unseen after GRACE seconds is lost."""
        t = now()
        while self.pending and t - self.pending[0][1] > GRACE:
            seq, _ = self.pending.popleft()
            if self.status.get(seq) == "p" and not self.reliable:
                self.status[seq] = "l"
                self.lost += 1
        if len(self.status) > 400:
            for k in [k for k in self.status if k < self.sent - 300]:
                del self.status[k]

    def snapshot(self, dt):
        def pct(sorted_list, p):
            if not sorted_list:
                return None
            return sorted_list[min(len(sorted_list) - 1, int(len(sorted_list) * p))]

        w = sorted(self.win)
        self.win = []
        r = sorted(self.recent)
        resolved = self.received + self.lost
        first = max(0, self.sent - STRIP)
        return {
            "sent": self.sent,
            "recv": self.received,
            "lost": self.lost,
            "reordered": self.reordered,
            "loss_pct": 100 * self.lost / resolved if resolved else 0,
            "avg": self.lat_sum / self.received if self.received else None,
            "p95": pct(r, 0.95),
            "jitter": self.jitter if self.received > 1 else None,
            "win_p95": pct(w, 0.95),
            "tput": len(w) / dt,
            "strip": "".join(self.status.get(s, ".") for s in range(first, self.sent)),
        }


udp = Stats("udp", reliable=False)
tcp = Stats("tcp", reliable=True)


def nodelay(writer):
    sock = writer.get_extra_info("socket")
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


# ---------------------------------------------------------------- UDP side
class UdpReceiver(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        if len(data) >= HDR.size:
            ep, seq, ts = HDR.unpack_from(data)
            if ep == epoch:
                udp.on_recv(seq, ts)


class UdpProxy(asyncio.DatagramProtocol):
    """Forwards each datagram independently: it may be dropped, delayed, reordered."""

    def __init__(self, out_transport):
        self.out = out_transport

    def datagram_received(self, data, addr):
        if random.random() * 100 < cfg["loss"]:
            return                                        # dropped, gone for good
        d = cfg["delay"] + random.uniform(-cfg["jitter"], cfg["jitter"])
        asyncio.get_running_loop().call_later(max(0.0, d) / 1000, self.out.sendto, data)


# ---------------------------------------------------------------- TCP side
async def tcp_receiver(reader, writer):
    nodelay(writer)
    try:
        while True:
            data = await reader.readexactly(RECORD)
            ep, seq, ts = HDR.unpack_from(data)
            if ep == epoch:
                tcp.on_recv(seq, ts)
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


async def tcp_proxy(creader, cwriter):
    """
    Byte streams cannot lose bytes, so TCP's reaction to loss is simulated the
    way it shows up in practice: a 'lost' segment is retransmitted after a
    timeout (doubling each time it is lost again) and everything behind it
    waits in order. This is head-of-line blocking.
    """
    ureader, uwriter = await asyncio.open_connection(HOST, TCP_RECV)
    nodelay(cwriter)
    nodelay(uwriter)
    q = asyncio.Queue()

    async def intake():
        try:
            while True:
                data = await creader.readexactly(RECORD)
                q.put_nowait((now(), data))       # stamp after the bytes arrive
        except (asyncio.IncompleteReadError, ConnectionError):
            q.put_nowait(None)

    async def forward():
        while True:
            item = await q.get()
            if item is None:
                break
            arrived, data = item
            rto = 0.2 + 2 * cfg["delay"] / 1000           # retransmission timeout
            extra, tries = 0.0, 0
            while tries < 4 and random.random() * 100 < cfg["loss"]:
                extra += rto
                rto = min(rto * 2, 2.0)                   # exponential backoff
                tries += 1
            d = max(0.0, cfg["delay"] + random.uniform(-cfg["jitter"], cfg["jitter"])) / 1000
            wait = arrived + d + extra - now()
            if wait > 0:
                await asyncio.sleep(wait)
            uwriter.write(data)                           # strictly in order
        uwriter.close()

    try:
        await asyncio.gather(intake(), forward())
    finally:
        cwriter.close()


# ---------------------------------------------------------------- sender
async def sender():
    global seq_counter
    loop = asyncio.get_running_loop()
    udp_out, _ = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol, remote_addr=(HOST, UDP_PROXY))
    _, tcp_out = await asyncio.open_connection(HOST, TCP_PROXY)
    nodelay(tcp_out)
    pad = bytes(RECORD - HDR.size)
    nxt = now()
    while True:
        if not cfg["running"]:
            await asyncio.sleep(0.05)
            nxt = now()
            continue
        ts = now()
        pkt = HDR.pack(epoch, seq_counter, ts) + pad
        udp.on_send(seq_counter, ts)
        tcp.on_send(seq_counter, ts)
        udp_out.sendto(pkt)
        tcp_out.write(pkt)
        seq_counter += 1
        nxt += 1 / cfg["rate"]
        if nxt < now() - 0.5:
            nxt = now()
        await asyncio.sleep(max(0.0, nxt - now()))


# ---------------------------------------------------------------- dashboard
clients = set()


async def ticker():
    while True:
        await asyncio.sleep(0.5)
        udp.expire()
        tcp.expire()
        payload = json.dumps({"cfg": cfg, "udp": udp.snapshot(0.5), "tcp": tcp.snapshot(0.5)})
        for q in list(clients):
            q.put_nowait(payload)


def apply_config(d):
    for k, (lo, hi) in LIMITS.items():
        if k in d:
            cfg[k] = min(hi, max(lo, float(d[k])))
    cfg["rate"] = int(cfg["rate"])
    if "running" in d:
        cfg["running"] = bool(d["running"])


def reset_all():
    global epoch, seq_counter
    epoch += 1
    seq_counter = 0
    udp.reset()
    tcp.reset()


def respond(writer, status, ctype, body):
    if isinstance(body, str):
        body = body.encode()
    head = (f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n")
    writer.write(head.encode() + body)


async def http(reader, writer):
    q = None
    try:
        line = await reader.readline()
        if not line:
            return
        method, path, _ = line.decode().split(" ", 2)
        headers = {}
        while True:
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            k, _, v = h.decode().partition(":")
            headers[k.strip().lower()] = v.strip()
        n = int(headers.get("content-length", 0))
        body = await reader.readexactly(n) if n else b""

        if method == "GET" and path in ("/", "/index.html"):
            respond(writer, "200 OK", "text/html; charset=utf-8", PAGE)
        elif method == "GET" and path == "/events":
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                         b"Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n")
            q = asyncio.Queue()
            clients.add(q)
            while True:
                writer.write(f"data: {await q.get()}\n\n".encode())
                await writer.drain()
        elif method == "POST" and path == "/config":
            apply_config(json.loads(body or b"{}"))
            respond(writer, "200 OK", "application/json", json.dumps(cfg))
        elif method == "POST" and path == "/reset":
            reset_all()
            respond(writer, "200 OK", "application/json", "{}")
        else:
            respond(writer, "404 Not Found", "text/plain", "not found")
        await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError, ValueError):
        pass
    finally:
        if q is not None:
            clients.discard(q)
        writer.close()


PAGE = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


async def main(port, open_browser):
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(UdpReceiver, local_addr=(HOST, UDP_RECV))
    out, _ = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol, remote_addr=(HOST, UDP_RECV))
    await loop.create_datagram_endpoint(lambda: UdpProxy(out), local_addr=(HOST, UDP_PROXY))
    await asyncio.start_server(tcp_receiver, HOST, TCP_RECV)
    await asyncio.start_server(tcp_proxy, HOST, TCP_PROXY)
    await asyncio.start_server(http, HOST, port)
    asyncio.create_task(sender())
    asyncio.create_task(ticker())
    url = f"http://{HOST}:{port}"
    print(f"TCP vs UDP Live Lab running at {url}  (Ctrl+C to stop)")
    print(f"  UDP: sender -> :{UDP_PROXY} (proxy) -> :{UDP_RECV} (receiver)")
    print(f"  TCP: sender -> :{TCP_PROXY} (proxy) -> :{TCP_RECV} (receiver)")
    if open_browser:
        webbrowser.open(url)
    await asyncio.Event().wait()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="TCP vs UDP Live Lab")
    ap.add_argument("--port", type=int, default=8080, help="dashboard port (default 8080)")
    ap.add_argument("--no-browser", action="store_true", help="do not open a browser tab")
    args = ap.parse_args()
    try:
        asyncio.run(main(args.port, not args.no_browser))
    except KeyboardInterrupt:
        pass
