# TCP vs UDP Live Lab

A tool that sends **the same live data over TCP and over UDP at the same time**, through a network you can make bad, and shows in a browser dashboard how each protocol behaves.

It was built for Assignment No. 4: build something using TCP/UDP in the backend, show how the protocols work in a real application, and use Agentic AI in the development process.

## What this project is

Most explanations of TCP and UDP stay theoretical ("TCP is reliable, UDP is fast"). This project lets you see the difference live.

A sender produces a stream of numbered packets carrying a moving signal (a sine wave). Every packet is sent twice at the same moment, once over a TCP connection and once as a UDP datagram. Both copies pass through a proxy that can add packet loss, delay, jitter and reordering. Two receivers measure what arrives, and the dashboard draws both results side by side.

Because both protocols carry exactly the same data through exactly the same conditions, any difference you see comes from the protocols themselves.

## What I did

- Designed a packet format with a sequence number, a send timestamp and a signal value, so the receiver can detect loss, reordering and delay.
- Wrote the backend in Python with raw sockets: a sender, a UDP receiver and a TCP receiver. UDP uses `sendto` and `recvfrom`. TCP uses `connect`, `accept` and `recv`.
- Wrote a UDP proxy that really drops, delays and reorders datagrams.
- Wrote a TCP proxy that models what TCP does on packet loss: resend the data after a delay and hold everything behind it, because TCP must deliver bytes in order.
- Added receiver-side measurements: packets lost, out-of-order packets, average and 95th percentile delay, jitter, longest wait between packets, and throughput.
- Built a live dashboard with sliders for loss, delay, jitter, reordering and send rate, plus presets such as Home Wi-Fi, Mobile 4G and Congested.
- Wrote automated tests that run the whole system over real localhost sockets and check that TCP and UDP behave as expected.
- Used an AI coding agent (Claude) as a development partner. See `docs/AGENTIC_AI_LOG.md` for how it was used, what went wrong and what I checked myself.

Only the Python standard library is used, so nothing needs to be installed.

## How to run it

```bash
python app.py
```

Then open http://127.0.0.1:8000 in a browser. Use `python3` instead of `python` on Mac or Linux. Press Ctrl+C in the terminal to stop it.

To run the tests:

```bash
python -m unittest discover -v
```

## How TCP and UDP look different in this project



| | UDP | TCP |
|---|---|---|
| Connection | None. Each packet is a separate datagram | A connection is opened first (3-way handshake) |
| Lost packets | Gone for good. The line on the chart gets holes, marked with pink ticks | Never lost to the application. Every packet is delivered |
| Order | Packets can arrive out of order, and the "out of order" counter goes up | Always in order |
| Delay | Stays low and steady, because it never waits for a missing packet | Spikes upward, because a lost packet must be resent and everything behind it waits |
| What the chart shows | A line with gaps that keeps moving forward | A complete line that freezes, then jumps forward to catch up |
| Message boundaries | Built in: one datagram is one message | None. TCP is a byte stream, so the receiver reads the length in each header to rebuild messages |

The key idea this demonstrates is **head-of-line blocking**. UDP drops data but stays current. TCP delivers everything but can stall.



### Which one to use

- **UDP** suits live data where being current matters more than being complete: video calls, online games, live sensor feeds.
- **TCP** suits data that must arrive complete and in order: web pages, file downloads, email.

## What is real and what is simulated

- UDP loss, delay, jitter and reordering are **real**: the proxy actually drops, delays and reorders the datagrams.
- TCP loss is **modelled**. The proxy's own connections are TCP, so the operating system already makes them reliable and the proxy cannot make a real TCP stream lose bytes. Instead, the proxy delays a "lost" message by about one round trip, or by a growing timeout if the resend is also lost, and holds back everything behind it. This reproduces the effect TCP has on timing, but it leaves out congestion control (TCP slowing down after loss).

## Project structure

| File | Purpose |
|---|---|
| `app.py` | Starts the lab and serves the dashboard |
| `lab/protocol.py` | Packet format and the TCP stream parser |
| `lab/endpoints.py` | Sender, UDP receiver and TCP receiver |
| `lab/impair.py` | UDP proxy and TCP proxy (the bad network) |
| `lab/metrics.py` | Loss, delay, jitter, stall and throughput measurements |
| `lab/engine.py` | Starts and stops everything |
| `dashboard/index.html` | The live dashboard |
| `tests/` | Automated tests |
| `docs/AGENTIC_AI_LOG.md` | How Agentic AI was used |
