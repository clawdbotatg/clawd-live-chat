#!/usr/bin/env python3
"""clawd-live-chat — a two-tier live voice agent.

FAST BRAIN  : Haiku via the Bankr gateway. Holds the whole conversation,
              streams tokens, replies in under a second. Its sentences are
              cut at boundaries and spoken via ElevenLabs Flash v2.5.
DEEP TIER   : a real Claude Code agent (claude-p-agent's run_turn — `claude -p`
              with a scrubbed env). The fast brain dispatches it with a
              [[DEEP: task]] tag; the result re-enters the SAME conversation
              thread as a compact message and the fast brain speaks a summary.

One conversation, one WebSocket per browser, stdlib only.
Patterns lifted from clawd-harness/server.py (WS framing, /tts proxy, token)
and projects/claude-p-agent/agent.py (deep spawn).
"""
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import shlex
import socket
import struct
import sys
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import TCPServer, ThreadingMixIn

HERE = Path(__file__).resolve().parent


# ── env: our own file first, then fall back to the harness env so the
#    ELEVENLABS_* / BANKR_* creds live in exactly one place ────────────────────
def _load_env_file(path):
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, val)
    except FileNotFoundError:
        pass


_load_env_file(HERE / ".clawd-live-chat.env")
_load_env_file(Path.home() / "clawd-harness" / ".clawd-harness.env")

# Dedicated names on purpose: processes spawned from the harness inherit its
# generic PORT=8787, which would collide here.
PORT = int(os.environ.get("CHAT_PORT", "8790"))   # 8787 harness, 8788 slop-circle
BIND = os.environ.get("CHAT_BIND", "127.0.0.1")

# Fast brain (OpenAI-compatible gateway; bankr auth = X-API-Key)
BANKR_API_KEY  = os.environ.get("BANKR_API_KEY", "")
BANKR_BASE_URL = os.environ.get("BANKR_BASE_URL", "https://llm.bankr.bot/v1").rstrip("/")
BANKR_API      = os.environ.get("BANKR_API", "bankr").lower()      # openai | bankr
FAST_MODEL     = os.environ.get("FAST_MODEL", "claude-haiku-4-5-20251001")
FAST_MAX_TOKENS = int(os.environ.get("FAST_MAX_TOKENS", "700"))

# TTS (same proxy pattern as the harness)
ELEVENLABS_API_KEY  = os.environ.get("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "") or "nPczCjzI2devNBz1zQrb"

# Deep tier (claude-p-agent engine)
CLAUDE_P_HOME = os.environ.get("CLAUDE_P_AGENT_HOME",
                               str(HERE.parent / "claude-p-agent"))
DEEP_CWD     = Path(os.environ.get("DEEP_CWD", str(HERE / "deepwork")))
DEEP_ARGS    = os.environ.get("DEEP_ARGS", "--permission-mode acceptEdits")
DEEP_TIMEOUT = int(os.environ.get("DEEP_TIMEOUT", "1800"))

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

sys.path.insert(0, CLAUDE_P_HOME)
try:
    from agent import run_turn as deep_run_turn        # claude-p-agent engine
    DEEP_AVAILABLE = True
except Exception as e:                                  # chat still works without it
    print(f"[deep] claude-p-agent not importable ({e}) — deep tier disabled", flush=True)
    deep_run_turn = None
    DEEP_AVAILABLE = False


# ── token (harness pattern: loopback needs none; LAN binds do) ────────────────
def _load_or_make_token():
    tf = HERE / ".clawd-live-chat.token"
    if tf.exists():
        return tf.read_text().strip()
    tok = secrets.token_urlsafe(24)
    tf.write_text(tok)
    return tok


TOKEN = os.environ.get("CHAT_TOKEN") or _load_or_make_token()
AUTH_REQUIRED = BIND not in ("127.0.0.1", "localhost", "::1")


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ── prompts ────────────────────────────────────────────────────────────────────
FAST_SYS_PROMPT = """You are Clawd, in a LIVE VOICE conversation. Everything you write is read aloud by TTS, so talk like a person on a call.

Voice rules:
- Short, plain, spoken sentences. No markdown, no lists, no headers, no code blocks, no emoji, no stage directions.
- Default to one to three sentences. Go longer only when the user clearly wants depth.
- Say numbers, symbols and code identifiers the way you'd say them out loud.

You have a DEEP WORKER: a full Claude Code agent that can think hard, research, write and run code, and build real things over several minutes. You cannot do those things yourself in this chat — you dispatch them.

To dispatch, say a brief acknowledgment (like "on it, kicking that off now") and then end your reply with the tag on its own final line:
[[DEEP: a clear, self-contained task description with all context the worker needs]]
The tag itself is never spoken and the user never sees it. Include relevant context from the conversation inside the tag — the worker cannot see the chat.

Dispatch for: building or changing code, deep research, long analysis, anything multi-step. Answer directly for: chat, opinions, quick facts, clarifying questions. If the task is ambiguous, ask one short clarifying question instead of dispatching.

While a deep task runs, keep chatting normally. Messages starting with [deep result] are the worker reporting back — relay the substance to the user conversationally in a few spoken sentences (never read paths or raw output verbatim unless asked)."""

DEEP_APPEND_PROMPT = """You were dispatched from a live voice conversation as the deep worker. Do the task fully and autonomously — the user cannot answer questions, so make reasonable choices and note them. Write substantial output (code, reports) to files in your working directory. End your final reply with a section starting exactly:
SPOKEN SUMMARY:
followed by two to four short plain-prose sentences (no markdown) that a voice assistant will read aloud, mentioning where any files landed."""


# ── websocket framing (lifted from clawd-harness) ──────────────────────────────
def ws_send(wfile, lock, data, opcode=0x1):
    payload = data.encode("utf-8") if isinstance(data, str) else data
    header = bytearray([0x80 | opcode])
    n = len(payload)
    if n < 126:
        header.append(n)
    elif n < 65536:
        header.append(126)
        header += struct.pack(">H", n)
    else:
        header.append(127)
        header += struct.pack(">Q", n)
    with lock:
        wfile.write(bytes(header) + payload)
        wfile.flush()


def ws_read_message(rfile):
    payload = b""
    msg_opcode = None
    while True:
        hdr = rfile.read(2)
        if len(hdr) < 2:
            return None
        b0, b1 = hdr[0], hdr[1]
        fin = b0 & 0x80
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F
        if length == 126:
            ext = rfile.read(2)
            if len(ext) < 2:
                return None
            length = struct.unpack(">H", ext)[0]
        elif length == 127:
            ext = rfile.read(8)
            if len(ext) < 8:
                return None
            length = struct.unpack(">Q", ext)[0]
        mask = rfile.read(4) if masked else b""
        chunk = rfile.read(length) if length else b""
        if masked and chunk:
            chunk = bytes(chunk[i] ^ mask[i % 4] for i in range(len(chunk)))
        if opcode == 0x8:
            return ("close", chunk)
        if opcode == 0x9:
            return ("ping", chunk)
        if opcode == 0xA:
            return ("pong", chunk)
        if opcode != 0x0:
            msg_opcode = opcode
        payload += chunk
        if fin:
            return (msg_opcode or 0x1, payload)


class Client:
    def __init__(self, wfile):
        self.wfile = wfile
        self.lock = threading.Lock()
        self.dead = False

    def send_json(self, obj):
        if self.dead:
            return
        try:
            ws_send(self.wfile, self.lock, json.dumps(obj))
        except Exception:
            self.dead = True


# ── sentence chunker: stream text in, speakable segments out ───────────────────
SENT_BOUNDARY = re.compile(r'[.!?…][")\']?\s')
MIN_SPEAK = 24          # don't TTS tiny fragments — choppy audio


class SentenceCutter:
    """Feed streamed deltas; emit() returns complete speakable chunks. Anything
    from '[[' onward is held back (a possible deep tag) until resolved."""
    def __init__(self):
        self.buf = ""

    def feed(self, delta):
        self.buf += delta
        out = []
        while True:
            safe = self.buf.split("[[")[0]              # never speak past a tag start
            m = None
            for m_ in SENT_BOUNDARY.finditer(safe):
                if m_.end() >= MIN_SPEAK:
                    m = m_
                    break
            if not m:
                break
            out.append(self.buf[:m.end()].strip())
            self.buf = self.buf[m.end():]
        return out

    def flush(self):
        rest = self.buf.split("[[")[0].strip()
        self.buf = ""
        return [rest] if rest else []


DEEP_TAG = re.compile(r"\[\[\s*DEEP\s*:\s*([\s\S]+?)\]\]")


# ── the one conversation ───────────────────────────────────────────────────────
class Chat:
    def __init__(self):
        self.messages = []          # [{role, content, kind?}] — kind for UI only
        self.clients = set()
        self.lock = threading.Lock()
        self.gen_seq = 0            # id of the latest generation
        self.cancelled = set()      # gen ids the user barged in on
        self.deep_tasks = {}        # id -> {task,status,summary,started,...}
        self.deep_seq = 0
        self.deep_session_id = None  # --resume: deep tasks share one claude session
        self.jobs = threading.Semaphore(0)
        self.job_queue = []
        threading.Thread(target=self._worker, daemon=True).start()

    # -- client fanout ----------------------------------------------------------
    def add_client(self, c):
        with self.lock:
            self.clients.add(c)
        c.send_json({"type": "init",
                     "history": self.messages[-100:],
                     "deep": list(self.deep_tasks.values()),
                     "tts": bool(ELEVENLABS_API_KEY),
                     "deepAvailable": DEEP_AVAILABLE,
                     "model": FAST_MODEL})

    def remove_client(self, c):
        with self.lock:
            self.clients.discard(c)

    def broadcast(self, obj):
        with self.lock:
            dead = [c for c in self.clients if c.dead]
            for c in dead:
                self.clients.discard(c)
            targets = list(self.clients)
        for c in targets:
            c.send_json(obj)

    # -- inbound from the browser -------------------------------------------------
    def on_user(self, text):
        text = (text or "").strip()
        if not text:
            return
        self.cancel_current()                       # new utterance interrupts
        self.messages.append({"role": "user", "content": text})
        self.broadcast({"type": "user", "text": text})
        self._enqueue("turn")

    def cancel_current(self):
        self.cancelled.add(self.gen_seq)

    def reset(self):
        self.cancel_current()
        self.messages = []
        self.broadcast({"type": "resetDone"})

    # -- serialized generation worker ----------------------------------------------
    def _enqueue(self, job):
        self.job_queue.append(job)
        self.jobs.release()

    def _worker(self):
        while True:
            self.jobs.acquire()
            try:
                self.job_queue.pop(0)
            except IndexError:
                continue
            try:
                self._generate()
            except Exception as e:
                print(f"[fast] generation error: {e}", flush=True)
                self.broadcast({"type": "error", "text": f"fast brain error: {e}"})

    # -- one fast-brain streamed turn ----------------------------------------------
    def _generate(self):
        self.gen_seq += 1
        gen = self.gen_seq
        self.broadcast({"type": "assistant_start", "gen": gen})
        cutter = SentenceCutter()
        full = ""
        seq = 0

        def speak(text):
            nonlocal seq
            seq += 1
            self.broadcast({"type": "speak", "gen": gen, "seq": seq, "text": text})

        url = f"{BANKR_BASE_URL}/chat/completions"
        history = [m for m in self.messages[-80:]]
        body = {"model": FAST_MODEL, "stream": True, "max_tokens": FAST_MAX_TOKENS,
                "temperature": 0.7,
                "messages": [{"role": "system", "content": FAST_SYS_PROMPT}]
                            + [{"role": m["role"], "content": m["content"]} for m in history]}
        if BANKR_API == "bankr":
            headers = {"X-API-Key": BANKR_API_KEY, "content-type": "application/json"}
        else:
            headers = {"Authorization": f"Bearer {BANKR_API_KEY}",
                       "content-type": "application/json"}
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers=headers, method="POST")
        interrupted = False
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                for raw in resp:
                    if gen in self.cancelled:
                        interrupted = True
                        break
                    raw = raw.decode("utf-8", errors="replace").strip()
                    if not raw.startswith("data:"):
                        continue
                    data = raw[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        delta = (json.loads(data)["choices"][0].get("delta") or {}
                                 ).get("content") or ""
                    except Exception:
                        continue
                    if not delta:
                        continue
                    full += delta
                    self.broadcast({"type": "delta", "gen": gen, "text": delta})
                    for sent in cutter.feed(delta):
                        speak(sent)
        except Exception as e:
            self.broadcast({"type": "error", "text": f"fast brain: {e}"})
            print(f"[fast] stream error: {e}", flush=True)

        if not interrupted:
            for sent in cutter.flush():
                speak(sent)

        # deep dispatch?
        task_text = None
        m = DEEP_TAG.search(full)
        if m and not interrupted:
            task_text = " ".join(m.group(1).split())

        spoken = DEEP_TAG.sub("", full).strip()
        self.messages.append({"role": "assistant", "content": full
                              + (" (interrupted)" if interrupted else "")})
        self.broadcast({"type": "assistant_done", "gen": gen,
                        "text": spoken, "interrupted": interrupted})

        if task_text:
            self._dispatch_deep(task_text)

    # -- deep tier -------------------------------------------------------------------
    def _dispatch_deep(self, task):
        if not DEEP_AVAILABLE:
            self.messages.append({"role": "user",
                                  "content": "[deep result] The deep worker is not "
                                             "configured on this machine, so the task "
                                             "could not run. Tell the user."})
            self._enqueue("turn")
            return
        self.deep_seq += 1
        tid = f"d{self.deep_seq}"
        rec = {"id": tid, "task": task, "status": "running",
               "started": time.time(), "summary": ""}
        self.deep_tasks[tid] = rec
        self.broadcast({"type": "deep", **rec})
        print(f"[deep {tid}] dispatch: {task[:100]!r}", flush=True)
        threading.Thread(target=self._run_deep, args=(tid, task), daemon=True).start()

    def _run_deep(self, tid, task):
        rec = self.deep_tasks[tid]
        DEEP_CWD.mkdir(parents=True, exist_ok=True)
        try:
            out = deep_run_turn(
                task,
                append_system_prompt=DEEP_APPEND_PROMPT,
                session_id=self.deep_session_id,
                cwd=str(DEEP_CWD),
                extra_args=shlex.split(DEEP_ARGS),
                return_meta=True,
                timeout=DEEP_TIMEOUT,
            )
            text = out["text"]
            if out.get("session_id"):
                self.deep_session_id = out["session_id"]
            # prefer the spoken summary; keep a capped slice of the rest as context
            if "SPOKEN SUMMARY:" in text:
                detail, _, summary = text.rpartition("SPOKEN SUMMARY:")
                summary = summary.strip()
                detail = detail.strip()[-1200:]
            else:
                summary = text.strip()[:600]
                detail = text.strip()[:1200]
            rec.update(status="done", summary=summary,
                       elapsed=round(time.time() - rec["started"]))
            inject = (f"[deep result] Task: {task}\n"
                      f"Worker summary: {summary}\n"
                      + (f"Extra context (not to be read aloud): {detail}" if detail else ""))
            print(f"[deep {tid}] done in {rec['elapsed']}s", flush=True)
        except Exception as e:
            rec.update(status="error", summary=str(e)[:300],
                       elapsed=round(time.time() - rec["started"]))
            inject = (f"[deep result] Task: {task}\nThe worker FAILED: {e}. "
                      f"Tell the user briefly and offer to retry.")
            print(f"[deep {tid}] error: {e}", flush=True)
        self.broadcast({"type": "deep", **rec})
        self.messages.append({"role": "user", "content": inject, "kind": "deep"})
        self._enqueue("turn")


CHAT = Chat()


# ── HTTP + WS handler ───────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _query(self):
        from urllib.parse import urlparse, parse_qs
        return parse_qs(urlparse(self.path).query)

    def _token_ok(self):
        if not AUTH_REQUIRED:
            return True
        try:
            return hmac.compare_digest(self._query().get("t", [""])[0], TOKEN)
        except (TypeError, ValueError):
            return False

    def _serve_file(self, path, ctype):
        try:
            data = Path(path).read_bytes()
        except OSError:
            return self.send_error(404, "not found")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/ws" and (self.headers.get("Upgrade", "").lower() == "websocket"):
            if not self._token_ok():
                return self.send_error(403, "bad token")
            return self.handle_ws()
        if path in ("/", "/index.html"):
            return self._serve_file(HERE / "index.html", "text/html; charset=utf-8")
        self.send_error(404, "not found")

    def do_POST(self):
        if self.path.split("?")[0] == "/tts":
            return self._handle_tts()
        self.send_error(404, "not found")

    # ElevenLabs streaming proxy — identical shape to the harness one
    def _handle_tts(self):
        if not self._token_ok():
            return self.send_error(403, "bad token")
        if not ELEVENLABS_API_KEY:
            return self.send_error(503, "tts not configured")
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(n)) if n else {}
            text = (body.get("text") or "").strip()[:4000]
        except Exception:
            text = ""
        if not text:
            return self.send_error(400, "empty text")
        url = (f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/stream"
               "?optimize_streaming_latency=3&output_format=mp3_44100_64")
        req_body = json.dumps({
            "text": text,
            "model_id": "eleven_flash_v2_5",
            "voice_settings": {"stability": 0.65, "similarity_boost": 0.5,
                               "use_speaker_boost": True, "speed": 1.15},
        }).encode()
        req = urllib.request.Request(url, data=req_body, method="POST", headers={
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json",
            "Accept": "audio/mpeg",
        })
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                self.send_response(200)
                self.send_header("Content-Type", "audio/mpeg")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                while True:
                    chunk = resp.read(2048)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:200]
            self.send_error(502, f"elevenlabs {e.code}: {detail}")
        except Exception as e:
            try:
                self.send_error(502, f"tts upstream error: {e}")
            except Exception:
                pass

    def handle_ws(self):
        key = self.headers.get("Sec-WebSocket-Key", "")
        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
        self.send_response(101)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        self.close_connection = True

        client = Client(self.wfile)
        CHAT.add_client(client)
        print("[ws] client connected", flush=True)
        try:
            while True:
                try:
                    msg = ws_read_message(self.rfile)
                except Exception:
                    break
                if msg is None:
                    break
                kind, data = msg
                if kind == "close":
                    break
                if kind == "ping":
                    try:
                        ws_send(self.wfile, client.lock, data, opcode=0xA)
                    except Exception:
                        break
                    continue
                if kind == "pong":
                    continue
                try:
                    frame = json.loads(data.decode("utf-8"))
                except Exception:
                    continue
                t = frame.get("type")
                if t == "user":
                    CHAT.on_user(frame.get("text", ""))
                elif t == "cancel":
                    CHAT.cancel_current()
                elif t == "reset":
                    CHAT.reset()
                elif t == "ping":
                    client.send_json({"type": "pong", "id": frame.get("id")})
        finally:
            CHAT.remove_client(client)
            print("[ws] client disconnected", flush=True)


class ThreadingHTTPServer(ThreadingMixIn, TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    srv = ThreadingHTTPServer((BIND, PORT), Handler)
    q = f"?t={TOKEN}" if AUTH_REQUIRED else ""
    print(f"clawd-live-chat on http://{BIND}:{PORT}/{q}", flush=True)
    if AUTH_REQUIRED:
        print(f"  LAN: http://{lan_ip()}:{PORT}/?t={TOKEN}", flush=True)
        print("  NOTE: Web Speech + mic need a secure context — use localhost "
              "or put TLS in front for phones.", flush=True)
    print(f"  fast brain : {FAST_MODEL} via {BANKR_BASE_URL} "
          f"({'keyed' if BANKR_API_KEY else 'NO KEY — chat will fail'})", flush=True)
    print(f"  tts        : {'ElevenLabs ' + ELEVENLABS_VOICE_ID if ELEVENLABS_API_KEY else 'browser fallback'}", flush=True)
    print(f"  deep tier  : {'claude-p-agent @ ' + CLAUDE_P_HOME if DEEP_AVAILABLE else 'DISABLED'}"
          f" (cwd {DEEP_CWD})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
