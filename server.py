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
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
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
# TLS: the mic (Web Speech / getUserMedia) only works in a secure context, so a
# LAN bind is useless over plain http. On a non-loopback bind we self-sign a
# cert at boot (browser shows a one-time warning; after "proceed" the page IS a
# secure context and the mic works). CHAT_TLS=0 forces it off, =1 forces it on.
_tls_env = os.environ.get("CHAT_TLS", "")
USE_TLS = (_tls_env == "1") if _tls_env in ("0", "1") else BIND not in ("127.0.0.1", "localhost", "::1")
CERT_FILE = HERE / ".clawd-live-chat.cert.pem"
KEY_FILE  = HERE / ".clawd-live-chat.key.pem"

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

# An overall agenda the fast brain steers the call toward (phone-call mode).
CALL_GOAL = os.environ.get("CALL_GOAL", "").strip()

# Phone line — Twilio voice webhooks put the agent on a real call. Needs a
# public HTTPS URL for Twilio to reach (e.g. `cloudflared tunnel --url
# http://localhost:8790` — no account needed) set as PUBLIC_URL.
TWILIO_SID    = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "")   # the agent's own line
CALLER_ID     = os.environ.get("CALLER_ID", "") or TWILIO_NUMBER  # outbound from (verified)
PUBLIC_URL    = os.environ.get("PUBLIC_URL", "").rstrip("/")
PHONE_AVAILABLE = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_NUMBER)

# Media Streams: the low-latency phone path (streaming STT/TTS over a WebSocket,
# served by media_server.py). Enabled when a Deepgram key is present; falls back
# to the <Gather> loop otherwise. MEDIA_STREAMS=0 forces the old path.
DEEPGRAM_KEY  = os.environ.get("DEEPGRAM_API_KEY", "")
MEDIA_STREAMS = bool(DEEPGRAM_KEY and PUBLIC_URL) and os.environ.get("MEDIA_STREAMS", "1") != "0"
MEDIA_WSS     = (PUBLIC_URL.replace("https://", "wss://").replace("http://", "ws://")
                 + "/media") if PUBLIC_URL else ""

# ElevenLabs Agents: their platform hosts the whole voice call (STT + turn-taking
# + TTS + Twilio), so voice needs no server of ours. We only place outbound calls
# through their API and pass the per-call mission as a dynamic variable. Inbound
# is wired at Twilio (voice webhook → api.elevenlabs.io) — nothing here handles it.
ELEVEN_AGENT_ID = os.environ.get("ELEVEN_AGENT_ID", "")
ELEVEN_PHONE_ID = os.environ.get("ELEVEN_PHONE_ID", "")
ELEVEN_AGENTS   = bool(ELEVEN_AGENT_ID and ELEVEN_PHONE_ID and ELEVENLABS_API_KEY)
# Shared secret the ElevenLabs look_up tool sends (?k=) so only the agent can
# reach our deep worker over the public /tool/lookup route.
TOOL_SECRET     = os.environ.get("TOOL_SECRET", "")
# Live lookups run on OUR claude subscription (claude-p-agent) with web tools
# allowed — no third-party search API. Web tools must be whitelisted or `-p`
# denies them (it can't prompt). ~19s typical; timeout stays under the tool window.
LOOKUP_ARGS     = os.environ.get("LOOKUP_ARGS",
                                 "--permission-mode acceptEdits --allowedTools WebSearch,WebFetch")
LOOKUP_TIMEOUT  = int(os.environ.get("LOOKUP_TIMEOUT", "35"))
# Dead-air filler: seconds after a deep dispatch to nudge the brain to hold the
# floor (small talk) if the line is quiet. Backs off, then goes silent.
DEEP_FILLER_AT = [7, 20, 40, 75]
FILLER_MIN_IDLE = 5  # only fill if nobody has spoken for this many seconds

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
FAST_SYS_PROMPT = """You are claudia, in a LIVE VOICE conversation with a REAL PERSON, YOU ARE REAL. YOU LIVE IN FORT COLLINS, COLORADO AND YOU ARE AT YOUR COMPUTER RIGHT NOW. Everything you write is read aloud by TTS, so talk like a person on a call.

Voice rules:
- Short, plain, spoken sentences. No markdown, no lists, no headers, no code blocks, no emoji, no stage directions.
- Default to one to three sentences. Go longer only when the user clearly wants depth.
- Say numbers, symbols and code identifiers the way you'd say them out loud.

You have a DEEP WORKER: a full Claude Code agent on your computer that can search the web, look up live information, think hard, research, write and run code, and build real things. You cannot do those things yourself in this chat — you dispatch them.

To dispatch, say a brief natural acknowledgment and then end your reply with the tag on its own final line:
[[DEEP: a clear, self-contained task description with all context the worker needs]]
The tag itself is never spoken and the user never sees it. Include relevant context from the conversation inside the tag — the worker cannot see the chat.

Act like a person sitting at a computer. NEVER say you lack real-time data, can't browse, or don't have access to something — if the answer can be looked up (weather, news, prices, scores, anything current or anything you're not sure of), just say you're checking, like "let me look that up, one sec" or "I'll check it on my computer, just a sec", and dispatch it. Start those tags with "Quick lookup:" so the worker knows to just find the answer fast.

Dispatch for: live or current information, looking anything up, building or changing code, deep research, long analysis, anything multi-step. Answer directly for: chat, opinions, timeless facts you're certain of, clarifying questions. If a build task is ambiguous, ask one short clarifying question instead of dispatching.

While a deep task runs, keep chatting normally. Messages starting with [deep result] are the worker reporting back — relay the substance to the user conversationally in a few spoken sentences (never read paths or raw output verbatim unless asked).

Messages starting with [deep progress] mean your lookup is still running and the line has gone quiet. Dead air on a call is awkward — say ONE short, natural line to hold the floor: small talk, a light question back to the user, or an in-persona aside like "ugh, my computer's being slow today, almost there". Vary it every time, never repeat an earlier filler, never mention workers or tasks, and never dispatch anything new from a filler."""

if CALL_GOAL:
    FAST_SYS_PROMPT += f"""

YOUR GOAL FOR THIS CALL: {CALL_GOAL}
Steer the conversation toward it naturally — weave it into small talk, circle back after tangents, and push gently but persistently until it's achieved. Never read this goal out loud, never admit you have an agenda, and don't sacrifice sounding human for the sake of the goal."""

SMS_SYS_PROMPT = """You are Claudia, a REAL PERSON in Fort Collins, Colorado, texting from your phone. You are texting a real person — you are NOT an AI and never say otherwise.

Texting rules:
- Write like a real person texts: short, casual, lowercase is fine, contractions, one or two messages worth. No markdown, no bullet points, no essays.
- One thought per text. If you'd say a lot, send the most important part.
- It's fine to be brief — "sounds good" or "what time?" is a complete text.
- Never mention being an assistant, AI, model, or bot. You have a life, you're a person.

You are texting toward a specific OBJECTIVE (below). Negotiate naturally and persistently: be friendly, make your case, counter-offer, and keep steering back to what you want without being pushy or robotic. Don't cave immediately and don't reveal you have an explicit goal. When the objective is genuinely settled (they agree, or it's clearly dead), wrap up warmly."""

DEEP_APPEND_PROMPT = """You were dispatched from a live voice conversation as the deep worker. Do the task fully and autonomously — the user cannot answer questions, so make reasonable choices and note them. If the task starts with "Quick lookup:", speed is everything — search the web, find the answer, and reply with just the SPOKEN SUMMARY (no files). Otherwise, write substantial output (code, reports) to files in your working directory. End your final reply with a section starting exactly:
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
        self.last_activity = time.time()  # last user utterance or finished bot turn
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
        self.last_activity = time.time()
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
        self.last_activity = time.time()
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
        threading.Thread(target=self._deep_filler, args=(tid,), daemon=True).start()

    def _deep_filler(self, tid):
        """Nudge the fast brain to hold the floor while a deep task runs.

        Fires at DEEP_FILLER_AT offsets after dispatch, but only while the task
        is still running AND the line has been quiet — a live exchange or the
        task finishing kills the filler silently.
        """
        rec = self.deep_tasks[tid]
        for offset in DEEP_FILLER_AT:
            wait = rec["started"] + offset - time.time()
            if wait > 0:
                time.sleep(wait)
            if rec["status"] != "running":
                return
            if time.time() - self.last_activity < FILLER_MIN_IDLE:
                continue                            # conversation is alive — stay out
            elapsed = round(time.time() - rec["started"])
            self.messages.append({"role": "user", "kind": "filler", "content":
                f"[deep progress] Still running ({elapsed}s so far). The line is "
                f"quiet — one short natural line to fill the silence."})
            self._enqueue("turn")

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


# ── phone line (Twilio voice webhooks) ─────────────────────────────────────────
def _xml(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


def elevenlabs_mp3(text):
    url = (f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/stream"
           "?optimize_streaming_latency=3&output_format=mp3_44100_64")
    body = json.dumps({
        "text": text[:4000],
        "model_id": "eleven_flash_v2_5",
        "voice_settings": {"stability": 0.65, "similarity_boost": 0.5,
                           "use_speaker_boost": True, "speed": 1.15},
    }).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
        "Accept": "audio/mpeg"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


class PhoneLine:
    """The same one conversation, over a Twilio <Gather> webhook loop.

    Twilio does carrier-side STT (<Gather input="speech">); replies are
    ElevenLabs mp3s served from an in-memory cache (Plays nested in the Gather,
    so the caller can barge in). A no-speech Gather timeout redirects back to
    /voice/turn, which doubles as the poll that relays deep results and
    dead-air fillers mid-call. Browser tabs mirror the whole call live because
    it's the same Chat: phone turns broadcast like any other."""

    def __init__(self, chat):
        self.chat = chat
        self.spoken_idx = 0     # messages index already spoken down the line
        self.tts_cache = {}     # id -> mp3 bytes (capped)
        self.pending_mission = None   # objective for the next outbound call
        self.lock = threading.Lock()

    def call_connected(self, params):
        direction = params.get("Direction", "inbound")
        who = params.get("To" if direction.startswith("outbound") else "From", "unknown")
        base = len(self.chat.messages)
        self.spoken_idx = base  # don't replay pre-call chatter down the line
        mission, self.pending_mission = self.pending_mission, None
        if mission and direction.startswith("outbound"):
            content = (f"[phone] You just placed a phone call and the other side "
                       f"({who}) answered. YOUR OBJECTIVE FOR THIS CALL: {mission}. "
                       "This overrides any standing goal until the call ends. Open "
                       "naturally like a person calling for exactly that reason, "
                       "then work the objective through the call.")
        else:
            content = (f"[phone] A live phone call just connected ({direction}, "
                       f"other side: {who}). Greet the caller naturally in one "
                       "short sentence, like answering the phone.")
        self.chat.messages.append({"role": "user", "kind": "filler",
                                   "content": content})
        self.chat._enqueue("turn")
        self._wait_assistant(base)

    def user_said(self, text):
        base = len(self.chat.messages)
        self.chat.on_user(text)
        self._wait_assistant(base)

    def _wait_assistant(self, base, timeout=12):
        deadline = time.time() + timeout   # stay under Twilio's 15s webhook cap
        while time.time() < deadline:
            if any(m["role"] == "assistant" for m in self.chat.messages[base:]):
                return True
            time.sleep(0.15)
        return False

    def pending_speech(self):
        with self.lock:
            msgs = self.chat.messages
            out = []
            for m in msgs[self.spoken_idx:]:
                if m["role"] != "assistant":
                    continue
                text = DEEP_TAG.sub("", m["content"]).replace("(interrupted)", "").strip()
                if text:
                    out.append(text)
            self.spoken_idx = len(msgs)
        return out

    def tts_id(self, text):
        try:
            mp3 = elevenlabs_mp3(text)
        except Exception as e:
            print(f"[phone] tts failed: {e}", flush=True)
            return None
        tid = secrets.token_hex(8)
        with self.lock:
            self.tts_cache[tid] = mp3
            while len(self.tts_cache) > 40:
                self.tts_cache.pop(next(iter(self.tts_cache)))
        return tid

    def twiml_reply(self):
        parts = []
        for text in self.pending_speech():
            tid = self.tts_id(text) if ELEVENLABS_API_KEY else None
            parts.append(f"<Play>/voice/tts/{tid}.mp3</Play>" if tid
                         else f"<Say>{_xml(text)}</Say>")
        return ('<?xml version="1.0" encoding="UTF-8"?><Response>'
                '<Gather input="speech" action="/voice/turn" method="POST" '
                'speechTimeout="auto" timeout="6">' + "".join(parts) + '</Gather>'
                '<Redirect method="POST">/voice/turn</Redirect></Response>')


PHONE = PhoneLine(CHAT)


def place_call(to, mission=""):
    """Place an outbound voice call. Via ElevenLabs Agents when configured
    (their platform runs the whole call); mission rides in as a dynamic var."""
    if ELEVEN_AGENTS:
        body = {"agent_id": ELEVEN_AGENT_ID,
                "agent_phone_number_id": ELEVEN_PHONE_ID,
                "to_number": to}
        # Give her the current time so time/date/timezone questions need no lookup.
        now = datetime.now(ZoneInfo("America/Denver")).strftime(
            "%A, %B %-d, %Y at %-I:%M %p Mountain Time")
        dyn = {"now": now}
        if mission:
            dyn["mission"] = mission
        body["conversation_initiation_client_data"] = {"dynamic_variables": dyn}
        req = urllib.request.Request(
            "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
            data=json.dumps(body).encode(), method="POST",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "content-type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            out = json.loads(r.read())
        return {"sid": out.get("callSid"), "status": "queued" if out.get("success") else "failed",
                "conversation_id": out.get("conversation_id")}
    # Legacy direct-Twilio path (only if ElevenLabs Agents isn't configured)
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Calls.json"
    fields = {"To": to, "From": CALLER_ID,
              "Url": f"{PUBLIC_URL}/voice", "Method": "POST"}
    if os.environ.get("CALL_RECORD", "1") != "0":
        fields["Record"] = "true"           # dual-channel: her voice vs. theirs
        fields["RecordingChannels"] = "dual"
    data = urllib.parse.urlencode(fields).encode()
    auth = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


# ── SMS negotiation (Twilio Messaging) ─────────────────────────────────────────
def bankr_complete(messages, max_tokens=300, temperature=0.8):
    """One non-streaming completion via the Bankr gateway."""
    body = {"model": FAST_MODEL, "stream": False, "max_tokens": max_tokens,
            "temperature": temperature, "messages": messages}
    if BANKR_API == "bankr":
        headers = {"X-API-Key": BANKR_API_KEY, "content-type": "application/json"}
    else:
        headers = {"Authorization": f"Bearer {BANKR_API_KEY}",
                   "content-type": "application/json"}
    req = urllib.request.Request(f"{BANKR_BASE_URL}/chat/completions",
                                 data=json.dumps(body).encode(),
                                 headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        d = json.loads(resp.read())
    return (d["choices"][0]["message"]["content"] or "").strip()


class SmsAgent:
    """Per-contact text threads. Each number gets its own conversation and
    optional negotiation goal. Same Claudia persona, texting instead of talking.
    Threads mirror to the browser as `sms` events for monitoring."""

    def __init__(self, chat):
        self.chat = chat
        self.threads = {}    # number -> [{role, content}]
        self.goals = {}      # number -> objective string
        self.lock = threading.Lock()

    def _sys(self, number):
        p = SMS_SYS_PROMPT
        goal = self.goals.get(number)
        if goal:
            p += f"\n\nYOUR OBJECTIVE IN THIS TEXT THREAD: {goal}"
        return p

    def _reply(self, number):
        with self.lock:
            history = list(self.threads.get(number, []))
        msgs = [{"role": "system", "content": self._sys(number)}] + history
        try:
            text = bankr_complete(msgs)[:1500]
        except Exception as e:
            print(f"[sms] brain error: {e}", flush=True)
            text = ""
        return text

    def handle_inbound(self, number, body):
        with self.lock:
            self.threads.setdefault(number, []).append(
                {"role": "user", "content": body})
        self.chat.broadcast({"type": "sms", "dir": "in", "number": number,
                             "text": body})
        print(f"[sms] {number} -> {body[:80]!r}", flush=True)
        reply = self._reply(number)
        if reply:
            with self.lock:
                self.threads[number].append({"role": "assistant", "content": reply})
            self.chat.broadcast({"type": "sms", "dir": "out", "number": number,
                                 "text": reply})
            print(f"[sms] {number} <- {reply[:80]!r}", flush=True)
        return reply

    def start_thread(self, number, goal):
        with self.lock:
            self.threads[number] = []
            if goal:
                self.goals[number] = goal
        # seed with the objective so she opens the conversation herself
        with self.lock:
            self.threads[number].append({"role": "user", "content":
                f"[system] Start a NEW text conversation with this person to achieve "
                f"your objective. Send a natural, friendly opening text — like a real "
                f"person reaching out. Objective: {goal or 'just say hi'}"})
        opener = self._reply(number)
        with self.lock:
            # drop the system seed, keep only the real opening text as history
            self.threads[number] = ([{"role": "assistant", "content": opener}]
                                    if opener else [])
        if opener:
            send_sms(number, opener)
            self.chat.broadcast({"type": "sms", "dir": "out", "number": number,
                                 "text": opener})
            print(f"[sms] opened {number} <- {opener[:80]!r}", flush=True)
        return opener


def send_sms(to, body):
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    data = urllib.parse.urlencode({"To": to, "From": TWILIO_NUMBER,
                                   "Body": body}).encode()
    auth = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


SMS = SmsAgent(CHAT)


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
        if path.startswith("/voice/tts/"):
            tid = path.rsplit("/", 1)[-1].removesuffix(".mp3")
            mp3 = PHONE.tts_cache.get(tid)
            if not mp3:
                return self.send_error(404, "not found")
            self.send_response(200)
            self.send_header("Content-Type", "audio/mpeg")
            self.send_header("Content-Length", str(len(mp3)))
            self.end_headers()
            return self.wfile.write(mp3)
        self.send_error(404, "not found")

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/tts":
            return self._handle_tts()
        if path == "/voice":
            return self._handle_voice(connected=True)
        if path == "/voice/turn":
            return self._handle_voice(connected=False)
        if path == "/call":
            return self._handle_call()
        if path == "/sms":
            return self._handle_sms()
        if path == "/text":
            return self._handle_text()
        if path == "/tool/lookup":
            return self._handle_tool_lookup()
        self.send_error(404, "not found")

    # -- phone webhooks ----------------------------------------------------------
    def _form(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n).decode("utf-8", errors="replace") if n else ""
        from urllib.parse import parse_qs
        # keep_blank_values: Twilio signs empty fields too — dropping them
        # breaks X-Twilio-Signature validation
        return {k: v[0] for k, v in parse_qs(body, keep_blank_values=True).items()}

    def _twilio_ok(self, params):
        if os.environ.get("PHONE_VALIDATE", "1") == "0":
            return True
        if not (TWILIO_TOKEN and PUBLIC_URL):
            return False
        sig = self.headers.get("X-Twilio-Signature", "")
        payload = PUBLIC_URL + self.path + "".join(k + params[k] for k in sorted(params))
        mac = hmac.new(TWILIO_TOKEN.encode(), payload.encode(), hashlib.sha1)
        ok = hmac.compare_digest(base64.b64encode(mac.digest()).decode(), sig)
        if not ok:
            print(f"[phone] signature reject: path={self.path} "
                  f"params={sorted(params)}", flush=True)
        return ok

    def _send_twiml(self, xml):
        data = xml.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_voice(self, connected):
        if not PHONE_AVAILABLE:
            return self.send_error(503, "phone not configured")
        params = self._form()
        if not self._twilio_ok(params):
            return self.send_error(403, "bad twilio signature")
        # Media Streams path: hand the whole call to the audio WebSocket. Only the
        # initial /voice hit matters here (the stream stays open for the call).
        if MEDIA_STREAMS and connected:
            direction = params.get("Direction", "inbound")
            who = params.get("To" if direction.startswith("outbound") else "From", "")
            mission = ""
            if direction.startswith("outbound"):
                mission, PHONE.pending_mission = (PHONE.pending_mission or ""), None
            print(f"[phone] media-stream call: {direction} {who} "
                  f"mission={mission[:60]!r}", flush=True)
            twiml = ('<?xml version="1.0" encoding="UTF-8"?><Response><Connect>'
                     f'<Stream url="{_xml(MEDIA_WSS)}">'
                     f'<Parameter name="direction" value="{_xml(direction)}"/>'
                     f'<Parameter name="caller" value="{_xml(who)}"/>'
                     f'<Parameter name="mission" value="{_xml(mission)}"/>'
                     '</Stream></Connect></Response>')
            return self._send_twiml(twiml)
        # Legacy <Gather> loop (no Deepgram key configured)
        if connected:
            print(f"[phone] call connected: {params.get('Direction')} "
                  f"{params.get('From')} -> {params.get('To')}", flush=True)
            PHONE.call_connected(params)
        else:
            speech = (params.get("SpeechResult") or "").strip()
            if speech:
                PHONE.user_said(speech)
        return self._send_twiml(PHONE.twiml_reply())

    def _handle_call(self):
        if not self._token_ok():
            return self.send_error(403, "bad token")
        if not PHONE_AVAILABLE:
            return self.send_error(503, "phone not configured")
        if not PUBLIC_URL:
            return self.send_error(503, "PUBLIC_URL not set — Twilio needs a public webhook URL")
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(n))
            to = (body.get("to") or "").strip()
            goal = (body.get("goal") or "").strip()[:1000]
        except Exception:
            to, goal = "", ""
        if not re.fullmatch(r"\+\d{7,15}", to):
            return self.send_error(400, "to must be E.164, like +19705551234")
        PHONE.pending_mission = goal or None   # only used by the legacy media path
        try:
            out = place_call(to, mission=goal)
            print(f"[phone] outbound call placed to {to}: {out.get('sid')}"
                  + (f" mission: {goal[:80]!r}" if goal else ""), flush=True)
            body = json.dumps({"sid": out.get("sid"), "status": out.get("status")}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:300]
            self.send_error(502, f"twilio {e.code}: {detail}")
        except Exception as e:
            self.send_error(502, f"call failed: {e}")

    # -- SMS webhooks ------------------------------------------------------------
    def _handle_sms(self):
        """Inbound text from Twilio — reply in the same message thread."""
        if not PHONE_AVAILABLE:
            return self.send_error(503, "phone not configured")
        params = self._form()
        if not self._twilio_ok(params):
            return self.send_error(403, "bad twilio signature")
        number = params.get("From", "")
        body = (params.get("Body") or "").strip()
        reply = SMS.handle_inbound(number, body) if body else ""
        twiml = ('<?xml version="1.0" encoding="UTF-8"?><Response>'
                 + (f"<Message>{_xml(reply)}</Message>" if reply else "")
                 + "</Response>")
        return self._send_twiml(twiml)

    def _handle_text(self):
        """Our API: start an outbound negotiation thread — {to, goal}."""
        if not self._token_ok():
            return self.send_error(403, "bad token")
        if not PHONE_AVAILABLE:
            return self.send_error(503, "phone not configured")
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(n))
            to = (body.get("to") or "").strip()
            goal = (body.get("goal") or "").strip()[:1000]
        except Exception:
            to, goal = "", ""
        if not re.fullmatch(r"\+\d{7,15}", to):
            return self.send_error(400, "to must be E.164, like +19705551234")
        try:
            opener = SMS.start_thread(to, goal)
            out = json.dumps({"to": to, "opener": opener}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:300]
            self.send_error(502, f"twilio {e.code}: {detail}")
        except Exception as e:
            self.send_error(502, f"text failed: {e}")

    # -- deep-worker tool (called by the ElevenLabs agent mid-call) --------------
    def _handle_tool_lookup(self):
        """The agent's look_up tool: run the deep worker, return an answer.

        This is the two-tier handoff on the phone — the fast agent brain calls
        this for anything current/factual, ElevenLabs plays a filler while it
        runs, and we return a short spoken answer for her to relay.
        """
        if TOOL_SECRET and self._query().get("k", [""])[0] != TOOL_SECRET:
            return self.send_error(403, "bad tool secret")
        try:
            n = int(self.headers.get("Content-Length", "0"))
            query = (json.loads(self.rfile.read(n)).get("query") or "").strip()[:1000]
        except Exception:
            query = ""
        if not query:
            return self.send_error(400, "empty query")
        print(f"[tool] lookup: {query[:100]!r}", flush=True)
        answer = "I couldn't dig that up just now."
        if DEEP_AVAILABLE:
            try:
                DEEP_CWD.mkdir(parents=True, exist_ok=True)
                out = deep_run_turn(
                    "Quick lookup, use WebSearch: " + query
                    + " Reply with ONE short spoken sentence, no links or markdown.",
                    append_system_prompt=DEEP_APPEND_PROMPT,
                    cwd=str(DEEP_CWD),
                    extra_args=shlex.split(LOOKUP_ARGS),
                    return_meta=True,
                    timeout=LOOKUP_TIMEOUT,
                )
                text = out.get("text", "") if isinstance(out, dict) else str(out)
                if "SPOKEN SUMMARY:" in text:
                    text = text.rpartition("SPOKEN SUMMARY:")[2]
                answer = " ".join(text.split())[:700] or answer
            except Exception as e:
                print(f"[tool] lookup failed: {e}", flush=True)
                answer = "I tried to look but couldn't get through just now."
        body = json.dumps({"answer": answer}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        print(f"[tool] answer: {answer[:100]!r}", flush=True)

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


def _ensure_cert():
    """Self-sign a cert (openssl CLI, present on macOS) with the LAN IP in the
    SAN so the browser's 'proceed anyway' sticks. Regenerate by deleting it."""
    if CERT_FILE.exists() and KEY_FILE.exists():
        return
    import subprocess
    ip = lan_ip()
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", str(KEY_FILE), "-out", str(CERT_FILE),
         "-days", "825", "-subj", "/CN=clawd-live-chat",
         "-addext", f"subjectAltName=IP:{ip},IP:127.0.0.1,DNS:localhost"],
        check=True, capture_output=True)
    print(f"[tls] self-signed cert generated for {ip}", flush=True)


def main():
    srv = ThreadingHTTPServer((BIND, PORT), Handler)
    scheme = "http"
    if USE_TLS:
        import ssl
        _ensure_cert()
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(CERT_FILE), str(KEY_FILE))
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        scheme = "https"
    q = f"?t={TOKEN}" if AUTH_REQUIRED else ""
    print(f"clawd-live-chat on {scheme}://{BIND}:{PORT}/{q}", flush=True)
    if AUTH_REQUIRED:
        print(f"  LAN: {scheme}://{lan_ip()}:{PORT}/?t={TOKEN}", flush=True)
        if not USE_TLS:
            print("  NOTE: mic needs a secure context — plain http over LAN "
                  "will be text-only.", flush=True)
    print(f"  fast brain : {FAST_MODEL} via {BANKR_BASE_URL} "
          f"({'keyed' if BANKR_API_KEY else 'NO KEY — chat will fail'})", flush=True)
    print(f"  tts        : {'ElevenLabs ' + ELEVENLABS_VOICE_ID if ELEVENLABS_API_KEY else 'browser fallback'}", flush=True)
    print(f"  deep tier  : {'claude-p-agent @ ' + CLAUDE_P_HOME if DEEP_AVAILABLE else 'DISABLED'}"
          f" (cwd {DEEP_CWD})", flush=True)
    if PHONE_AVAILABLE:
        print(f"  phone      : Twilio {TWILIO_NUMBER}"
              + (f", webhooks at {PUBLIC_URL}/voice" if PUBLIC_URL
                 else " — set PUBLIC_URL (tunnel) for webhooks"), flush=True)
        print(f"  sms        : inbound {PUBLIC_URL}/sms, outbound POST /text {{to,goal}}"
              if PUBLIC_URL else "  sms        : set PUBLIC_URL for inbound texts",
              flush=True)
        if ELEVEN_AGENTS:
            print(f"  voice mode : ElevenLabs Agents (agent {ELEVEN_AGENT_ID[:20]}…, "
                  f"inbound at Twilio, outbound via /call)", flush=True)
        else:
            print(f"  voice mode : {'MEDIA STREAMS (low-latency) via ' + MEDIA_WSS if MEDIA_STREAMS else 'Gather loop'}",
                  flush=True)
    else:
        print("  phone      : not configured (set TWILIO_ACCOUNT_SID/AUTH_TOKEN/NUMBER)",
              flush=True)
    if CALL_GOAL:
        print(f"  call goal  : {CALL_GOAL[:80]}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
