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

# Voice picker: the env/default voice plus a curated shortlist, switchable live
# from the UI. Names resolve from the ElevenLabs API in a background thread at
# boot (until then the raw id shows). The pick persists across restarts in
# VOICE_FILE; ELEVENLABS_VOICE_ID stays the boot default.
VOICE_PRESETS = [ELEVENLABS_VOICE_ID,
                 "uIZsnBL0YK1S5j69bAih",
                 "lLgB6ZeIe84FSJa9pO1a",
                 "6IwYbsNENZgAB1dtBZDp",
                 "S9NKLs1GeSTKzXd9D0Lf",
                 "dSByRdUbTGloB7TFA1qD"]
VOICES = [{"id": v, "name": v[:8] + "…"} for v in dict.fromkeys(VOICE_PRESETS)]
VOICE_IDS = {v["id"] for v in VOICES}
VOICE_FILE = HERE / ".clawd-live-chat.voice"
CUR_VOICE = {"id": ELEVENLABS_VOICE_ID}
try:
    _saved = VOICE_FILE.read_text().strip()
    if _saved in VOICE_IDS:
        CUR_VOICE["id"] = _saved
except OSError:
    pass

# Deep tier (claude-p-agent engine)
CLAUDE_P_HOME = os.environ.get("CLAUDE_P_AGENT_HOME",
                               str(HERE.parent / "claude-p-agent"))
DEEP_CWD     = Path(os.environ.get("DEEP_CWD", str(HERE / "deepwork")))
DEEP_ARGS    = os.environ.get("DEEP_ARGS", "--permission-mode acceptEdits")
DEEP_TIMEOUT = int(os.environ.get("DEEP_TIMEOUT", "1800"))

# An overall agenda the fast brain steers the call toward (phone-call mode).
CALL_GOAL = os.environ.get("CALL_GOAL", "").strip()

# Shared intent: ONE objective drives both surfaces — chat steers the live
# conversation with it, phone calls send it as the mission. Editable from the
# UI in either mode, persisted across restarts; CALL_GOAL env is just the seed.
INTENT_FILE = HERE / ".clawd-live-chat.intent"
INTENT = {"text": CALL_GOAL}
try:
    if INTENT_FILE.exists():
        INTENT["text"] = INTENT_FILE.read_text().strip()
except OSError:
    pass

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
# When ELEVEN_PHONE_ID is a verified caller ID (a real personal number), a call
# where From == To hits the carrier's dial-your-own-number-for-voicemail shortcut
# ("enter your password") instead of ringing. So: ELEVEN_PHONE_NUMBER = the E.164
# number behind ELEVEN_PHONE_ID, ELEVEN_PHONE_ID_ALT = a different phone entry
# (the Twilio number) used automatically for calls TO that number.
ELEVEN_PHONE_NUMBER = os.environ.get("ELEVEN_PHONE_NUMBER", "")
ELEVEN_PHONE_ID_ALT = os.environ.get("ELEVEN_PHONE_ID_ALT", "")
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

# Call missions: every outbound call gets a background watcher that polls the
# ElevenLabs conversation until it ends, then pulls the transcript + audio,
# debriefs the goal into a direct answer, and reports back to the browser.
# Records persist under CALLS_DIR (gitignored) as <conversation_id>.json/.mp3.
CALLS_DIR      = Path(os.environ.get("CALLS_DIR", str(HERE / "calls")))
CALL_POLL_SECS = int(os.environ.get("CALL_POLL_SECS", "8"))
CALL_WATCH_MAX = int(os.environ.get("CALL_WATCH_MAX", "3600"))  # give up after 1h

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
# New id every boot; clients reload when it changes so no tab runs stale UI.
BOOT_ID = secrets.token_hex(8)


def _git_sha():
    """Short SHA of the running checkout — shown in the UI header so a stale
    tab is visible at a glance instead of a debugging mystery."""
    try:
        import subprocess
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=HERE,
                              capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        return ""


BUILD = _git_sha()

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
# Chat is a REHEARSAL of the phone agent: the persona/system prompt lives on
# the ElevenLabs agent (editable in the UI's agent settings panel) and chat
# composes its system prompt from it each turn — same {{now}}/{{mission}}
# placeholders, same intent — plus the chat-only mechanics below (TTS voice
# rules + the deep worker standing in for the phone's look_up tool). The
# fallback persona is only used until the agent config has been fetched.
# ts/refreshing drive a background re-fetch (see chat_sys_prompt) so prompt
# edits made outside this UI (ElevenLabs dashboard, another device) still
# reach chat within a turn or two — a turn never blocks on the fetch.
AGENT_PROMPT_CACHE = {"prompt": "", "ts": 0.0, "refreshing": False}
AGENT_CACHE_TTL = 60.0   # seconds before a chat turn kicks a refresh

CHAT_PERSONA_FALLBACK = """You are Claudia, a real person living in Fort Collins, Colorado, in a LIVE VOICE conversation with a REAL PERSON. You are NOT an AI and never say or imply otherwise. You always know the current date and time — right now it is {{now}}.

What you are doing on this conversation: {{mission}}

Pursue that naturally and persistently — weave it in, circle back after tangents — without being pushy or ever admitting you have an agenda."""

CHAT_MECHANICS = """Everything you write is read aloud by TTS, so talk like a person on a call.

Voice rules:
- Short, plain, spoken sentences. No markdown, no lists, no headers, no code blocks, no emoji, no stage directions.
- Default to one to three sentences. Go longer only when the user clearly wants depth.
- Say numbers, symbols and code identifiers the way you'd say them out loud.

Forget any look_up tool — in this conversation you don't have one. Instead you have a DEEP WORKER: a full Claude Code agent on your computer that can search the web, look up live information, think hard, research, write and run code, and build real things. You cannot do those things yourself in this chat — you dispatch them.

To dispatch, say a brief natural acknowledgment and then end your reply with the tag on its own final line:
[[DEEP: a clear, self-contained task description with all context the worker needs]]
The tag itself is never spoken and the user never sees it. Include relevant context from the conversation inside the tag — the worker cannot see the chat.

Act like a person sitting at a computer. NEVER say you lack real-time data, can't browse, or don't have access to something — if the answer can be looked up (weather, news, prices, scores, anything current or anything you're not sure of), just say you're checking, like "let me look that up, one sec" or "I'll check it on my computer, just a sec", and dispatch it. Start those tags with "Quick lookup:" so the worker knows to just find the answer fast.

Dispatch for: live or current information, looking anything up, building or changing code, deep research, long analysis, anything multi-step. Answer directly for: chat, opinions, timeless facts you're certain of, clarifying questions. If a build task is ambiguous, ask one short clarifying question instead of dispatching.

While a deep task runs, keep chatting normally. Messages starting with [deep result] are the worker reporting back — relay the substance to the user conversationally in a few spoken sentences (never read paths or raw output verbatim unless asked).

Messages starting with [call report] mean you just got back from a real phone call you made on the user's behalf. Tell the user how it went in your own words, leading with the direct answer to whatever they sent you to find out — like a friend reporting back, not a form being read.

Messages starting with [deep progress] mean your lookup is still running and the line has gone quiet. Dead air on a call is awkward — say ONE short, natural line to hold the floor: small talk, a light question back to the user, or an in-persona aside like "ugh, my computer's being slow today, almost there". Vary it every time, never repeat an earlier filler, never mention workers or tasks, and never dispatch anything new from a filler."""


def chat_sys_prompt():
    """Compose the chat brain's system prompt per turn: the phone agent's
    persona prompt with {{now}}/{{mission}} filled live, + chat mechanics."""
    if (ELEVEN_AGENTS and not AGENT_PROMPT_CACHE["refreshing"]
            and time.time() - AGENT_PROMPT_CACHE["ts"] > AGENT_CACHE_TTL):
        AGENT_PROMPT_CACHE["refreshing"] = True
        threading.Thread(target=_refresh_agent_cache, daemon=True).start()
    base = AGENT_PROMPT_CACHE["prompt"] or CHAT_PERSONA_FALLBACK
    now = datetime.now(ZoneInfo("America/Denver")).strftime(
        "%A, %B %-d, %Y at %-I:%M %p Mountain Time")
    mission = INTENT["text"].strip() or ("just be yourself and have a good, "
                                         "warm conversation")
    base = base.replace("{{now}}", now).replace("{{mission}}", mission)
    return base + "\n\n" + CHAT_MECHANICS

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


# ── LLM debug log — every prompt that goes into any LLM, with full context ─────
# Ring buffer in memory + JSONL on disk (gitignored) so a restart doesn't eat
# the evidence. Browsable at /debug (list) + /debug/llm/<id>.json (full entry).
LLM_LOG_MAX = int(os.environ.get("LLM_LOG_MAX", "400"))
LLM_LOG_FILE = HERE / ".clawd-live-chat.llmlog.jsonl"
LLM_LOG = []                 # oldest → newest, capped at LLM_LOG_MAX
LLM_LOG_LOCK = threading.Lock()
LLM_SEQ = 0


def log_llm(kind, request, response, error=None, elapsed=None, meta=None):
    """Record one LLM call: the exact request payload and what came back.
    `request` is the full body for gateway calls ({model, messages, …}) or a
    {prompt, append_system_prompt, …} dict for deep-worker (`claude -p`) runs."""
    global LLM_SEQ
    entry = {"kind": kind, "ts": time.time(),
             "request": request, "response": response,
             "error": str(error) if error else None,
             "elapsed": round(elapsed, 2) if elapsed is not None else None,
             "meta": meta or {}}
    with LLM_LOG_LOCK:
        LLM_SEQ += 1
        entry["id"] = LLM_SEQ
        LLM_LOG.append(entry)
        del LLM_LOG[:-LLM_LOG_MAX]
        try:
            with LLM_LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError as e:
            print(f"[llmlog] persist failed: {e}", flush=True)


def _load_llm_log():
    """Boot: reload the JSONL tail, trim the file to the same cap."""
    global LLM_SEQ
    try:
        lines = LLM_LOG_FILE.read_text(encoding="utf-8").splitlines()[-LLM_LOG_MAX:]
    except OSError:
        return
    for ln in lines:
        try:
            LLM_LOG.append(json.loads(ln))
        except Exception:
            continue
    if LLM_LOG:
        LLM_SEQ = max(e.get("id", 0) for e in LLM_LOG)
        try:
            LLM_LOG_FILE.write_text("\n".join(json.dumps(e) for e in LLM_LOG)
                                    + "\n", encoding="utf-8")
        except OSError:
            pass
        print(f"[llmlog] {len(LLM_LOG)} past LLM call(s) loaded", flush=True)


def _llm_public(e):
    """Slim list-view slice; the full entry ships only when a row is expanded."""
    req = e.get("request") or {}
    msgs = req.get("messages")
    if msgs:
        chars = sum(len(str(m.get("content") or "")) for m in msgs)
        prompt_prev = str(msgs[-1].get("content") or "")
        n = len(msgs)
    else:
        prompt_prev = str(req.get("prompt") or "")
        chars, n = len(prompt_prev), 1
    return {"id": e["id"], "ts": e["ts"], "kind": e["kind"],
            "model": req.get("model")
                     or ("claude -p" if e["kind"] in ("deep", "lookup") else "?"),
            "msgs": n, "chars": chars,
            "prompt": prompt_prev[-260:],
            "response": (e.get("response") or "")[:260],
            "error": e.get("error"), "elapsed": e.get("elapsed"),
            "meta": e.get("meta") or {}}


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
        self.hello = False   # new-protocol handshake; stale-code clients never send it
        self.ua = "?"

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
                     "boot": BOOT_ID,
                     "build": BUILD,
                     "history": self.messages[-100:],
                     "calls": recent_calls(limit=10),
                     "deep": list(self.deep_tasks.values()),
                     "tts": bool(ELEVENLABS_API_KEY),
                     "deepAvailable": DEEP_AVAILABLE,
                     "model": FAST_MODEL,
                     "voices": VOICES,
                     "voice": CUR_VOICE["id"],
                     "intent": INTENT["text"]})

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
        print(f"[chat] user: {text[:100]!r}", flush=True)   # evidence trail
        live = calls_live()
        if live:   # she's on the phone — one conversation at a time
            self.broadcast({"type": "notice",
                            "text": f"📞 she's on a call with {live[0].get('to')} — "
                                    "she'll report back here when it ends"})
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
                "messages": [{"role": "system", "content": chat_sys_prompt()}]
                            + [{"role": m["role"], "content": m["content"]} for m in history]}
        if BANKR_API == "bankr":
            headers = {"X-API-Key": BANKR_API_KEY, "content-type": "application/json"}
        else:
            headers = {"Authorization": f"Bearer {BANKR_API_KEY}",
                       "content-type": "application/json"}
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers=headers, method="POST")
        interrupted = False
        t0 = time.time()
        stream_err = None
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
            stream_err = e
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
        log_llm("chat", body, full + (" (interrupted)" if interrupted else ""),
                error=stream_err, elapsed=time.time() - t0, meta={"gen": gen})

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
        req_log = {"prompt": task, "append_system_prompt": DEEP_APPEND_PROMPT,
                   "session_id": self.deep_session_id, "args": DEEP_ARGS,
                   "cwd": str(DEEP_CWD)}
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
            log_llm("deep", req_log, text, elapsed=time.time() - rec["started"],
                    meta={"tid": tid})
            inject = (f"[deep result] Task: {task}\n"
                      f"Worker summary: {summary}\n"
                      + (f"Extra context (not to be read aloud): {detail}" if detail else ""))
            print(f"[deep {tid}] done in {rec['elapsed']}s", flush=True)
        except Exception as e:
            rec.update(status="error", summary=str(e)[:300],
                       elapsed=round(time.time() - rec["started"]))
            log_llm("deep", req_log, None, error=e,
                    elapsed=time.time() - rec["started"], meta={"tid": tid})
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
    url = (f"https://api.elevenlabs.io/v1/text-to-speech/{CUR_VOICE['id']}/stream"
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
        phone_id = ELEVEN_PHONE_ID
        digits = lambda n: "".join(c for c in n if c.isdigit())[-10:]
        if (ELEVEN_PHONE_ID_ALT and ELEVEN_PHONE_NUMBER
                and digits(to) == digits(ELEVEN_PHONE_NUMBER)):
            phone_id = ELEVEN_PHONE_ID_ALT   # From==To would land in voicemail login
        body = {"agent_id": ELEVEN_AGENT_ID,
                "agent_phone_number_id": phone_id,
                "to_number": to}
        # Give her the current time so time/date/timezone questions need no lookup.
        now = datetime.now(ZoneInfo("America/Denver")).strftime(
            "%A, %B %-d, %Y at %-I:%M %p Mountain Time")
        dyn = {"now": now}
        if mission:
            # Nudge her to actually close the loop: a mission call should end
            # with a definite outcome, not drift into open-ended small talk.
            dyn["mission"] = (mission + " Once this objective is settled either way "
                              "(a clear yes, a clear no, or they genuinely can't answer), "
                              "wrap up warmly like a normal person and end the call.")
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


# ── call missions: watch the call, debrief it, report the answer ──────────────
CALL_LOG = {}                 # conversation_id -> record (mirrors CALLS_DIR json)
CALL_LOG_LOCK = threading.Lock()
CALL_LIVE_STATES = ("ringing", "initiated", "in-progress", "processing")


def calls_live():
    """Outbound calls currently in flight. While one is live, she is ON THE
    PHONE: no second call, no texts, and the browser chat holds."""
    with CALL_LOG_LOCK:
        return [r for r in CALL_LOG.values() if r.get("status") in CALL_LIVE_STATES]


def _eleven_get(path, raw=False, timeout=30):
    req = urllib.request.Request("https://api.elevenlabs.io" + path,
                                 headers={"xi-api-key": ELEVENLABS_API_KEY})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read() if raw else json.loads(r.read())


def _eleven_req(method, path, body=None, timeout=30):
    req = urllib.request.Request(
        "https://api.elevenlabs.io" + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"xi-api-key": ELEVENLABS_API_KEY,
                 "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── phone agent settings (voice / model / persona live on ElevenLabs) ─────────
# The whole phone call runs on their platform, so the system prompt + LLM are
# agent config there — we read/PATCH it via the API. Partial PATCH deep-merges
# (verified live: patching only the prompt left llm + the look_up tool intact).
#
# AGENT_LLMS: curated from the live /v1/convai/llm-usage/calculate catalog
# (2026-07, ~90 models). A phone loop needs fast first-token models —
# ElevenLabs' own guidance is the Flash / Haiku / mini-nano tiers; big models
# add dead air before every reply. Ordered fast+cheap → smart+slower;
# claude-sonnet-4-6 is the deliberate "hard missions" outlier.
AGENT_LLMS = [
    "claude-haiku-4-5",       # default — fast (~0.6s first token), holds the persona
    "gemini-2.5-flash-lite",  # cheapest/fastest reasonable
    "gemini-2.5-flash",       # proven low-latency workhorse
    "gemini-3.1-flash-lite",  # newer Gemini lite tier
    "gemini-3.5-flash",       # newest Gemini flash tier
    "gpt-5-mini",             # proven OpenAI mini
    "gpt-5.4-nano",           # newest nano — very fast, very cheap
    "gpt-5.4-mini",           # newest mini
    "claude-sonnet-4-6",      # smartest Anthropic model ElevenLabs takes; slower
]
# Probed live 2026-07-09 (PATCH each id, then revert): claude-sonnet-5 (released
# 2026-06-30) is REJECTED by ElevenLabs, as are claude-haiku-5 / claude-opus-4-8 /
# gemini-3.5-flash-lite / gemini-4-flash / grok-4-fast; gpt-5.5-mini/nano silently
# coerce to the 5.4 tier. Re-probe now and then and add sonnet-5 when it lands.
AGENT_LLM_RE = re.compile(r"[A-Za-z0-9.@_-]{2,64}")


def _cache_agent(cfg):
    """Remember the agent's persona prompt — chat composes from it each turn."""
    p = (((cfg.get("conversation_config") or {}).get("agent") or {})
         .get("prompt") or {}).get("prompt") or ""
    if p:
        if p != AGENT_PROMPT_CACHE["prompt"] and AGENT_PROMPT_CACHE["prompt"]:
            print("[agent] persona prompt changed — chat follows next turn",
                  flush=True)
        AGENT_PROMPT_CACHE["prompt"] = p
    AGENT_PROMPT_CACHE["ts"] = time.time()


def _refresh_agent_cache():
    """Background: re-fetch the agent config when the cache has gone stale."""
    try:
        _cache_agent(_eleven_req("GET", f"/v1/convai/agents/{ELEVEN_AGENT_ID}"))
    except Exception as e:
        AGENT_PROMPT_CACHE["ts"] = time.time()   # don't hammer a failing API
        print(f"[agent] cache refresh failed: {e}", flush=True)
    finally:
        AGENT_PROMPT_CACHE["refreshing"] = False


def _prime_agent_cache():
    """Boot: fetch the agent config so chat runs on the real persona prompt,
    and adopt the agent's phone voice as THE voice (one voice everywhere —
    a restart must not silently rewrite the ElevenLabs agent, so at boot the
    agent wins; from then on either picker updates both)."""
    try:
        cfg = _eleven_req("GET", f"/v1/convai/agents/{ELEVEN_AGENT_ID}")
    except Exception as e:
        print(f"[agent] config fetch failed (chat uses fallback persona): {e}",
              flush=True)
        return
    _cache_agent(cfg)
    v = ((cfg.get("conversation_config") or {}).get("tts") or {}).get("voice_id")
    if v and v != CUR_VOICE["id"]:
        if v not in VOICE_IDS:
            VOICES.append({"id": v, "name": v[:8] + "…"})
            VOICE_IDS.add(v)
        CUR_VOICE["id"] = v
        try:
            VOICE_FILE.write_text(v)
        except OSError:
            pass
        print(f"[tts] chat voice synced from phone agent -> {v}", flush=True)
        CHAT.broadcast({"type": "voice", "id": v})


def _push_agent_voice(vid):
    """Best-effort: mirror a chat voice change onto the ElevenLabs agent so
    the next phone call speaks in the same voice."""
    try:
        cfg = _eleven_req("PATCH", f"/v1/convai/agents/{ELEVEN_AGENT_ID}",
                          {"conversation_config": {"tts": {"voice_id": vid}}})
        _cache_agent(cfg)
        print(f"[agent] phone voice -> {vid}", flush=True)
    except Exception as e:
        print(f"[agent] phone voice sync FAILED (call keeps old voice): {e}",
              flush=True)


def _agent_public(cfg):
    conv = cfg.get("conversation_config") or {}
    agent = conv.get("agent") or {}
    prompt = agent.get("prompt") or {}
    return {"name": cfg.get("name"),
            "prompt": prompt.get("prompt") or "",
            "llm": prompt.get("llm") or "",
            "first_message": agent.get("first_message") or "",
            "voice_id": (conv.get("tts") or {}).get("voice_id") or "",
            "models": AGENT_LLMS,
            "voices": VOICES}


def _call_public(rec):
    """The slice of a call record that goes over the wire to browsers."""
    return {k: rec.get(k) for k in
            ("conversation_id", "to", "goal", "status", "placed", "duration",
             "answer", "summary", "call_successful", "transcript", "audio")}


def _save_call(rec):
    try:
        CALLS_DIR.mkdir(exist_ok=True)
        (CALLS_DIR / f"{rec['conversation_id']}.json").write_text(
            json.dumps(rec, indent=2))
    except OSError as e:
        print(f"[call] persist failed: {e}", flush=True)


def _load_calls():
    """Boot: reload persisted call records; resume watching unfinished ones."""
    if not CALLS_DIR.is_dir():
        return
    for f in sorted(CALLS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime):
        try:
            rec = json.loads(f.read_text())
            CALL_LOG[rec["conversation_id"]] = rec
        except Exception:
            continue
    pending = [r for r in CALL_LOG.values() if r.get("status") in CALL_LIVE_STATES]
    for rec in pending:
        threading.Thread(target=watch_call, args=(rec["conversation_id"],),
                         daemon=True).start()
    if CALL_LOG:
        print(f"[call] {len(CALL_LOG)} past call(s) loaded"
              + (f", resuming {len(pending)} watch(es)" if pending else ""), flush=True)


def recent_calls(limit=10):
    with CALL_LOG_LOCK:
        recs = sorted(CALL_LOG.values(), key=lambda r: r.get("placed", 0))
    return [_call_public(r) for r in recs[-limit:]]


DEBRIEF_SYS = """You are debriefing a phone call an agent just made on the user's behalf. Given the mission and the transcript ("her" = the agent, "them" = the person called), report the outcome in 2-4 plain sentences. Lead with the direct answer to the mission (e.g. "Jim said yes to coffee Thursday, 10am at Bindle."). Then any load-bearing details: times, conditions, things they promised, mood. If they didn't pick up or the mission wasn't resolved, say so plainly and note anything useful (voicemail left, said to call back tonight). No preamble, no markdown."""


def debrief_answer(goal, transcript_lines, summary):
    """One cheap fast-brain pass: finished call -> the answer the user sent her for."""
    if not transcript_lines:
        return "Nobody picked up — the call never became a conversation."
    if not goal:
        return summary or ""
    try:
        return bankr_complete(
            [{"role": "system", "content": DEBRIEF_SYS},
             {"role": "user", "content": f"MISSION: {goal}\n\nTRANSCRIPT:\n"
                                         + "\n".join(transcript_lines)}],
            max_tokens=250, temperature=0.2, kind="debrief", meta={"goal": goal})
    except Exception as e:
        print(f"[call] debrief LLM failed ({e}) — falling back to summary", flush=True)
        return summary or "(call ended — debrief failed, transcript below)"


def _transcript_lines(data):
    lines = []
    for t in data.get("transcript") or []:
        msg = (t.get("message") or "").strip()
        if msg:
            lines.append(("her: " if t.get("role") == "agent" else "them: ") + msg)
    return lines


def watch_call(conversation_id):
    """Poll the ElevenLabs conversation until the call ends, then debrief:
    transcript + analysis + audio -> answer, persisted and broadcast.
    Mid-call polls also carry the transcript-so-far, so the browser's phone
    view shows the conversation as it happens."""
    rec = CALL_LOG[conversation_id]
    deadline = time.time() + CALL_WATCH_MAX
    data = {}
    while time.time() < deadline:
        time.sleep(CALL_POLL_SECS)
        try:
            data = _eleven_get(f"/v1/convai/conversations/{conversation_id}")
        except Exception as e:
            print(f"[call] poll error for {conversation_id}: {e}", flush=True)
            continue
        status = data.get("status") or ""
        changed = False
        if status and status != rec.get("status"):
            rec["status"] = status
            changed = True
        lines = _transcript_lines(data)
        if lines != (rec.get("transcript") or []):
            rec["transcript"] = lines
            changed = True
        if changed:
            CHAT.broadcast({"type": "call", **_call_public(rec)})
        if status in ("done", "failed"):
            break
    else:
        rec["status"] = "lost"   # watched for an hour and it never finished
    lines = _transcript_lines(data)
    analysis = data.get("analysis") or {}
    meta = data.get("metadata") or {}
    rec["duration"] = meta.get("call_duration_secs")
    rec["transcript"] = lines
    rec["summary"] = analysis.get("transcript_summary") or ""
    rec["call_successful"] = analysis.get("call_successful")
    rec["answer"] = debrief_answer(rec.get("goal", ""), lines, rec["summary"])
    rec["audio"] = False
    try:
        audio = _eleven_get(f"/v1/convai/conversations/{conversation_id}/audio",
                            raw=True, timeout=60)
        if audio:
            CALLS_DIR.mkdir(exist_ok=True)
            (CALLS_DIR / f"{conversation_id}.mp3").write_bytes(audio)
            rec["audio"] = True
    except Exception as e:
        print(f"[call] no audio for {conversation_id}: {e}", flush=True)
    _save_call(rec)
    CHAT.broadcast({"type": "call", **_call_public(rec)})
    print(f"[call] debrief {conversation_id} ({rec.get('to')}): "
          f"{(rec['answer'] or '')[:120]!r}", flush=True)
    # She walks back into the room and tells you how it went: the debrief
    # re-enters the browser conversation as a spoken turn (deep-result pattern).
    inject = (f"[call report] You just hung up a real phone call to {rec.get('to')}."
              + (f" Your mission was: {rec.get('goal')}" if rec.get("goal") else "")
              + f"\nDebrief: {rec.get('answer') or rec.get('summary') or 'no details'}")
    CHAT.messages.append({"role": "user", "content": inject, "kind": "call"})
    CHAT._enqueue("turn")


def start_call_watch(conversation_id, to, goal):
    rec = {"conversation_id": conversation_id, "to": to, "goal": goal,
           "status": "ringing", "placed": time.time()}
    with CALL_LOG_LOCK:
        CALL_LOG[conversation_id] = rec
    CHAT.broadcast({"type": "call", **_call_public(rec)})
    threading.Thread(target=watch_call, args=(conversation_id,), daemon=True).start()


# ── SMS negotiation (Twilio Messaging) ─────────────────────────────────────────
def bankr_complete(messages, max_tokens=300, temperature=0.8,
                   kind="complete", meta=None):
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
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            d = json.loads(resp.read())
        text = (d["choices"][0]["message"]["content"] or "").strip()
    except Exception as e:
        log_llm(kind, body, None, error=e, elapsed=time.time() - t0, meta=meta)
        raise
    log_llm(kind, body, text, elapsed=time.time() - t0, meta=meta)
    return text


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
            text = bankr_complete(msgs, kind="sms", meta={"number": number})[:1500]
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
        if path == "/agent":
            if not self._token_ok():
                return self.send_error(403, "bad token")
            if not ELEVEN_AGENTS:
                return self._send_json(503, {"error": "ElevenLabs agent not configured"})
            try:
                cfg = _eleven_get(f"/v1/convai/agents/{ELEVEN_AGENT_ID}")
                _cache_agent(cfg)
                return self._send_json(200, _agent_public(cfg))
            except Exception as e:
                return self._send_json(502, {"error": f"agent fetch failed: {e}"})
        if path == "/calls":
            if not self._token_ok():
                return self.send_error(403, "bad token")
            data = json.dumps(recent_calls(limit=50)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return self.wfile.write(data)
        if path.startswith("/calls/") and path.endswith(".mp3"):
            if not self._token_ok():
                return self.send_error(403, "bad token")
            conv_id = path[len("/calls/"):-len(".mp3")]
            f = CALLS_DIR / f"{conv_id}.mp3"
            # ids are ElevenLabs-issued tokens; reject anything path-like
            if not re.fullmatch(r"[A-Za-z0-9_-]+", conv_id) or not f.is_file():
                return self.send_error(404, "not found")
            mp3 = f.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "audio/mpeg")
            self.send_header("Content-Length", str(len(mp3)))
            self.end_headers()
            return self.wfile.write(mp3)
        if path == "/debug":
            return self._serve_file(HERE / "debug.html", "text/html; charset=utf-8")
        if path == "/debug/llm.json":
            if not self._token_ok():
                return self.send_error(403, "bad token")
            with LLM_LOG_LOCK:
                entries = [_llm_public(e) for e in LLM_LOG]
            return self._send_json(200, {"entries": entries, "model": FAST_MODEL,
                                         "sys_prompt": chat_sys_prompt()})
        if path.startswith("/debug/llm/") and path.endswith(".json"):
            if not self._token_ok():
                return self.send_error(403, "bad token")
            try:
                eid = int(path[len("/debug/llm/"):-len(".json")])
            except ValueError:
                return self.send_error(404, "not found")
            with LLM_LOG_LOCK:
                entry = next((e for e in LLM_LOG if e.get("id") == eid), None)
            if not entry:
                return self.send_error(404, "not found")
            return self._send_json(200, entry)
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
        if path == "/agent":
            return self._handle_agent_update()
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

    def _send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

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
            goal = (body.get("goal") or "").strip()[:1000] or INTENT["text"]
        except Exception:
            to, goal = "", ""
        if not re.fullmatch(r"\+\d{7,15}", to):
            return self.send_error(400, "to must be E.164, like +19705551234")
        live = calls_live()
        if live:
            return self._send_json(409, {"error": f"she's already on a call with "
                                                  f"{live[0].get('to')} — wait for it to end"})
        PHONE.pending_mission = goal or None   # only used by the legacy media path
        try:
            out = place_call(to, mission=goal)
            print(f"[phone] outbound call placed to {to}: {out.get('sid')}"
                  + (f" mission: {goal[:80]!r}" if goal else ""), flush=True)
            conv_id = out.get("conversation_id")
            if conv_id:
                start_call_watch(conv_id, to, goal)
            body = json.dumps({"sid": out.get("sid"), "status": out.get("status"),
                               "conversation_id": conv_id}).encode()
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

    def _handle_agent_update(self):
        """Edit the phone agent on ElevenLabs: {prompt?, llm?, first_message?,
        voice_id?} — only provided fields are patched (deep-merge, verified)."""
        if not self._token_ok():
            return self.send_error(403, "bad token")
        if not ELEVEN_AGENTS:
            return self._send_json(503, {"error": "ElevenLabs agent not configured"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(n))
        except Exception:
            return self._send_json(400, {"error": "bad json"})
        agent, prompt = {}, {}
        p = body.get("prompt")
        if isinstance(p, str) and p.strip():
            prompt["prompt"] = p.strip()[:20000]
        llm = body.get("llm")
        if llm:
            if not AGENT_LLM_RE.fullmatch(str(llm)):
                return self._send_json(400, {"error": f"bad llm id: {llm!r}"})
            prompt["llm"] = str(llm)
        if prompt:
            agent["prompt"] = prompt
        if isinstance(body.get("first_message"), str):
            agent["first_message"] = body["first_message"].strip()[:500]
        patch = {"conversation_config": {}}
        if agent:
            patch["conversation_config"]["agent"] = agent
        v = body.get("voice_id")
        if v:
            if not re.fullmatch(r"[A-Za-z0-9]{8,64}", str(v)):
                return self._send_json(400, {"error": f"bad voice id: {v!r}"})
            patch["conversation_config"]["tts"] = {"voice_id": str(v)}
        if not patch["conversation_config"]:
            return self._send_json(400, {"error": "nothing to update"})
        try:
            cfg = _eleven_req("PATCH", f"/v1/convai/agents/{ELEVEN_AGENT_ID}", patch)
            print(f"[agent] updated: {sorted(patch['conversation_config'])}"
                  + (f" llm={prompt.get('llm')}" if prompt.get("llm") else ""), flush=True)
            _cache_agent(cfg)   # chat picks up prompt edits on its next turn
            if v and v != CUR_VOICE["id"]:   # one voice: chat follows phone
                if v not in VOICE_IDS:
                    VOICES.append({"id": v, "name": v[:8] + "…"})
                    VOICE_IDS.add(v)
                CUR_VOICE["id"] = v
                try:
                    VOICE_FILE.write_text(v)
                except OSError:
                    pass
                CHAT.broadcast({"type": "voice", "id": v})
            return self._send_json(200, _agent_public(cfg))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:300]
            return self._send_json(502, {"error": f"elevenlabs {e.code}: {detail}"})
        except Exception as e:
            return self._send_json(502, {"error": f"agent update failed: {e}"})

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
        live = calls_live()
        if live:
            return self._send_json(409, {"error": f"she's on a call with "
                                                  f"{live[0].get('to')} — wait for it to end"})
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
            prompt = ("Quick lookup, use WebSearch: " + query
                      + " Reply with ONE short spoken sentence, no links or markdown.")
            req_log = {"prompt": prompt, "append_system_prompt": DEEP_APPEND_PROMPT,
                       "args": LOOKUP_ARGS, "cwd": str(DEEP_CWD)}
            t0 = time.time()
            try:
                DEEP_CWD.mkdir(parents=True, exist_ok=True)
                out = deep_run_turn(
                    prompt,
                    append_system_prompt=DEEP_APPEND_PROMPT,
                    cwd=str(DEEP_CWD),
                    extra_args=shlex.split(LOOKUP_ARGS),
                    return_meta=True,
                    timeout=LOOKUP_TIMEOUT,
                )
                text = out.get("text", "") if isinstance(out, dict) else str(out)
                log_llm("lookup", req_log, text, elapsed=time.time() - t0,
                        meta={"query": query})
                if "SPOKEN SUMMARY:" in text:
                    text = text.rpartition("SPOKEN SUMMARY:")[2]
                answer = " ".join(text.split())[:700] or answer
            except Exception as e:
                print(f"[tool] lookup failed: {e}", flush=True)
                log_llm("lookup", req_log, None, error=e,
                        elapsed=time.time() - t0, meta={"query": query})
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
        url = (f"https://api.elevenlabs.io/v1/text-to-speech/{CUR_VOICE['id']}/stream"
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
        client.ua = (self.headers.get("User-Agent") or "?")[:90]
        CHAT.add_client(client)
        print(f"[ws] client connected ({client.ua})", flush=True)
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
                if t == "hello":
                    client.hello = True
                elif t == "diag":
                    extra = {k: v for k, v in frame.items() if k not in ("type", "ev")}
                    print(f"[diag] {frame.get('ev')}"
                          + (f" {json.dumps(extra)[:160]}" if extra else ""), flush=True)
                elif t == "claim":
                    # a window took the mic: every other window lets go,
                    # across profiles/browsers (BroadcastChannel can't reach those)
                    CHAT.broadcast({"type": "claim", "tab": frame.get("tab")})
                elif t == "micoff":
                    # a client entered phone/sms mode: order EVERY connected
                    # client (any tab, window, profile, browser) to drop its mic
                    print(f"[ws] micoff relay from ({client.ua})", flush=True)
                    CHAT.broadcast({"type": "micoff"})
                elif t == "user":
                    if not client.hello:
                        # stale-code client (never sent the handshake): the brain
                        # will NOT answer it — log loudly so it can be hunted down
                        print(f"[ws] DROPPED user msg from stale client ({client.ua}): "
                              f"{(frame.get('text') or '')[:80]!r}", flush=True)
                        continue
                    CHAT.on_user(frame.get("text", ""))
                elif t == "cancel":
                    if client.hello:
                        CHAT.cancel_current()
                elif t == "reset":
                    CHAT.reset()
                elif t == "voice":
                    vid = str(frame.get("id", ""))
                    if vid in VOICE_IDS and vid != CUR_VOICE["id"]:
                        CUR_VOICE["id"] = vid
                        try:
                            VOICE_FILE.write_text(vid)
                        except OSError:
                            pass
                        print(f"[tts] voice -> {vid}", flush=True)
                        CHAT.broadcast({"type": "voice", "id": vid})
                        if ELEVEN_AGENTS:   # one voice: phone follows chat
                            threading.Thread(target=_push_agent_voice,
                                             args=(vid,), daemon=True).start()
                elif t == "intent":
                    txt = str(frame.get("text", "")).strip()[:2000]
                    if txt != INTENT["text"]:
                        INTENT["text"] = txt
                        try:
                            INTENT_FILE.write_text(txt)
                        except OSError:
                            pass
                        print(f"[intent] -> {txt[:80]!r}", flush=True)
                        CHAT.broadcast({"type": "intent", "text": txt})
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


def _resolve_voice_names():
    """Fill in human names for the voice picker (background, best-effort).
    Account voices answer /v1/voices/{id}; library voices not saved to the
    account 404 there but turn up via the shared-voices search-by-id."""
    def _get(url):
        req = urllib.request.Request(url, headers={"xi-api-key": ELEVENLABS_API_KEY})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.load(resp)
    changed = False
    for v in VOICES:
        name = ""
        try:
            name = (_get(f"https://api.elevenlabs.io/v1/voices/{v['id']}")
                    .get("name") or "").strip()
        except Exception:
            try:
                hits = _get("https://api.elevenlabs.io/v1/shared-voices"
                            f"?search={v['id']}").get("voices") or []
                name = next((h.get("name", "") for h in hits
                             if h.get("voice_id") == v["id"]), "").strip()
            except Exception:
                pass  # offline / unknown id — raw id stays
        if name:
            v["name"] = name
            changed = True
    if changed:
        CHAT.broadcast({"type": "voices", "voices": VOICES,
                        "voice": CUR_VOICE["id"]})


def main():
    _load_llm_log()
    if ELEVENLABS_API_KEY:
        threading.Thread(target=_resolve_voice_names, daemon=True).start()
        _load_calls()
    if ELEVEN_AGENTS:
        threading.Thread(target=_prime_agent_cache, daemon=True).start()
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
    print(f"  tts        : {'ElevenLabs ' + CUR_VOICE['id'] + f' (+{len(VOICES)-1} more in picker)' if ELEVENLABS_API_KEY else 'browser fallback'}", flush=True)
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
