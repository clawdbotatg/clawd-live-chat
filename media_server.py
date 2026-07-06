#!/usr/bin/env python3
"""clawd-live-chat media server — Twilio Media Streams (bidirectional audio).

The low-latency phone path. Everything streams, all over WebSockets:

  Twilio ──inbound μ-law──▶ Deepgram (streaming STT) ──transcript──▶ brain (Bankr)
  brain ──tokens──▶ ElevenLabs (streaming TTS, μ-law) ──audio──▶ Twilio ──▶ caller

She starts talking ~0.5s after you stop (vs 3-5s on the old <Gather> loop), and
barge-in works: Deepgram's SpeechStarted while she's talking flushes Twilio's
buffer and cancels her mid-sentence, just like the browser.

Runs as its own asyncio service (port MEDIA_PORT) behind nginx at wss://…/media.
Persona, goal and creds are imported from server.py — one source of truth, so
editing the Claudia prompt there also changes how she sounds on the phone.
"""
import asyncio
import base64
import json
import os
import queue
import threading
import urllib.request

import websockets

import server  # persona prompt, creds, CALL_GOAL, live mission — single source of truth

DEEPGRAM_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
MEDIA_PORT   = int(os.environ.get("MEDIA_PORT", "8792"))
VOICE        = server.ELEVENLABS_VOICE_ID
ELEVEN_KEY   = server.ELEVENLABS_API_KEY

# Deepgram: μ-law 8k (Twilio's native format), utterance endpointing + VAD events
# (SpeechStarted) so we can detect barge-in.
DG_URL = ("wss://api.deepgram.com/v1/listen?encoding=mulaw&sample_rate=8000"
          "&channels=1&model=nova-2-phonecall&interim_results=true"
          "&endpointing=300&vad_events=true&punctuate=true&smart_format=true")

# ElevenLabs input-streaming WS: feed text as the brain generates it, get μ-law
# audio back in near-real-time. Lowest-latency ElevenLabs option.
def eleven_url():
    return (f"wss://api.elevenlabs.io/v1/text-to-speech/{VOICE}/stream-input"
            f"?model_id=eleven_flash_v2_5&output_format=ulaw_8000&inactivity_timeout=60")

# On the phone there's no deep worker wired, so neutralize the lookup/[[DEEP]]
# mechanics from the shared voice prompt — she should just talk like a person.
MEDIA_OVERRIDE = ("\n\nIMPORTANT override for THIS phone call: you do NOT have a "
                  "computer or worker to look things up right now. Never say 'let "
                  "me look that up', never promise to check anything, and never "
                  "emit any bracketed tags. If you don't know something, just say "
                  "so casually or give your best guess like a normal person on a "
                  "call would. Keep replies to one or two short spoken sentences.")


def brain_stream_blocking(messages, token_q, stop_flag):
    """Blocking SSE read from the Bankr gateway; pushes tokens onto a queue.

    Runs in a thread (asyncio-friendly). Terminates the stream with None.
    """
    body = {"model": server.FAST_MODEL, "stream": True,
            "max_tokens": 300, "temperature": 0.8, "messages": messages}
    if server.BANKR_API == "bankr":
        headers = {"X-API-Key": server.BANKR_API_KEY, "content-type": "application/json"}
    else:
        headers = {"Authorization": f"Bearer {server.BANKR_API_KEY}",
                   "content-type": "application/json"}
    req = urllib.request.Request(f"{server.BANKR_BASE_URL}/chat/completions",
                                 data=json.dumps(body).encode(),
                                 headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            for raw in resp:
                if stop_flag.is_set():
                    break
                line = raw.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    delta = (json.loads(data)["choices"][0].get("delta") or {}
                             ).get("content") or ""
                except Exception:
                    continue
                if delta:
                    token_q.put(delta)
    except Exception as e:
        print(f"[media] brain error: {e}", flush=True)
    token_q.put(None)


class CallSession:
    """One phone call's audio pipeline."""

    def __init__(self, twilio_ws):
        self.tw = twilio_ws
        self.stream_sid = None
        self.messages = []
        self.dg = None
        self.speaking = False       # she is currently streaming audio out
        self.speak_task = None
        self.mission = None

    async def run(self):
        headers = {"Authorization": f"Token {DEEPGRAM_KEY}"}
        async with websockets.connect(DG_URL, additional_headers=headers,
                                      max_size=None) as dg:
            self.dg = dg
            reader = asyncio.create_task(self._deepgram_reader())
            try:
                await self._twilio_reader()
            finally:
                reader.cancel()
                if self.speak_task:
                    self.speak_task.cancel()

    async def _twilio_reader(self):
        async for raw in self.tw:
            msg = json.loads(raw)
            ev = msg.get("event")
            if ev == "start":
                self.stream_sid = msg["start"]["streamSid"]
                params = msg["start"].get("customParameters") or {}
                self.mission = (params.get("mission") or "").strip() or None
                direction = params.get("direction", "inbound")
                caller = params.get("caller", "")
                self._seed(direction, caller)
                print(f"[media] call start ({direction}, {caller}) "
                      f"mission={self.mission!r}", flush=True)
                self.speak_task = asyncio.create_task(self._respond())
            elif ev == "media":
                audio = base64.b64decode(msg["media"]["payload"])
                if self.dg:
                    await self.dg.send(audio)
            elif ev == "stop":
                print("[media] call stop", flush=True)
                break

    async def _deepgram_reader(self):
        async for raw in self.dg:
            m = json.loads(raw)
            t = m.get("type")
            if t == "SpeechStarted":
                if self.speaking:
                    await self._barge_in()
            elif t == "Results":
                alt = m["channel"]["alternatives"][0]
                text = (alt.get("transcript") or "").strip()
                if text and m.get("is_final") and m.get("speech_final"):
                    print(f"[media] caller: {text}", flush=True)
                    self.messages.append({"role": "user", "content": text})
                    if self.speak_task and not self.speak_task.done():
                        self.speak_task.cancel()
                    self.speak_task = asyncio.create_task(self._respond())

    async def _barge_in(self):
        if self.speak_task and not self.speak_task.done():
            self.speak_task.cancel()
        self.speaking = False
        try:
            await self.tw.send(json.dumps({"event": "clear",
                                           "streamSid": self.stream_sid}))
        except Exception:
            pass

    def _seed(self, direction, caller):
        self.messages = [{"role": "system",
                          "content": server.FAST_SYS_PROMPT + MEDIA_OVERRIDE}]
        if self.mission and direction.startswith("outbound"):
            opener = (f"[phone] You just placed this call and they answered. YOUR "
                      f"OBJECTIVE: {self.mission}. This overrides any standing goal. "
                      "Open naturally like a person calling for exactly that reason.")
        elif direction.startswith("outbound"):
            opener = "[phone] You placed this call and they answered. Greet them warmly in one short sentence."
        else:
            opener = ("[phone] A call just connected. Greet the caller naturally in "
                      "one short sentence, like answering the phone.")
        self.messages.append({"role": "user", "content": opener})

    async def _respond(self):
        """Stream brain → ElevenLabs → Twilio for one turn, pipelined by token."""
        stop_flag = threading.Event()
        token_q = queue.Queue()
        full = ""
        try:
            self.speaking = True
            msgs = [{"role": m["role"], "content": m["content"]} for m in self.messages]
            threading.Thread(target=brain_stream_blocking,
                             args=(msgs, token_q, stop_flag), daemon=True).start()
            headers = {"xi-api-key": ELEVEN_KEY}
            async with websockets.connect(eleven_url(), additional_headers=headers,
                                          max_size=None) as el:
                await el.send(json.dumps({
                    "text": " ",
                    "voice_settings": {"stability": 0.5, "similarity_boost": 0.6,
                                       "use_speaker_boost": True, "speed": 1.05},
                    "generation_config": {"chunk_length_schedule": [80, 160, 250]},
                }))
                pump = asyncio.create_task(self._eleven_to_twilio(el))
                loop = asyncio.get_event_loop()
                while True:
                    tok = await loop.run_in_executor(None, token_q.get)
                    if tok is None:
                        break
                    # never voice a stray bracketed tag if one slips through
                    full += tok
                    await el.send(json.dumps({"text": tok}))
                await el.send(json.dumps({"text": ""}))   # flush + end generation
                await pump
            spoken = server.DEEP_TAG.sub("", full).strip()
            if spoken:
                self.messages.append({"role": "assistant", "content": spoken})
                print(f"[media] claudia: {spoken[:100]}", flush=True)
        except asyncio.CancelledError:
            stop_flag.set()
            raise
        finally:
            stop_flag.set()
            self.speaking = False

    async def _eleven_to_twilio(self, el):
        async for raw in el:
            m = json.loads(raw)
            audio = m.get("audio")
            if audio:
                await self.tw.send(json.dumps({
                    "event": "media",
                    "streamSid": self.stream_sid,
                    "media": {"payload": audio}}))
            if m.get("isFinal"):
                break


async def handler(ws):
    path = getattr(getattr(ws, "request", None), "path", "") or ""
    if "/media" not in path:
        await ws.close()
        return
    sess = CallSession(ws)
    try:
        await sess.run()
    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[media] session error: {e}", flush=True)


async def main():
    print(f"clawd-media on ws://127.0.0.1:{MEDIA_PORT}/media "
          f"(deepgram {'keyed' if DEEPGRAM_KEY else 'NO KEY — STT will fail'}, "
          f"tts {'keyed' if ELEVEN_KEY else 'NO KEY'})", flush=True)
    async with websockets.serve(handler, "127.0.0.1", MEDIA_PORT, max_size=None):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
