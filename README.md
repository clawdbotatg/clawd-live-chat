# clawd-live-chat

A **two-tier live voice agent**: a fast conversational brain you talk to in
real time, backed by a deep worker (a real Claude Code agent) it can dispatch
for reasoning and building — with results flowing back into the same
conversation.

```
you (voice) ── Web Speech STT ──▶ FAST BRAIN (Haiku via Bankr, streams tokens)
     ▲                                │
     │ ElevenLabs Flash v2.5,         ├── normal turn: answers in <1s
     │ sentence-pipelined TTS         └── [[DEEP: task]] tag
     │                                       │
     └── spoken summary ◀── same thread ◀── DEEP WORKER (claude -p via
                                             claude-p-agent, minutes of work)
```

- **Fast brain** holds the *whole* conversation (including deep results, so
  you can ask follow-ups about them). First spoken audio typically ~1s after
  you stop talking: tokens stream, sentences are cut at boundaries and each
  is TTS'd while the next still generates.
- **Deep tier** is `claude -p` on your subscription (env-scrubbed via
  claude-p-agent's `run_turn`). The fast brain dispatches it with an unspoken
  `[[DEEP: …]]` tag, acknowledges out loud, and keeps chatting. The worker's
  `SPOKEN SUMMARY:` re-enters the thread and gets relayed by voice. Deep tasks
  share one resumed claude session, so they build context across a call.
- **Open mic + barge-in**: talk over it and it stops and listens (with a
  self-echo guard so it doesn't barge in on its own voice).

## Run

```bash
python3 server.py      # → http://127.0.0.1:8790/
```

Zero config on a box with clawd-harness set up: creds fall back to
`~/clawd-harness/.clawd-harness.env` (`BANKR_*`, `ELEVENLABS_*`). Otherwise
`cp .clawd-live-chat.env.example .clawd-live-chat.env` and fill in. The deep
tier needs `../claude-p-agent` (or `CLAUDE_P_AGENT_HOME`) and the `claude` CLI.

Mic + Web Speech need a **secure context** — use `http://localhost:8790` (or
put TLS in front for phones). Deep worker output lands in `deepwork/`
(gitignored). Chat is in-memory only; restart = fresh conversation.

## Test without a browser

Open a WS to `/ws`, send `{"type":"user","text":"…"}`, and watch `delta` /
`speak` / `deep` events stream back. `POST /tts {"text":"…"}` returns MP3.

Stdlib only. WS framing + ElevenLabs proxy lifted from clawd-harness;
deep spawn from claude-p-agent.
