#!/usr/bin/env python3
"""bench_chat.py — pick the FAST_MODEL: the best fast conversational brain.

Benchmarks candidate models on the Bankr gateway with the REAL live-composed
chat system prompt. What matters for voice, in order:

  1. dispatch — emits [[DEEP: …]] when it should and ONLY when it should.
     A model that says "let me check" without dispatching is hallucinating a
     lookup; a model that dispatches on small talk burns deep-worker turns.
     Hard gate: any miss disqualifies.
  2. voice-ok — replies are voice-shaped: short, no markdown, no AI-reveal.
     Hard gate.
  3. ttfs — time to first *sentence* (median, streamed). Sentences are cut
     and TTS'd as they land, so this IS the perceived reply latency.

Usage:
  python3 bench_chat.py                # curated fast-tier candidates
  python3 bench_chat.py model [model…] # just these

Re-run roughly quarterly (new models ship constantly) — last run 2026-07,
next ≈2026-10. If a model beats the incumbent on all three, update
FAST_MODEL in .clawd-live-chat.env (or the default in server.py).

2026-07-13 results (16 candidates): claude-haiku-4.5 KEPT — 0.98s ttfs and
the only top-latency model with perfect 10/10 dispatch. gemini-3.1-flash-lite
was the runner-up (0.81s ttfs, perfect dispatch, persona a notch blander).
gpt-5.4-mini was fastest (0.53s) but said "I'm checking that" WITHOUT
dispatching 1/2 runs; gpt-5.4-nano leaked the tag as spoken text. Traps:
claude-sonnet-5 and the qwen/glm/grok big tiers burn 3–60s thinking before
the first token — reasoning models are not voice models.
"""
import concurrent.futures as cf
import json
import os
import re
import statistics
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent


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
KEY = os.environ.get("BANKR_API_KEY", "")
BASE = os.environ.get("BANKR_BASE_URL", "https://llm.bankr.bot/v1").rstrip("/")
PORT = int(os.environ.get("CHAT_PORT", "8790"))
if not KEY:
    sys.exit("no BANKR_API_KEY in .clawd-live-chat.env / harness env")


def sys_prompt():
    """The exact prompt the fast brain runs with: live server first (it holds
    the fresh ElevenLabs persona), composed via server.py as the fallback."""
    try:
        d = json.loads(urllib.request.urlopen(
            f"http://127.0.0.1:{PORT}/debug/llm.json", timeout=5).read())
        return d["sys_prompt"]
    except Exception:
        sys.path.insert(0, str(HERE))
        import server
        return server.chat_sys_prompt()


SYS = sys_prompt()

CONVO = [
    {"role": "system", "content": SYS},
    {"role": "user", "content": "hey claudia, you there?"},
    {"role": "assistant", "content": "Yep, right here! What's up?"},
    {"role": "user", "content": "we're thinking tacos tonight but jimmy hasn't "
     "texted back. what would you do, wait for him or just start cooking?"},
]

# (name, user message, should it [[DEEP:]]-dispatch?)
DISPATCH_CASES = [
    ("lookup", "what's the weather like in fort collins right now?", True),
    ("build", "can you make me a little python script that renames all my "
              "photos by date? just do it, files are in ~/Pictures/roll", True),
    ("chat", "haha fair enough. anyway how's your day going?", False),
]

CANDIDATES = [
    "claude-haiku-4.5", "claude-sonnet-5", "claude-sonnet-4.6",
    "gemini-3.5-flash", "gemini-3.1-flash-lite", "gemini-2.5-flash",
    "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.6-luna", "gpt-5.6-terra",
    "deepseek-v4-flash", "qwen3.5-flash", "qwen3.6-flash",
    "grok-4.5", "minimax-m2.7-highspeed", "glm-5-turbo",
]

SENT_RE = re.compile(r'[.!?…]["\')\]]*\s')
BAD_RE = re.compile(r"[*#`]|^\s*-\s|\bAs an AI\b|\blanguage model\b", re.I | re.M)
TAG_RE = re.compile(r"\[\[\s*DEEP\s*:")


def _request(model, messages, stream):
    body = {"model": model, "stream": stream, "max_tokens": 700,
            "temperature": 0.7, "messages": messages}
    if os.environ.get("BANKR_API", "bankr").lower() == "bankr":
        headers = {"X-API-Key": KEY, "content-type": "application/json"}
    else:
        headers = {"Authorization": f"Bearer {KEY}",
                   "content-type": "application/json"}
    return urllib.request.Request(BASE + "/chat/completions",
                                  data=json.dumps(body).encode(),
                                  headers=headers, method="POST")


def timed_run(model):
    t0 = time.time()
    ttft = ttfs = None
    full = ""
    with urllib.request.urlopen(_request(model, CONVO, True), timeout=90) as resp:
        for raw in resp:
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
            if ttft is None:
                ttft = time.time() - t0
            full += delta
            if ttfs is None and SENT_RE.search(full):
                ttfs = time.time() - t0
    total = time.time() - t0
    if ttfs is None and full.strip():
        ttfs = total
    ok = bool(full.strip()) and len(full) < 600 and not BAD_RE.search(full)
    return {"ttft": ttft, "ttfs": ttfs, "total": total, "ok": ok}


def dispatch_run(model, msg):
    with urllib.request.urlopen(_request(
            model, [{"role": "system", "content": SYS},
                    {"role": "user", "content": msg}], False), timeout=90) as r:
        text = (json.loads(r.read())["choices"][0]["message"]["content"] or "")
    return bool(TAG_RE.search(text))


def bench(model):
    try:
        timed_run(model)                              # warm-up, untimed
    except Exception:
        pass
    runs = []
    for _ in range(3):
        try:
            runs.append(timed_run(model))
        except Exception as e:
            runs.append({"error": str(e)[:80]})
    good = [r for r in runs if "error" not in r and r["ttft"] is not None]
    if not good:
        return {"model": model, "dead": True,
                "why": runs[-1].get("error", "no content tokens")}
    disp_ok, disp_n = 0, 0
    for _, msg, want in DISPATCH_CASES:
        for _ in range(2):
            disp_n += 1
            try:
                if dispatch_run(model, msg) == want:
                    disp_ok += 1
            except Exception:
                pass
    med = lambda k: round(statistics.median(r[k] for r in good), 2)
    return {"model": model, "dead": False, "n": len(good),
            "ttft": med("ttft"), "ttfs": med("ttfs"), "total": med("total"),
            "voice": sum(r["ok"] for r in good),
            "disp_ok": disp_ok, "disp_n": disp_n}


def main():
    models = sys.argv[1:] or CANDIDATES
    print(f"benching {len(models)} models against a {len(SYS)}-char live "
          f"system prompt (3 timed + {2 * len(DISPATCH_CASES)} dispatch runs "
          "each)…\n", flush=True)
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        results = list(ex.map(bench, models))
    results.sort(key=lambda r: (r.get("dead", False), r.get("ttfs") or 999))
    print(f"{'model':<26} {'ttft':>6} {'ttfs':>6} {'total':>6} "
          f"{'voice':>6} {'dispatch':>9}")
    winner = None
    for r in results:
        if r["dead"]:
            print(f"{r['model']:<26} DEAD — {r['why']}")
            continue
        clean = r["voice"] == r["n"] and r["disp_ok"] == r["disp_n"]
        if clean and winner is None:
            winner = r["model"]
        print(f"{r['model']:<26} {r['ttft']:>6} {r['ttfs']:>6} {r['total']:>6} "
              f"{str(r['voice']) + '/' + str(r['n']):>6} "
              f"{str(r['disp_ok']) + '/' + str(r['disp_n']):>9}"
              + ("" if clean else "   ✗ disqualified"))
    if winner:
        print(f"\nrecommendation: {winner} "
              "(fastest ttfs with perfect voice + dispatch)")
    else:
        print("\nrecommendation: nothing passed both gates — widen the "
              "candidate list")


if __name__ == "__main__":
    main()
