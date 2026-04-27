#!/usr/bin/env python3
"""
Mock vLLM backend with SSE streaming for OpenResty body-log stress tests.

OpenAI /v1/chat/completions compatible:
- Accepts POST with arbitrary body (echoes session_id from metadata/header)
- Streams `data: {...}\n\n` chunks ending with `data: [DONE]\n\n`
- Configurable output length + per-chunk delay + prefill delay

Usage:
  python3 mock_vllm_sse.py --port 28001 --name mock-1 \\
      --output-len 1500 --chunk-delay-ms 5 --prefill-delay-ms 50

aiohttp event loop scales to thousands of concurrent connections per process.
Default values can also be overridden per-request via JSON body fields:
  {"max_tokens": 64000}   -> override output_len
  {"chunk_delay_ms": 10}  -> override chunk delay
  {"prefill_delay_ms": 0} -> override prefill delay
"""
import argparse
import asyncio
import json
import time
import os
import sys

try:
    from aiohttp import web
except ImportError:
    print("ERROR: aiohttp required. Install via 'pip install aiohttp' or use python with aiohttp baked in.", file=sys.stderr)
    sys.exit(1)


# -- chunk text generator -----------------------------------------------------
# 每 chunk 一个 token；token 文本是固定字符串以避免 JSON 转义复杂度
_CHUNK_CONTENT = "x"


def _make_chunk(model: str, session_id: str, content: str, idx: int) -> bytes:
    """Build one OpenAI-style SSE chunk."""
    payload = {
        "id": f"chatcmpl-mock-{session_id or 'none'}-{idx}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {"index": 0, "delta": {"content": content}, "finish_reason": None}
        ],
    }
    return ("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode()


def _make_final_chunk(model: str, session_id: str) -> bytes:
    payload = {
        "id": f"chatcmpl-mock-{session_id or 'none'}-final",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {"index": 0, "delta": {}, "finish_reason": "stop"}
        ],
    }
    return ("data: " + json.dumps(payload) + "\n\n").encode()


# -- handlers -----------------------------------------------------------------
async def health(request: web.Request):
    return web.Response(text="OK")


async def stats(request: web.Request):
    return web.json_response({
        "peer": request.app["name"],
        "port": request.app["port"],
        "requests": request.app["req_count"],
        "in_flight": request.app["in_flight"],
    })


async def chat_completions(request: web.Request):
    app = request.app
    app["req_count"] += 1
    app["in_flight"] += 1
    try:
        # Read full body (could be 50K+ tokens, ~150KB)
        try:
            body = await request.json()
        except Exception:
            body = {}

        model = body.get("model", "mock-model")
        # Extract session_id from various sources (matches openresty extract_session_id chain)
        sid = (
            request.headers.get("x-litellm-session-id")
            or request.headers.get("x-claude-code-session-id")
            or request.headers.get("x-session-id")
            or (body.get("metadata") or {}).get("session_id")
            or body.get("user")
            or ""
        )
        out_len = int(body.get("max_tokens") or app["output_len"])
        chunk_delay = float(body.get("chunk_delay_ms") or app["chunk_delay_ms"]) / 1000.0
        prefill_delay = float(body.get("prefill_delay_ms") or app["prefill_delay_ms"]) / 1000.0

        # Decide if streaming (default true per vllm openai compat)
        stream = body.get("stream", True)

        if not stream:
            # Non-streaming: simulate prefill + decode then return one big response
            await asyncio.sleep(prefill_delay + chunk_delay * out_len)
            content = _CHUNK_CONTENT * out_len
            payload = {
                "id": f"chatcmpl-mock-{sid or 'none'}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": out_len, "total_tokens": out_len + 1},
            }
            return web.json_response(payload, headers={"X-Mock-Peer": app["name"]})

        # Streaming path
        resp = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Mock-Peer": app["name"],
            },
        )
        await resp.prepare(request)

        # prefill
        if prefill_delay > 0:
            await asyncio.sleep(prefill_delay)

        # SSE chunks
        for i in range(out_len):
            await resp.write(_make_chunk(model, sid, _CHUNK_CONTENT, i))
            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)

        await resp.write(_make_final_chunk(model, sid))
        await resp.write(b"data: [DONE]\n\n")
        await resp.write_eof()
        return resp
    finally:
        app["in_flight"] -= 1


# -- app factory --------------------------------------------------------------
def make_app(args):
    app = web.Application(client_max_size=10 * 1024 * 1024)  # 10 MB request body
    app["name"] = args.name
    app["port"] = args.port
    app["output_len"] = args.output_len
    app["chunk_delay_ms"] = args.chunk_delay_ms
    app["prefill_delay_ms"] = args.prefill_delay_ms
    app["req_count"] = 0
    app["in_flight"] = 0
    app.router.add_get("/health", health)
    app.router.add_get("/_stats", stats)
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_post("/v1/completions", chat_completions)  # alias for older bench
    return app


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--output-len", type=int, default=int(os.environ.get("OUTPUT_LEN", "1500")))
    p.add_argument("--chunk-delay-ms", type=float, default=float(os.environ.get("CHUNK_DELAY_MS", "5")))
    p.add_argument("--prefill-delay-ms", type=float, default=float(os.environ.get("PREFILL_DELAY_MS", "50")))
    args = p.parse_args()

    app = make_app(args)
    print(f"[mock-sse] {args.name} on :{args.port}  out={args.output_len}  "
          f"chunk={args.chunk_delay_ms}ms  prefill={args.prefill_delay_ms}ms", flush=True)
    web.run_app(app, host="127.0.0.1", port=args.port, access_log=None, print=lambda *_: None)


if __name__ == "__main__":
    main()
