#!/usr/bin/env python3
"""
Mock vLLM backend for OpenResty body-log + routing stress tests.

支持三个真 vllm 接口的线缆兼容模拟（参考 gpu-node 真后端 Kimi-K2.5 输出对齐）：

  POST /v1/chat/completions
       - 非流：返回 chat.completion 对象（choices[].message.content / reasoning / tool_calls）
       - 流式：data: chat.completion.chunk + 末尾 [DONE]
                init chunk → N 个 content/reasoning chunk → finish_reason chunk
                → （可选）usage chunk → [DONE]
  POST /v1/completions
       - text_completion 格式（choices[].text 或非流的 message-less 格式）
  POST /v1/messages   (Anthropic)
       - 非流：单 JSON，content[]={type:"text",text:...} 或 thinking 块
       - 流式：message_start / content_block_start / content_block_delta / content_block_stop
               / message_delta / message_stop / [DONE]，每条 SSE 都带 `event: <type>` 头

Usage:
  python3 mock_vllm_sse.py --port 28001 --name mock-1 \\
      --output-len 1500 --chunk-delay-ms 5 --prefill-delay-ms 50

aiohttp event loop scales to thousands of concurrent connections per process.
Default values can also be overridden per-request via JSON body fields:
  {"max_tokens": 64000}   -> override output_len
  {"chunk_delay_ms": 10}  -> override chunk delay
  {"prefill_delay_ms": 0} -> override prefill delay
  {"emit_reasoning": true}-> 用 reasoning 字段（Kimi-K2.5 思考链）而非 content
  {"emit_tool_call": true}-> 末尾追加一个 tool_call（chat 接口）

非流式 finish_reason / stop_reason 默认 "length"（mock 总是发完 max_tokens 才停），
和真 vllm 在 max_tokens 触发时一致。
"""
import argparse
import asyncio
import json
import os
import sys
import time
import uuid

try:
    from aiohttp import web
except ImportError:
    print("ERROR: aiohttp required. Install via 'pip install aiohttp'.", file=sys.stderr)
    sys.exit(1)


# 单 token 文本：固定 ASCII 字符避免 JSON 转义复杂度，便于在 bodylog 抽取后断言
_CONTENT_TOKEN = "x"
_REASONING_TOKEN = " think"


def _gen_id(prefix: str) -> str:
    """Generate a vllm-style id (chatcmpl-<hex16> / cmpl-<hex16>)."""
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


def _extract_session_id(request: web.Request, body: dict) -> str:
    """Mirror openresty extract_session_id chain（不依赖完整 metadata 解析）。"""
    return (
        request.headers.get("x-litellm-session-id")
        or request.headers.get("x-claude-code-session-id")
        or request.headers.get("x-session-id")
        or (body.get("metadata") or {}).get("session_id")
        or body.get("user")
        or ""
    )


# ─── /v1/chat/completions: chunk builders（OpenAI 兼容）──────────────────
def _chat_init_chunk(cid: str, model: str, created: int) -> bytes:
    return _sse(json.dumps({
        "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""},
                     "logprobs": None, "finish_reason": None}],
        "prompt_token_ids": None,
    }))


def _chat_content_chunk(cid: str, model: str, created: int, content: str,
                        finish_reason=None, kind="content") -> bytes:
    """kind ∈ {"content", "reasoning"}：决定 delta 字段名"""
    delta_key = "reasoning" if kind == "reasoning" else "content"
    return _sse(json.dumps({
        "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{"index": 0, "delta": {delta_key: content}, "logprobs": None,
                     "finish_reason": finish_reason, "stop_reason": None,
                     "token_ids": None}],
    }))


def _chat_tool_call_chunk(cid: str, model: str, created: int, name: str, args: str) -> bytes:
    """模拟一个 tool_call delta（一帧整发，不拆分）。"""
    return _sse(json.dumps({
        "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 0, "id": "call_" + uuid.uuid4().hex[:12],
                            "type": "function",
                            "function": {"name": name, "arguments": args}}]
        }, "logprobs": None, "finish_reason": None, "stop_reason": None,
        "token_ids": None}],
    }))


def _chat_usage_chunk(cid: str, model: str, created: int,
                      prompt_tokens: int, completion_tokens: int) -> bytes:
    """include_usage=true 时的最终 chunk（choices=[] + usage）。"""
    return _sse(json.dumps({
        "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [],
        "usage": {"prompt_tokens": prompt_tokens, "total_tokens": prompt_tokens + completion_tokens,
                  "completion_tokens": completion_tokens},
    }))


# ─── /v1/completions: chunk builders（legacy text completion）─────────────
def _compl_text_chunk(cid: str, model: str, created: int, text: str,
                      finish_reason=None) -> bytes:
    return _sse(json.dumps({
        "id": cid, "object": "text_completion", "created": created, "model": model,
        "choices": [{"index": 0, "text": text, "logprobs": None,
                     "finish_reason": finish_reason, "stop_reason": None,
                     "prompt_token_ids": None, "token_ids": None}],
        "usage": None,
    }))


def _compl_usage_chunk(cid: str, model: str, created: int,
                       prompt_tokens: int, completion_tokens: int) -> bytes:
    return _sse(json.dumps({
        "id": cid, "object": "text_completion", "created": created, "model": model,
        "choices": [],
        "usage": {"prompt_tokens": prompt_tokens, "total_tokens": prompt_tokens + completion_tokens,
                  "completion_tokens": completion_tokens},
    }))


# ─── /v1/messages: SSE event builders（Anthropic）─────────────────────────
def _anthropic_event(event_name: str, payload: dict) -> bytes:
    """Anthropic SSE 每个事件两行：`event: <name>\\n` + `data: {...}\\n\\n`。"""
    return (f"event: {event_name}\n".encode() + _sse(json.dumps(payload)))


def _msg_start(mid: str, model: str, input_tokens: int) -> bytes:
    return _anthropic_event("message_start", {
        "type": "message_start",
        "message": {"id": mid, "content": [], "model": model,
                    "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": input_tokens, "output_tokens": 0}},
    })


def _msg_block_start(idx: int, kind="text") -> bytes:
    """kind ∈ {"text", "thinking"}"""
    block = {"type": kind, kind: ""}
    return _anthropic_event("content_block_start", {
        "type": "content_block_start", "content_block": block, "index": idx,
    })


def _msg_block_delta(idx: int, text: str, kind="text") -> bytes:
    """kind ∈ {"text", "thinking"}：决定 delta type 字段"""
    delta_type = f"{kind}_delta"
    delta_key = kind  # delta.text 或 delta.thinking
    return _anthropic_event("content_block_delta", {
        "type": "content_block_delta",
        "delta": {"type": delta_type, delta_key: text}, "index": idx,
    })


def _msg_block_stop(idx: int) -> bytes:
    return _anthropic_event("content_block_stop",
                            {"type": "content_block_stop", "index": idx})


def _msg_delta(stop_reason: str, input_tokens: int, output_tokens: int) -> bytes:
    return _anthropic_event("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": stop_reason},
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    })


def _msg_stop() -> bytes:
    return _anthropic_event("message_stop", {"type": "message_stop"})


# ─── Common: SSE framing ──────────────────────────────────────────────────
def _sse(data_payload: str) -> bytes:
    return ("data: " + data_payload + "\n\n").encode()


_DONE = b"data: [DONE]\n\n"


# ─── Handlers ────────────────────────────────────────────────────────────
async def health(request: web.Request):
    return web.Response(text="OK")


async def models_list(request: web.Request):
    """openresty health probe 打 GET /v1/models，只看状态行是否含 200。
    回一个 OpenAI 兼容的模型列表（id 用 --name，便于辨认是哪个 mock）。"""
    name = request.app["name"]
    st = request.app.get("models_status", 200)
    if st != 200:
        return web.Response(status=st, text="mock forced %d" % st)
    return web.json_response({
        "object": "list",
        "data": [{"id": name, "object": "model", "owned_by": "mock"}],
    })


async def stats(request: web.Request):
    s = request.app["stats"]
    return web.json_response({
        "peer": request.app["name"], "port": request.app["port"],
        "requests": s["req_count"], "in_flight": s["in_flight"],
    })


async def _read_params(request: web.Request):
    """读 body + 通用参数。返回 (body_dict, model, sid, out_len, chunk_delay, prefill_delay)。"""
    app = request.app
    try:
        body = await request.json()
    except Exception:
        body = {}
    model = body.get("model", "mock-model")
    sid = _extract_session_id(request, body)
    out_len = int(body.get("max_tokens") or app["output_len"])
    chunk_delay = float(body.get("chunk_delay_ms") or app["chunk_delay_ms"]) / 1000.0
    prefill_delay = float(body.get("prefill_delay_ms") or app["prefill_delay_ms"]) / 1000.0
    return body, model, sid, out_len, chunk_delay, prefill_delay


async def chat_completions(request: web.Request):
    """OpenAI /v1/chat/completions（流 + 非流）"""
    app = request.app
    stats = app["stats"]
    stats["req_count"] += 1
    cs = app.get("chat_status", 200)
    if cs != 200:
        return web.Response(status=cs, text="mock forced %d" % cs,
                            headers={"X-Mock-Peer": app["name"]})
    stats["in_flight"] += 1
    try:
        body, model, sid, out_len, chunk_delay, prefill_delay = await _read_params(request)
        stream = body.get("stream", False)
        emit_reasoning = bool(body.get("emit_reasoning"))
        emit_tool_call = bool(body.get("emit_tool_call"))
        token = _REASONING_TOKEN if emit_reasoning else _CONTENT_TOKEN
        kind = "reasoning" if emit_reasoning else "content"
        # token_bytes：每个 token 放大到 N 字节，少量 out_len 即可产出 MB 级响应(G8 截断测试用)
        token_bytes = max(1, int(body.get("token_bytes") or 1))
        unit = token * token_bytes
        # emit_routed_peer：模拟 router 上游回 X-Routed-Peer，验证 bodylog forwarded_to(G17)
        extra_hdr = {}
        if body.get("emit_routed_peer"):
            extra_hdr["X-Routed-Peer"] = "http://mock-upstream/" + app["name"]
        cid = _gen_id("chatcmpl")
        created = int(time.time())
        prompt_tokens = max(1, len(json.dumps(body.get("messages") or [])) // 4)
        finish_reason = "length"  # mock 总是把 max_tokens 跑满，对应 vllm 的 length

        if not stream:
            # ── 非流：一次性返回 chat.completion 对象 ──
            await asyncio.sleep(prefill_delay + chunk_delay * out_len)
            content_text = unit * out_len if not emit_reasoning else ""
            reasoning_text = unit * out_len if emit_reasoning else None
            tool_calls = []
            if emit_tool_call:
                tool_calls = [{"id": "call_" + uuid.uuid4().hex[:12], "type": "function",
                               "function": {"name": "get_weather",
                                            "arguments": '{"city":"Beijing"}'}}]
                finish_reason = "tool_calls"
            payload = {
                "id": cid, "object": "chat.completion", "created": created, "model": model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": content_text or None,
                                "refusal": None, "annotations": None, "audio": None,
                                "function_call": None, "tool_calls": tool_calls,
                                "reasoning": reasoning_text},
                    "logprobs": None, "finish_reason": finish_reason,
                    "stop_reason": None, "token_ids": None,
                }],
                "service_tier": None, "system_fingerprint": None,
                "usage": {"prompt_tokens": prompt_tokens,
                          "total_tokens": prompt_tokens + out_len,
                          "completion_tokens": out_len,
                          "prompt_tokens_details": None},
                "prompt_logprobs": None, "prompt_token_ids": None,
                "kv_transfer_params": None,
            }
            h = {"X-Mock-Peer": app["name"]}; h.update(extra_hdr)
            return web.json_response(payload, headers=h)

        # ── 流式 ──
        _sh = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Mock-Peer": app["name"],
        }
        _sh.update(extra_hdr)
        resp = web.StreamResponse(status=200, headers=_sh)
        await resp.prepare(request)
        if prefill_delay > 0:
            await asyncio.sleep(prefill_delay)

        # init chunk: role:"assistant", empty content
        await resp.write(_chat_init_chunk(cid, model, created))

        # N - 1 个普通 content/reasoning chunk
        for i in range(out_len - 1):
            await resp.write(_chat_content_chunk(cid, model, created, unit, kind=kind))
            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)

        # 最后一个 content chunk 带 finish_reason（与真 vllm 一致——last chunk 在
        # delta 里也带 reasoning/content，且在 choices[0].finish_reason 填写值）
        if not emit_tool_call:
            await resp.write(_chat_content_chunk(cid, model, created, unit,
                                                 finish_reason=finish_reason, kind=kind))
        else:
            # tool_calls 路径：finish_reason 在 tool_call chunk 之后单独一个空 delta chunk
            await resp.write(_chat_content_chunk(cid, model, created, unit, kind=kind))
            await resp.write(_chat_tool_call_chunk(cid, model, created,
                                                    "get_weather", '{"city":"Beijing"}'))
            # finish chunk: empty delta + finish_reason
            await resp.write(_sse(json.dumps({
                "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [{"index": 0, "delta": {}, "logprobs": None,
                             "finish_reason": "tool_calls", "stop_reason": None,
                             "token_ids": None}]
            })))

        # decoy_ct（测 TPS gmatch 末匹配 fix#3）：在真 usage 前塞一个 content chunk，
        # 其文本含 "completion_tokens": <decoy> 字面量（模拟正文里 tool-call/JSON 回显）。
        # 正确实现应取流末尾真 usage 的 completion_tokens，而非这个更早的 decoy。
        dct = body.get("decoy_ct")
        if dct is not None:
            await resp.write(_chat_content_chunk(cid, model, created,
                '{"completion_tokens": %d}' % int(dct), kind="content"))

        # usage chunk（仅当 stream_options.include_usage=true）
        if (body.get("stream_options") or {}).get("include_usage"):
            await resp.write(_chat_usage_chunk(cid, model, created, prompt_tokens, out_len))

        await resp.write(_DONE)
        await resp.write_eof()
        return resp
    finally:
        stats["in_flight"] -= 1


async def completions(request: web.Request):
    """OpenAI legacy /v1/completions（text_completion 格式）"""
    app = request.app
    stats = app["stats"]
    stats["req_count"] += 1
    stats["in_flight"] += 1
    try:
        body, model, sid, out_len, chunk_delay, prefill_delay = await _read_params(request)
        stream = body.get("stream", False)
        cid = _gen_id("cmpl")
        created = int(time.time())
        prompt = body.get("prompt", "")
        if isinstance(prompt, list):
            prompt = " ".join(str(x) for x in prompt)
        prompt_tokens = max(1, len(str(prompt)) // 4)

        if not stream:
            await asyncio.sleep(prefill_delay + chunk_delay * out_len)
            text = _CONTENT_TOKEN * out_len
            payload = {
                "id": cid, "object": "text_completion", "created": created, "model": model,
                "choices": [{"index": 0, "text": text, "logprobs": None,
                             "finish_reason": "length", "stop_reason": None,
                             "token_ids": None, "prompt_logprobs": None,
                             "prompt_token_ids": None}],
                "service_tier": None, "system_fingerprint": None,
                "usage": {"prompt_tokens": prompt_tokens,
                          "total_tokens": prompt_tokens + out_len,
                          "completion_tokens": out_len,
                          "prompt_tokens_details": None},
                "kv_transfer_params": None,
            }
            return web.json_response(payload, headers={"X-Mock-Peer": app["name"]})

        # 流式
        resp = web.StreamResponse(status=200, headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache", "X-Mock-Peer": app["name"],
        })
        await resp.prepare(request)
        if prefill_delay > 0:
            await asyncio.sleep(prefill_delay)
        for i in range(out_len - 1):
            await resp.write(_compl_text_chunk(cid, model, created, _CONTENT_TOKEN))
            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)
        # 最后一 chunk 带 finish_reason
        await resp.write(_compl_text_chunk(cid, model, created, _CONTENT_TOKEN,
                                            finish_reason="length"))
        # usage chunk（只有 stream_options.include_usage=true 才发）
        if (body.get("stream_options") or {}).get("include_usage"):
            await resp.write(_compl_usage_chunk(cid, model, created, prompt_tokens, out_len))
        await resp.write(_DONE)
        await resp.write_eof()
        return resp
    finally:
        stats["in_flight"] -= 1


async def messages(request: web.Request):
    """Anthropic /v1/messages（流 + 非流）"""
    app = request.app
    stats = app["stats"]
    stats["req_count"] += 1
    stats["in_flight"] += 1
    try:
        body, model, sid, out_len, chunk_delay, prefill_delay = await _read_params(request)
        stream = body.get("stream", False)
        emit_thinking = bool(body.get("emit_thinking"))
        kind = "thinking" if emit_thinking else "text"
        token = _REASONING_TOKEN if emit_thinking else _CONTENT_TOKEN
        mid = _gen_id("msg")
        # 真 vllm 的 /v1/messages 用 chatcmpl 前缀（实测 #5），保留兼容
        if (request.headers.get("x-mock-anthropic-id-style") or "vllm") == "vllm":
            mid = _gen_id("chatcmpl")
        input_tokens = max(1, len(json.dumps(body.get("messages") or [])) // 4)

        if not stream:
            await asyncio.sleep(prefill_delay + chunk_delay * out_len)
            text = token * out_len
            content_block = {"type": kind, kind: text}
            if emit_thinking:
                content_block["signature"] = uuid.uuid4().hex[:32]
            payload = {
                "id": mid, "type": "message", "role": "assistant",
                "content": [content_block], "model": model,
                "stop_reason": "max_tokens",
                "usage": {"input_tokens": input_tokens, "output_tokens": out_len},
            }
            return web.json_response(payload, headers={"X-Mock-Peer": app["name"]})

        # 流式
        resp = web.StreamResponse(status=200, headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache", "X-Mock-Peer": app["name"],
        })
        await resp.prepare(request)
        if prefill_delay > 0:
            await asyncio.sleep(prefill_delay)

        await resp.write(_msg_start(mid, model, input_tokens))
        await resp.write(_msg_block_start(0, kind=kind))
        for i in range(out_len):
            await resp.write(_msg_block_delta(0, token, kind=kind))
            if chunk_delay > 0:
                await asyncio.sleep(chunk_delay)
        await resp.write(_msg_block_stop(0))
        await resp.write(_msg_delta("max_tokens", input_tokens, out_len))
        await resp.write(_msg_stop())
        await resp.write(_DONE)
        await resp.write_eof()
        return resp
    finally:
        stats["in_flight"] -= 1


# -- app factory --------------------------------------------------------------
def make_app(args):
    app = web.Application(client_max_size=10 * 1024 * 1024)  # 10 MB request body
    app["name"] = args.name
    app["port"] = args.port
    app["output_len"] = args.output_len
    app["chunk_delay_ms"] = args.chunk_delay_ms
    app["prefill_delay_ms"] = args.prefill_delay_ms
    app["models_status"] = args.models_status
    app["chat_status"] = args.chat_status
    app["stats"] = {"req_count": 0, "in_flight": 0}
    app.router.add_get("/health", health)
    app.router.add_get("/v1/models", models_list)
    app.router.add_get("/_stats", stats)
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_post("/v1/completions", completions)
    app.router.add_post("/v1/messages", messages)
    return app


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--output-len", type=int, default=int(os.environ.get("OUTPUT_LEN", "1500")))
    p.add_argument("--chunk-delay-ms", type=float, default=float(os.environ.get("CHUNK_DELAY_MS", "5")))
    p.add_argument("--prefill-delay-ms", type=float, default=float(os.environ.get("PREFILL_DELAY_MS", "50")))
    p.add_argument("--models-status", type=int, default=200, help="坏 mock 模拟:/v1/models 返此状态(F5 测健康探测 ban)")
    p.add_argument("--chat-status", type=int, default=200, help="坏 mock 模拟:/v1/chat/completions 返此状态(E5 测 next_upstream retry)")
    args = p.parse_args()

    app = make_app(args)
    print(f"[mock-sse] {args.name} on :{args.port}  out={args.output_len}  "
          f"chunk={args.chunk_delay_ms}ms  prefill={args.prefill_delay_ms}ms", flush=True)
    web.run_app(app, host="127.0.0.1", port=args.port, access_log=None, print=lambda *_: None)


if __name__ == "__main__":
    main()
