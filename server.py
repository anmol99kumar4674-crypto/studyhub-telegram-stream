import os
import re
import asyncio
from time import monotonic
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

app = FastAPI(title="StudyHub Telegram Stream Server")

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION = os.environ["TELEGRAM_SESSION"]
STREAM_KEY = os.environ.get("STREAM_KEY", "")

client = TelegramClient(
    StringSession(SESSION),
    API_ID,
    API_HASH,
    connection_retries=3,
    retry_delay=1,
)

# Avoid a Telegram API lookup every time the browser opens a new HTTP range.
# The message object is immutable enough for streaming purposes and is cached
# briefly per process. This substantially reduces startup latency.
_MESSAGE_CACHE = {}
_MESSAGE_CACHE_TTL = 300  # seconds
_MESSAGE_LOCKS = {}


def authorized(request: Request):
    # Normal HTML <video> requests cannot set a custom header.
    # Accept the same key as ?key=... for direct browser playback.
    supplied = request.headers.get("x-stream-key") or request.query_params.get("key")
    if STREAM_KEY and supplied != STREAM_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.on_event("startup")
async def startup():
    # Never use client.start() on Render: it can enter an interactive
    # phone/code prompt when the StringSession is missing or invalid.
    if not SESSION.strip():
        raise RuntimeError("TELEGRAM_SESSION is empty. Generate a Telethon StringSession and set it in Render Environment Variables.")

    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("TELEGRAM_SESSION is invalid or not authorized. Generate a fresh Telethon StringSession and replace the Render variable.")


@app.on_event("shutdown")
async def shutdown():
    await client.disconnect()


async def get_message(message_id: int, chat: str):
    cache_key = (str(chat), int(message_id))
    now = monotonic()
    cached = _MESSAGE_CACHE.get(cache_key)
    if cached and now - cached[0] < _MESSAGE_CACHE_TTL:
        return cached[1]

    # Prevent several simultaneous browser range requests from all doing the
    # same Telegram get_messages call during startup.
    lock = _MESSAGE_LOCKS.setdefault(cache_key, asyncio.Lock())
    async with lock:
        cached = _MESSAGE_CACHE.get(cache_key)
        if cached and monotonic() - cached[0] < _MESSAGE_CACHE_TTL:
            return cached[1]

        try:
            # Try the supplied ID first, then the common -100 channel-ID form.
            candidates = [chat]
            if re.fullmatch(r"-?\d+", chat):
                n = int(chat)
                if n > 0:
                    candidates.append(f"-100{n}")
                elif not chat.startswith("-100"):
                    candidates.append(f"-100{abs(n)}")

            last_error = None
            for candidate in candidates:
                try:
                    message = await client.get_messages(candidate, ids=message_id)
                    if message:
                        _MESSAGE_CACHE[cache_key] = (monotonic(), message)
                        return message
                except Exception as e:
                    last_error = e

            raise HTTPException(
                status_code=404,
                detail=f"Telegram message not found. Check channel ID and session authorization. {last_error or ''}"
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Telegram message not found: {e}")
        finally:
            _MESSAGE_LOCKS.pop(cache_key, None)


@app.get("/health")
async def health():
    return {"ok": True}


@app.options("/video/{message_id}")
async def video_options(request: Request, message_id: int):
    authorized(request)
    return StreamingResponse(
        iter(()),
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Range, X-Stream-Key, Content-Type",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        },
    )


@app.get("/video/{message_id}")
async def video(request: Request, message_id: int, chat: str):
    authorized(request)
    message = await get_message(message_id, chat)

    if not message or not message.media:
        raise HTTPException(status_code=404, detail="No media in message")

    document = getattr(message, "document", None)
    if not document:
        raise HTTPException(status_code=400, detail="Message is not a document/video")

    size = getattr(document, "size", None)
    mime = getattr(document, "mime_type", None) or "video/mp4"

    # Range support for browser seeking/streaming.
    range_header = request.headers.get("range")
    start = 0
    end = (size - 1) if size else None

    if range_header and size:
        try:
            value = range_header.replace("bytes=", "")
            left, right = value.split("-", 1)
            if left:
                start = int(left)
            if right:
                end = min(int(right), size - 1)
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid Range")

    if size and start >= size:
        raise HTTPException(status_code=416, detail="Range not satisfiable")

    if end is None:
        end = size - 1

    length = end - start + 1

    async def body():
        # Stream only the requested range. Start with a reasonably sized chunk
        # so the player can begin quickly, then use larger chunks for throughput.
        remaining = length
        offset = start
        # Telegram GetFile requests have a 512 KiB maximum. Larger
        # request_size values can make the generator fail before playback.
        # Keep the first Telegram fetch small so mobile users receive the
        # first playable bytes quickly. Larger requests are used afterwards
        # for better sustained throughput.
        first_chunk = 64 * 1024
        normal_chunk = 512 * 1024
        first = True

        while remaining > 0:
            request_size = min(first_chunk if first else normal_chunk, remaining)

            # Telethon's iter_download yields media chunks without buffering
            # the complete Telegram file in memory.
            got_any = False
            async for data in client.iter_download(
                message.media,
                offset=offset,
                request_size=request_size,
                chunk_size=request_size,
            ):
                if not data:
                    break

                if len(data) > remaining:
                    data = data[:remaining]

                yield data
                got = len(data)
                offset += got
                remaining -= got
                got_any = True
                first = False

                if remaining <= 0:
                    break

            if not got_any:
                break

    headers = {
        "Accept-Ranges": "bytes",
        "Connection": "keep-alive",
        "Content-Type": mime,
        # Telegram is the origin, so don't ask the browser to cache the whole
        # stream. Keep connections reusable for sequential range requests.
        "Cache-Control": "private, max-age=300, stale-while-revalidate=60",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }

    if size:
        headers["Content-Length"] = str(length)
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    status = 206 if range_header and size else 200
    return StreamingResponse(body(), status_code=status, headers=headers)


@app.get("/")
async def root():
    return {"service": "StudyHub Telegram Stream Server", "status": "running"}
