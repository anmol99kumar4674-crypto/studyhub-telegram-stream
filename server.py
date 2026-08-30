import os
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

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)


def authorized(request: Request):
    if STREAM_KEY and request.headers.get("x-stream-key") != STREAM_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.on_event("startup")
async def startup():
    await client.start()


@app.on_event("shutdown")
async def shutdown():
    await client.disconnect()


async def get_message(message_id: int, chat: str):
    try:
        return await client.get_messages(chat, ids=message_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Telegram message not found: {e}")


@app.get("/health")
async def health():
    return {"ok": True}


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
        # Telethon downloads in chunks and yields them immediately.
        # This avoids loading the whole 500–600 MB file into RAM.
        offset = start
        remaining = length
        chunk_size = 1024 * 1024

        while remaining > 0:
            take = min(chunk_size, remaining)
            data = await client.download_media(
                message,
                file=bytes,
                offset=offset,
                limit=take,
            )
            if not data:
                break
            yield data
            got = len(data)
            offset += got
            remaining -= got

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Cache-Control": "private, no-store",
    }

    if size:
        headers["Content-Length"] = str(length)
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    status = 206 if range_header and size else 200
    return StreamingResponse(body(), status_code=status, headers=headers)


@app.get("/")
async def root():
    return {"service": "StudyHub Telegram Stream Server", "status": "running"}
