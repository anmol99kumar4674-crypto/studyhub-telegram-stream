# StudyHub Telegram Streaming Server

This server is for videos you own or are authorized to stream.

## Deploy

Deploy this folder to a server/container host that supports Python + Docker.

Set these environment variables as secrets:

- TELEGRAM_API_ID
- TELEGRAM_API_HASH
- TELEGRAM_SESSION
- STREAM_KEY

Do NOT commit real credentials.

## Endpoint

GET /video/{message_id}?chat=<private-channel-identifier>

The server uses the authorized Telegram account to read the message and
supports HTTP Range requests so a browser video player can seek/stream
without buffering the complete 500–600 MB file into RAM.

## Important

The exact private-channel identifier must be supplied to Telethon in a form
the authorized account can resolve. For a private channel this can be the
numeric channel ID (with the appropriate Telegram peer resolution) or another
identifier supported by the authenticated account.

## Security

Keep this server private behind a strong STREAM_KEY and/or your Worker.
Never put Telegram API credentials or the session string in the StudyHub
frontend.
