import os
import asyncio
from aiohttp import web
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

API_ID = int(os.environ.get("API_ID", "0").strip())
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SERVER_URL = os.environ.get("SERVER_URL", "").rstrip('/')
PORT = int(os.environ.get("PORT", 8080))

bot = TelegramClient("vlc_clean_bot_engine", API_ID, API_HASH)
routes = web.RouteTableDef()

MEDIA_CACHE = {}

@routes.get("/stream/{chat_id}/{message_id}")
async def stream_handler(request):
    try:
        raw_chat_id = request.match_info["chat_id"].strip()
        message_id = int(request.match_info["message_id"])

        cache_key = f"{raw_chat_id}_{message_id}"
        message = MEDIA_CACHE.get(cache_key)

        if not message:
            # Parse Chat/Channel ID for Telethon Bot API
            if raw_chat_id.startswith("-100"):
                peer = PeerChannel(int(raw_chat_id[4:]))
            elif raw_chat_id.startswith("-"):
                peer = PeerChat(int(raw_chat_id[1:]))
            else:
                peer = PeerUser(int(raw_chat_id))

            try:
                message = await bot.get_messages(peer, ids=message_id)
            except Exception:
                try:
                    entity = await bot.get_entity(int(raw_chat_id))
                    message = await bot.get_messages(entity, ids=message_id)
                except Exception as err:
                    return web.Response(text=f"Fetch Error: {str(err)}", status=404)

        if not message or not message.media:
            return web.Response(text="Media expired or invalid message ID", status=404)

        media = message.video or message.document
        if not media:
            return web.Response(text="No video stream found", status=400)

        file_size = media.size
        mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"

        # Byte Range Handling for VLC Seeking
        range_header = request.headers.get("Range")
        start = 0
        end = file_size - 1

        if range_header:
            bytes_range = range_header.replace("bytes=", "").split("-")
            start = int(bytes_range[0])
            if len(bytes_range) > 1 and bytes_range[1]:
                end = int(bytes_range[1])

        content_length = (end - start) + 1

        response = web.StreamResponse(
            status=206 if range_header else 200,
            headers={
                "Content-Type": mime_type,
                "Content-Length": str(content_length),
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            },
        )

        await response.prepare(request)

        async for chunk in bot.iter_download(media, offset=start, request_size=1024 * 1024):
            if len(chunk) > (end - start + 1):
                chunk = chunk[: end - start + 1]
            await response.write(chunk)
            start += len(chunk)
            if start > end:
                break

        return response

    except Exception as e:
        return web.Response(text=f"Streaming Error: {str(e)}", status=500)


@bot.on(events.NewMessage)
async def handle_video(event):
    if event.video or (event.document and event.document.mime_type and event.document.mime_type.startswith("video/")):
        chat_id = event.chat_id
        msg_id = event.message.id

        # Retain media reference in RAM cache
        cache_key = f"{chat_id}_{msg_id}"
        MEDIA_CACHE[cache_key] = event.message

        base_url = SERVER_URL if SERVER_URL else f"http://{event.host}"
        stream_url = f"{base_url}/stream/{chat_id}/{msg_id}"

        reply_text = f"🚀 **VLC Stream Link:**\n`{stream_url}`"
        await event.reply(reply_text)


async def main():
    await bot.start(bot_token=BOT_TOKEN)
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Engine Live on Port {PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
