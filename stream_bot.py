import os
import asyncio
from aiohttp import web
from hydrogram import Client, filters

API_ID = int(os.environ.get("API_ID", "0").strip())
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SERVER_URL = os.environ.get("SERVER_URL", "").rstrip('/')
PORT = int(os.environ.get("PORT", 8080))

bot = Client(
    "stream_engine",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

routes = web.RouteTableDef()

@routes.get("/stream/{chat_id}/{message_id}")
async def stream_handler(request):
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])

        message = await bot.get_messages(chat_id, message_ids=message_id)
        if not message or not message.media:
            return web.Response(text="Media not found", status=404)

        media = getattr(message, message.media.value, None)
        if not media:
            return web.Response(text="Invalid Media", status=400)

        file_size = media.file_size
        mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"

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
            },
        )

        await response.prepare(request)

        async for chunk in bot.stream_media(message, offset=start // (1024 * 1024)):
            await response.write(chunk)

        return response

    except Exception as e:
        return web.Response(text=f"Stream Error: {str(e)}", status=500)


@bot.on_message(filters.video | filters.document)
async def handle_video(client, message):
    chat_id = message.chat.id
    msg_id = message.id

    base_url = SERVER_URL if SERVER_URL else "http://localhost:8080"
    stream_url = f"{base_url}/stream/{chat_id}/{msg_id}"

    reply_text = f"🚀 **Direct Stream Link:**\n`{stream_url}`"
    await message.reply_text(reply_text)


async def main():
    await bot.start()
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🤖 Engine Online on Port {PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
