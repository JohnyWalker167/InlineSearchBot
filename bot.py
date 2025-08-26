# =========================
# Imports
# =========================
import asyncio
from bson import ObjectId
import os
import re
import sys
from datetime import datetime, timezone
from collections import defaultdict

from pyrogram import Client, enums, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import ListenerTimeout
import uvicorn
from pyrogram.types import InlineQueryResultCachedDocument, InlineQueryResultArticle, InputTextMessageContent

from config import *
from utility import (
    add_user, is_token_valid, authorize_user, is_user_authorized,
    generate_token, shorten_url, get_token_link, extract_channel_and_msg_id,
    safe_api_call, get_allowed_channels, invalidate_search_cache,
    auto_delete_message, human_readable_size,
    queue_file_for_processing, file_queue_worker,
    file_queue, extract_tmdb_link, periodic_expiry_cleanup,
    restore_tmdb_photos, build_search_pipeline,
    get_user_link, delete_after_delay)
from db import (db, users_col, 
                tokens_col, 
                files_col, 
                allowed_channels_col, 
                auth_users_col,
                tmdb_col
                )

from fast_api import api
from tmdb import get_by_id
import logging
# =========================
# Constants & Globals
# ========================= 

TOKEN_VALIDITY_SECONDS = 24 * 60 * 60  # 24 hours token validity
MAX_FILES_PER_SESSION = 10           # Max files a user can access per session
PAGE_SIZE = 10  # Number of files per page
SEARCH_PAGE_SIZE = 10  # You can adjust this

# Initialize Pyrogram bot client
bot = Client(
    "bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    parse_mode=enums.ParseMode.HTML
)

# Track how many files each user has accessed in the current session
user_file_count = defaultdict(int)
copy_lock = asyncio.Lock()

if "file_name_text" not in [idx["name"] for idx in files_col.list_indexes()]:
    files_col.create_index([("file_name", "text")])


def sanitize_query(query):
    """Sanitizes and normalizes a search query for consistent matching of 'and' and '&'."""
    query = query.strip().lower()
    # Replace all '&' with 'and'
    query = re.sub(r"\s*&\s*", " and ", query)
    # Replace multiple spaces and limit length
    query = re.sub(r"[:',]", "", query)
    query = re.sub(r"[.\s_\-\(\)\[\]]+", " ", query).strip()
    return query

# =========================
# Bot Command Handlers
# =========================

@bot.on_message(filters.command("start"))
async def start_handler(client, message):
    """
    Handles the /start command.
    - Registers the user.
    - Handles token-based authorization.
    - Handles file access via deep link.
    - Sends a greeting if no special argument is provided.
    - Deletes every message sent and received, but only once after all tasks are done.
    """
    reply_msg = None  
    try: 
        user_id = message.from_user.id
        user_link = await get_user_link(message.from_user) 
        add_user(user_id)
        # --- Token-based authorization ---
        if len(message.command) == 2 and message.command[1].startswith("token_"):
            if is_token_valid(message.command[1][6:], user_id):
                authorize_user(user_id)
                reply_msg = await safe_api_call(message.reply_text("✅ You are now authorized to access files for 24 hours."))
                await safe_api_call(bot.send_message(LOG_CHANNEL_ID, f"✅ User <b>{user_link}</b> authorized via token."))
            else:
                reply_msg = await safe_api_call(message.reply_text("❌ Invalid or expired token. Please get a new link."))

        #-- Get Unlock link ---
        elif len(message.command) == 2 and message.command[1].startswith("unlock"):
            if not is_user_authorized(user_id):
                now = datetime.now(timezone.utc)
                token_doc = tokens_col.find_one({
                    "user_id": user_id,
                    "expiry": {"$gt": now}
                })
                token_id = token_doc["token_id"] if token_doc else generate_token(user_id)
                short_link = shorten_url(get_token_link(token_id, BOT_USERNAME))
                reply_msg = await safe_api_call(
                    message.reply_text(
                        f"🔓 Hello {user_link}!\n\n"
                        "To access files, please contribute a little by clicking the button below. "
                        "It’s completely free for you — and it helps keep the bot running by supporting the server costs. ❤️\n\n"
                        "Click below to get 24-hour access:",
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [InlineKeyboardButton("🔓 Get Unlock Link", url=short_link)]
                            ]
                        )
                    )
                )

        #-- Get Limit Info ---
        elif len(message.command) == 2 and message.command[1].startswith("limit"):
            if is_user_authorized(user_id):
                reply_msg = await safe_api_call(
                    message.reply_text(
                        f"✅ Hello {user_link}!\n\n"
                        f"You are authorized to access files for the next 24 hours.\n\n"
                        f"However, you have reached the maximum of {MAX_FILES_PER_SESSION} files per session. Try again later."
                    )
                )

        # --- Get Search info ---
        elif len(message.command) == 2 and message.command[1].startswith("help"):
            if is_user_authorized(user_id):
                reply_msg = await safe_api_call(
                    message.reply_text(
                    f"𝐇𝖾𝗒 {user_link} ✨ \n\n"
                    "𝐈. 𝐇ⱺω 𝗍ⱺ 𝐒𝖾α𝗋𝖼ɦ 𝐅ⱺ𝗋 𝐌𝐎𝐕𝐈𝐄𝐒: \n\n"
                    "1. 𝐌ⱺ𝗏𝗂𝖾 𝐍αꭑ𝖾\n"
                    "𝐄𝗑αꭑρᥣ𝖾: movie Man of Steel\n"
                    "(𝐌α𝗄𝖾 𝗌υ𝗋𝖾 𝗍ⱺ 𝐂ɦ𝖾𝖼𝗄 𝗍ɦ𝖾 𝐒ρ𝖾ᥣᥣ𝗂𐓣𝗀 ⱺ𐓣 𝐆ⱺⱺ𝗀ᥣ𝖾)\n"
                    "𝐈𝐈. 𝐇ⱺω 𝗍ⱺ 𝐒𝖾α𝗋𝖼ɦ 𝐅ⱺ𝗋 𝐓𝐕-𝐒𝐄𝐑𝐈𝐄𝐒:\n\n"
                    "1. show 𝐍αꭑ𝖾 + 𝐒𝖾α𝗌ⱺ𐓣 𝐍υꭑᑲ𝖾𝗋\n"
                    "𝐄𝗑αꭑρᥣ𝖾:  show WandaVision S01\n"
                    "(𝐓ɦ𝗂𝗌 ω𝗂ᥣᥣ 𝐒ɦⱺω 𝐑𝖾𝗌υᥣ𝗍𝗌 𝖿ⱺ𝗋 𝐒𝖾α𝗌ⱺ𐓣 1 Pack available)\n"
                    "2. 𝐅ⱺ𝗋 𝐒ρ𝖾𝖼𝗂𝖿𝗂𝖼 𝐄ρ𝗂𝗌ⱺᑯ𝖾𝗌:\n"
                    "𝐀ᑯᑯ \"𝐄\" 𝐅ⱺᥣᥣⱺω𝖾ᑯ ᑲ𝗒 𝗍ɦ𝖾 𝐄ρ𝗂𝗌ⱺᑯ𝖾 𝐍υꭑᑲ𝖾𝗋.\n"
                    "𝐄𝗑αꭑρᥣ𝖾:  show WandaVision S02E01\n"
                    "(𝐓ɦ𝗂𝗌 𝗂𝗌 𝖿ⱺ𝗋 𝐒𝖾α𝗌ⱺ𐓣 2, 𝐄ρ𝗂𝗌ⱺᑯ𝖾 1)\n"
                    "(𝐌α𝗄𝖾 𝐒υ𝗋𝖾 𝗍ⱺ 𝐂ɦ𝖾𝖼𝗄 𝗍ɦ𝖾 𝐒ρ𝖾ᥣᥣ𝗂𐓣𝗀 ⱺ𐓣 𝐆ⱺⱺ𝗀ᥣ𝖾)\n\n"
                    "🎥  𝐑𝖾𝖼ⱺꭑꭑ𝖾𐓣ᑯ𝖾ᑯ 𝐕𝗂ᑯ𝖾ⱺ 𝐏ᥣα𝗒𝖾𝗋𝗌:\n"
                    "𝐅ⱺ𝗋 𝗍ɦ𝖾 ᑲ𝖾𝗌𝗍 ρᥣα𝗒ᑲα𝖼𝗄 𝖾𝗑ρ𝖾𝗋𝗂𝖾𐓣𝖼𝖾, υ𝗌𝖾: 𝐕𝐋𝐂 𝐌𝖾ᑯ𝗂α 𝐏ᥣα𝗒𝖾𝗋, 𝐌𝐏𝐕 𝐏ᥣα𝗒𝖾𝗋, 𝐌𝐗 𝐏ᥣα𝗒𝖾𝗋 𝐏𝗋ⱺ... \n\n"
                    f"❓ What's available? Check <a href='{UPDATE_CHANNEL_LINK}'>here</a>.</b>"
                    )
                )

        # --- Default greeting ---
        else:
            # Build buttons for each allowed channel
            allowed_channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_name": 1}))
            buttons = [
                [InlineKeyboardButton(f"🔎 {c['channel_name']}", switch_inline_query_current_chat=f"{c['channel_name']} ")]
                for c in allowed_channels if c.get("channel_name")
            ]
            if not buttons:
                buttons = [[InlineKeyboardButton("🕵️ Search", switch_inline_query_current_chat="")]]
            welcome_text = (
                f"👋 <b>🔰 Hello {user_link}! 🔰\n\n"
                f"Nice to meet you, my dear friend! 🤗\n\n"
                f"I can help you search media on telegram archives.\n\n"
                f"❤️ Enjoy your experience here! ❤️\n\n"
            )
            reply_msg = await safe_api_call(
                message.reply_text(
                    welcome_text,
                    reply_markup=InlineKeyboardMarkup(buttons),
                    parse_mode=enums.ParseMode.HTML
                )
            )
    except Exception as e:
        reply_msg = await safe_api_call(message.reply_text(f"⚠️ An unexpected error occurred: {e}"))

    if reply_msg:
        bot.loop.create_task(auto_delete_message(message, reply_msg))

@bot.on_message(filters.channel & (filters.document | filters.video | filters.audio | filters.photo))
async def channel_file_handler(client, message):
    allowed_channels = await get_allowed_channels()
    if message.chat.id not in allowed_channels:
        return
    await queue_file_for_processing(message, reply_func=message.reply_text)
    await file_queue.join()
    invalidate_search_cache()

@bot.on_message(filters.command("index") & filters.private & filters.user(OWNER_ID))
async def index_channel_files(client, message):
    """
    Handles the /index command for the owner.
    - Supports optional 'dup' flag.
    """
    # Support both `/index` and `/index dup`
    dup = False
    if len(message.command) > 1 and message.command[1].lower() == "dup":
        dup = True

    prompt = await safe_api_call(message.reply_text("Please send the **start file link** (Telegram message link, only /c/ links supported):"))
    try:
        start_msg = await client.listen(message.chat.id, timeout=120)
    except ListenerTimeout:
        await safe_api_call(prompt.edit_text("⏰ Timeout! You took too long to reply. Please try again."))
        return
    start_link = start_msg.text.strip()

    prompt2 = await safe_api_call(message.reply_text("Now send the **end file link** (Telegram message link, only /c/ links supported):"))
    try:
        end_msg = await client.listen(message.chat.id, timeout=120)
    except ListenerTimeout:
        await safe_api_call(prompt2.edit_text("⏰ Timeout! You took too long to reply. Please try again."))
        return
    end_link = end_msg.text.strip()

    try:
        start_id, start_msg_id = extract_channel_and_msg_id(start_link)
        end_id, end_msg_id = extract_channel_and_msg_id(end_link)

        if start_id != end_id:
            await message.reply_text("Start and end links must be from the same channel.")
            return

        channel_id = start_id
        allowed_channels = await get_allowed_channels()
        if channel_id not in allowed_channels:
            await message.reply_text("❌ This channel is not allowed for indexing.")
            return

        if start_msg_id > end_msg_id:
            start_msg_id, end_msg_id = end_msg_id, start_msg_id

    except Exception as e:
        await message.reply_text(f"Invalid link: {e}")
        return

    reply = await message.reply_text(f"Indexing files from {start_msg_id} to {end_msg_id} in channel {channel_id}... Duplicates allowed: {dup}")

    batch_size = 50
    total_queued = 0
    for batch_start in range(start_msg_id, end_msg_id + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_msg_id)
        ids = list(range(batch_start, batch_end + 1))
        try:
            messages = []
            for msg_id in ids:
                msg = await safe_api_call(client.get_messages(channel_id, msg_id))
                messages.append(msg)
        except Exception as e:
            await message.reply_text(f"Failed to get messages {batch_start}-{batch_end}: {e}")
            continue
        for msg in messages:
            if not msg:
                continue
            if msg.document or msg.video or msg.audio or msg.photo:
                await queue_file_for_processing(
                    msg,
                    channel_id=channel_id,
                    reply_func=reply.edit_text,
                    duplicate=dup      # Pass the flag here!
                )
                total_queued += 1
        invalidate_search_cache()

    await message.reply_text(f"✅ Queued {total_queued} files from channel {channel_id} for processing. Duplicates allowed: {dup}")


@bot.on_message(filters.private & filters.command("del") & filters.user(OWNER_ID))
async def delete_command(client, message):
    try:
        args = message.text.split(maxsplit=3)
        reply = None
        if len(args) < 3:
            reply = await message.reply_text("Usage: /del <file|tmdb> <link> [end_link]")
            return
        delete_type = args[1].strip().lower()
        user_input = args[2].strip()
        end_input = args[3].strip() if len(args) > 3 else None

        if delete_type == "file":
            try:
                channel_id, msg_id = extract_channel_and_msg_id(user_input)
                if end_input:
                    end_channel_id, end_msg_id = extract_channel_and_msg_id(end_input)
                    if channel_id != end_channel_id:
                        await message.reply_text("Start and end links must be from the same channel.")
                        return
                    if msg_id > end_msg_id:
                        msg_id, end_msg_id = end_msg_id, msg_id
                    # Delete in range
                    result = files_col.delete_many({
                        "channel_id": channel_id,
                        "message_id": {"$gte": msg_id, "$lte": end_msg_id}
                    })
                    reply = await message.reply_text(f"Deleted {result.deleted_count} files from {msg_id} to {end_msg_id} in channel {channel_id}.")
                    return
            except Exception as e:
                await message.reply_text(f"Error: {e}")
                return
            # Single file delete
            file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
            if not file_doc:
                reply = await message.reply_text("No file found with that link in the database.")
                return
            result = files_col.delete_one({"channel_id": channel_id, "message_id": msg_id})
            if result.deleted_count > 0:
                reply = await message.reply_text(f"Database record deleted. File name: {file_doc.get('file_name')}\n({user_input})")
            else:
                reply = await message.reply_text(f"No file found with File name: {file_doc.get('file_name')}")
        elif delete_type == "tmdb":
            try:
                tmdb_type, tmdb_id = await extract_tmdb_link(user_input)
            except Exception as e:
                reply = await message.reply_text(f"Error: {e}")
                return
            result = tmdb_col.delete_one({"tmdb_type": tmdb_type, "tmdb_id": tmdb_id})
            if result.deleted_count > 0:
                reply = await message.reply_text(f"Database record deleted {tmdb_type}/{tmdb_id}.")
            else:
                reply = await message.reply_text(f"No TMDB record found with ID {tmdb_type}/{tmdb_id} in the database.")
        else:
            reply = await message.reply_text("Invalid delete type. Use 'file' or 'tmdb'.")
        if reply:
            bot.loop.create_task(auto_delete_message(message, reply))
    except Exception as e:
        await message.reply_text(f"Error: {e}")
                                 
@bot.on_message(filters.command('restart') & filters.private & filters.user(OWNER_ID))
async def restart(client, message):
    """
    Handles the /restart command for the owner.
    - Deletes the log file, runs update.py, and restarts the bot.
    """    
    log_file = "bot_log.txt"
    if os.path.exists(log_file):
        try:
            os.remove(log_file)
        except Exception as e:
            await safe_api_call(message.reply_text(f"Failed to delete log file: {e}"))
    os.system("python3 update.py")
    os.execl(sys.executable, sys.executable, "bot.py")

@bot.on_message(filters.private & filters.command("restore") & filters.user(OWNER_ID))
async def update_info(client, message):
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("Usage: /restore tmdb [start_objectid]")
            return
        restore_type = args[1].strip()
        start_id = args[2] if len(args) > 2 else None
        if start_id:
            try:
                start_id = ObjectId(start_id)
            except Exception:
                await message.reply_text("Invalid ObjectId format for start_id.")
                return
        if restore_type == "tmdb":
            await restore_tmdb_photos(bot, start_id)
        else:
            await message.reply_text("Invalid restore type. Use 'tmdb'.")
    except Exception as e:
        await message.reply_text(f"Error in Update Command: {e}")
        

@bot.on_message(filters.command("add") & filters.private & filters.user(OWNER_ID))
async def add_channel_handler(client, message: Message):
    """
    Handles the /add command for the owner.
    - Adds a channel to the allowed channels list in the database.
    """
    if len(message.command) < 3:
        await message.reply_text("Usage: /add channel_id channel_name")
        return
    try:
        channel_id = int(message.command[1])
        channel_name = " ".join(message.command[2:])
        allowed_channels_col.update_one(
            {"channel_id": channel_id},
            {"$set": {"channel_id": channel_id, "channel_name": channel_name}},
            upsert=True
        )
        await message.reply_text(f"✅ Channel {channel_id} ({channel_name}) added to allowed channels.")
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("rm") & filters.private & filters.user(OWNER_ID))
async def remove_channel_handler(client, message: Message):
    """
    Handles the /rm command for the owner.
    - Removes a channel from the allowed channels list in the database.
    """
    if len(message.command) != 2:
        await message.reply_text("Usage: /rm channel_id")
        return
    try:
        channel_id = int(message.command[1])
        result = allowed_channels_col.delete_one({"channel_id": channel_id})
        if result.deleted_count:
            await message.reply_text(f"✅ Channel {channel_id} removed from allowed channels.")
        else:
            await message.reply_text("❌ Channel not found in allowed channels.")
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("broadcast") & filters.private & filters.user(OWNER_ID))
async def broadcast_handler(client, message: Message):
    """
    Handles the /broadcast command for the owner.
    - If replying to a message, copies that message to all users.
    - Otherwise, broadcasts a text message.
    - Removes users from DB if any exception occurs during message send.
    """
    if message.reply_to_message:
        users = users_col.find({}, {"_id": 0, "user_id": 1})
        total = 0
        failed = 0
        removed = 0

        for user in users:
             try:
                await asyncio.sleep(3)  # Rate limit
                await safe_api_call(message.reply_to_message.copy(user["user_id"]))
                total += 1
             except Exception:
                failed += 1
                users_col.delete_one({"user_id": user["user_id"]})
                removed += 1
                continue
             await asyncio.sleep(3)

        await message.reply_text(f"✅ Broadcasted to {total} users.\n❌ Failed: {failed}\n🗑️ Removed: {removed}")
                                                                                                

@bot.on_message(filters.command("log") & filters.private & filters.user(OWNER_ID))
async def send_log_file(client, message: Message):
    """
    Handles the /log command for the owner.
    - Sends the bot.log file to the owner.
    """
    log_file = "bot_log.txt"
    if not os.path.exists(log_file):
        await safe_api_call(message.reply_text("Log file not found."))
        return
    try:
        await safe_api_call(client.send_document(message.chat.id, log_file, caption="Here is the log file."))
    except Exception as e:
        await safe_api_call(message.reply_text(f"Failed to send log file: {e}"))

@bot.on_message(filters.command("stats") & filters.private & filters.user(OWNER_ID))
async def stats_command(client, message: Message):
    """Show statistics including per-channel file counts (OWNER only)."""
    try:
        total_auth_users = auth_users_col.count_documents({})
        total_users = users_col.count_documents({})

        # Total file storage size
        pipeline = [
            {"$group": {"_id": None, "total": {"$sum": "$file_size"}}}
        ]
        result = list(files_col.aggregate(pipeline))
        total_storage = result[0]["total"] if result else 0

        # Database storage size
        stats = db.command("dbstats")
        db_storage = stats.get("storageSize", 0)

        # Per-channel counts
        channel_pipeline = [
            {"$group": {"_id": "$channel_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        channel_counts = list(files_col.aggregate(channel_pipeline))
        channel_docs = allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1})
        channel_names = {c["channel_id"]: c.get("channel_name", "") for c in channel_docs}

        # Compose stats message
        text = (
            f"👤 <b>Total auth users:</b> {total_auth_users} / {total_users}\n"
            f"💾 <b>Files size:</b> {human_readable_size(total_storage)}\n"
            f"📊 <b>Database storage used:</b> {db_storage / (1024 * 1024):.2f} MB\n"
        )

        if not channel_counts:
            text += " <b>No files indexed yet.</b>"
        else:
            for c in channel_counts:
                chan_id = c['_id']
                chan_name = channel_names.get(chan_id, 'Unknown')
                text += f"<b>{chan_name}</b>: {c['count']} files\n"

        reply = await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
        if reply:
            bot.loop.create_task(auto_delete_message(message, reply))
    except Exception as e:
        await message.reply_text(f"⚠️ An error occurred while fetching stats:\n<code>{e}</code>")

@bot.on_message(filters.private & filters.command("tmdb") & filters.user(OWNER_ID))
async def tmdb_command(client, message):
    try:
        if len(message.command) < 2:
            reply = await safe_api_call(message.reply_text("Usage: /tmdb tmdb_link"))
            await auto_delete_message(message, reply)
            return

        tmdb_link = message.command[1]
        tmdb_type, tmdb_id = await extract_tmdb_link(tmdb_link)
        result = await get_by_id(tmdb_type, tmdb_id)
        poster_url = result.get('poster_url')
        trailer = result.get('trailer_url')
        info = result.get('message')

        update = {
            "$setOnInsert": {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type}
        }
        tmdb_col.update_one(
            {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type},
            update,
            upsert=True
        )
        
        if poster_url:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎥 Trailer", url=trailer)]]) if trailer else None
            await safe_api_call(
                client.send_photo(
                    UPDATE_CHANNEL_ID,
                    photo=poster_url,
                    caption=info,
                    parse_mode=enums.ParseMode.HTML,
                    reply_markup=keyboard
                )
            )
    except Exception as e:
        logging.exception("Error in tmdb_command")
        await safe_api_call(message.reply_text(f"Error in tmdb command: {e}"))
    await message.delete()

@bot.on_inline_query()
async def inline_query_handler(client, inline_query):
    raw_query = inline_query.query.strip()
    user_id = inline_query.from_user.id
    results = []

    await asyncio.sleep(3)

    if not is_user_authorized(user_id):
        await inline_query.answer(
            results=[],
            cache_time=0,
            switch_pm_text="Click here to get your unlock link.",
            switch_pm_parameter="unlock"
        )
        return

    if user_file_count[user_id] >= MAX_FILES_PER_SESSION:
        await inline_query.answer(
            results=[],
            cache_time=0,
            switch_pm_text="⚠️Your Limit Reached. Try again later.",
            switch_pm_parameter="limit"
        )
        return

    if not raw_query:
        await inline_query.answer(
            results=[],
            cache_time=0,
            switch_pm_text="Enter valid query tap here to know more.",
            switch_pm_parameter="help"
        )
        return

    try:
        offset = int(inline_query.offset) if inline_query.offset else 0
    except Exception:
        offset = 0

    # Fetch allowed channels with names
    channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1}))

    # Try to match the first word as a channel name (partial, case-insensitive)
    parts = raw_query.split(maxsplit=1)
    if len(parts) < 2:
        # If no channel name and query, do not search at all
        await inline_query.answer(
            results=[],
            cache_time=0,
            switch_pm_text="Enter valid query tap here to know more.",
            switch_pm_parameter="help"
        )
        return

    first_word = parts[0].lower()
    rest_query = parts[1].strip()

    matched_channels = [
        c for c in channels if first_word in c.get("channel_name", "").lower()
    ]
    if not matched_channels:
        await inline_query.answer(
            results=[],
            cache_time=0,
            switch_pm_text=f"No channel found matching '{first_word}'.",
            switch_pm_parameter="help"
        )
        return

    channel_ids = [c["channel_id"] for c in matched_channels]
    search_query = sanitize_query(rest_query)

    pipeline = build_search_pipeline(search_query, channel_ids, offset, SEARCH_PAGE_SIZE)
    result = list(files_col.aggregate(pipeline))
    files = result[0]["results"] if result and result[0]["results"] else []
    total_count = result[0]["totalCount"][0]["total"] if result and result[0]["totalCount"] else 0

    search_button = InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"🔎 Search: {search_query}", switch_inline_query_current_chat=raw_query)]]
    )

    results = [
        InlineQueryResultCachedDocument(
            title=f.get("file_name", "File"),
            document_file_id=f.get("file_id"),
            description=f"Size: {human_readable_size(f.get('file_size', 0))}",
            caption=f"<b>{f.get('file_name', 'File')}</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=search_button
        )
        for f in files
    ]

    next_offset = str(offset + SEARCH_PAGE_SIZE) if (offset + SEARCH_PAGE_SIZE) < total_count else ""

    await inline_query.answer(
        results,
        cache_time=0,
        next_offset=next_offset,
        switch_pm_text=f"Result for {search_query}" if results else "Enter valid query tap here to know more.",
        switch_pm_parameter="start" if results else "help"
    )
    return

@bot.on_message(filters.via_bot)
async def private_file_handler(client, message: Message):
        bot.loop.create_task(delete_after_delay(message))
        return

@bot.on_chosen_inline_result()
async def chosen_result_handler(client, chosen_result):
    user_id = chosen_result.from_user.id

    # Increment counter only when a result is chosen
    user_file_count[user_id] += 1
    return

@bot.on_message(filters.command("chatop") & filters.private & filters.user(OWNER_ID))
async def chatop_handler(client, message: Message):
    """
    Usage:
      /chatop send <chat_id> (reply to a message to send)
      /chatop del <chat_id> <message_id>
    """
    args = message.text.split(maxsplit=3)
    if len(args) < 3:
        await message.reply_text("Usage:\n/chatop send <chat_id> (reply to a message)\n/chatop del <chat_id> <message_id>")
        return
    op = args[1].lower()
    chat_id = args[2]
    if op == "send":
        if not message.reply_to_message:
            await message.reply_text("Reply to a message to send it.\nUsage: /chatop send <chat_id> (reply to a message)")
            return
        try:
            sent = await message.reply_to_message.copy(int(chat_id))
            await message.reply_text(f"✅ Sent to {chat_id} (message_id: {sent.id})")
        except Exception as e:
            await message.reply_text(f"❌ Failed: {e}")
    elif op == "del":
        if len(args) != 4:
            await message.reply_text("Usage: /chatop del <chat_id> <message_id>")
            return
        try:
            await client.delete_messages(int(chat_id), int(args[3]))
            await message.reply_text(f"✅ Deleted message {args[3]} in chat {chat_id}")
        except Exception as e:
            await message.reply_text(f"❌ Failed: {e}")
    else:
        await message.reply_text("Invalid operation. Use 'send' or 'del'.")
        
# =========================
# Main Entrypoint
# =========================

async def main():
    """
    Starts the bot and FastAPI server.
    """
    # Set bot commands
    await bot.start()

    bot.loop.create_task(start_fastapi())
    bot.loop.create_task(file_queue_worker(bot))  # Start the queue worker
    bot.loop.create_task(periodic_expiry_cleanup())

    # Send startup message to log channel
    try:
        me = await bot.get_me()
        user_name = me.username or "Bot"
        await bot.send_message(LOG_CHANNEL_ID, f"✅ @{user_name} started and FastAPI server running.")
        logger.info("Bot started and FastAPI server running.")
    except Exception as e:
        print(f"Failed to send startup message to log channel: {e}")

async def start_fastapi():
    """
    Starts the FastAPI server using Uvicorn.
    """
    try:
        config = uvicorn.Config(api, host="0.0.0.0", port=8000, loop="asyncio", log_level="warning")
        server = uvicorn.Server(config)
        await server.serve()
    except KeyboardInterrupt:
        pass
        logger.info("FastAPI server stopped.")

if __name__ == "__main__":
    """
    Main process entrypoint.
    - Runs the bot and FastAPI server.
    - Handles graceful shutdown on KeyboardInterrupt.
    """
    try:
        bot.loop.run_until_complete(main())
        bot.loop.run_forever()
    except KeyboardInterrupt:
        bot.stop()
        tasks = asyncio.all_tasks(loop=bot.loop)
        for task in tasks:
            task.cancel()
        bot.loop.stop()
        logger.info("Bot stopped.")