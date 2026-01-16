import os
import asyncio
import secrets
import sqlite3
from contextlib import closing

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart

BOT_TOKEN = os.getenv("BOT_TOKEN", "8495830935:AAFQP9hOq31jFUdvTZs4YGQlEdJM_S05uq8").strip()
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003642090936").strip())  # contoh: -1003642090936
BOT_USERNAME = os.getenv("BOT_USERNAME", "hepini_storage_bot").strip().lstrip("@")  # contoh: bico_storage_bot

# Owner IDs (comma-separated), contoh: "5577603728,6016383456"
OWNER_IDS = set()
_raw_owner = os.getenv("OWNER_IDS", "5577603728,6016383456").strip()
for part in _raw_owner.split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

DB_PATH = os.getenv("DB_PATH", "files.db").strip()  # default local sqlite

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set")
if not BOT_USERNAME:
    print("PERINGATAN: BOT_USERNAME belum di-set. Link share tidak akan dibuat otomatis.")

def make_slug() -> str:
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:12]

def db_init():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
          slug TEXT PRIMARY KEY,
          channel_id INTEGER NOT NULL,
          channel_message_id INTEGER NOT NULL,
          uploaded_by INTEGER NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)
        conn.commit()

def db_put(slug: str, channel_id: int, channel_message_id: int, uploaded_by: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT INTO files (slug, channel_id, channel_message_id, uploaded_by) VALUES (?,?,?,?)",
            (slug, channel_id, channel_message_id, uploaded_by),
        )
        conn.commit()

def db_get(slug: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute(
            "SELECT channel_id, channel_message_id FROM files WHERE slug = ?",
            (slug,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]), int(row[1])

async def main():
    db_init()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start_handler(message: Message):
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) == 1:
            await message.answer(
                "📦 Kirim file ke bot ini (khusus owner).\n"
                "Kalau kamu punya link file, buka dari link tersebut ya."
            )
            return

        slug = parts[1].strip()
        found = db_get(slug)
        if not found:
            await message.answer("❌ File tidak ditemukan / link sudah tidak valid.")
            return

        ch_id, ch_msg_id = found
        try:
            await bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=ch_id,
                message_id=ch_msg_id,
            )
        except Exception as e:
            await message.answer(f"❌ Gagal mengirim file. ({type(e).__name__})")

    @dp.message(
        F.content_type.in_({"document", "video", "audio", "voice", "photo", "animation", "sticker"})
        | F.video_note
    )
    async def upload_handler(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if uid not in OWNER_IDS:
            await message.answer("⛔ Kamu tidak punya akses upload.")
            return

        # 1) copy ke channel DB (storage)
        try:
            copied = await bot.copy_message(
                chat_id=CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception as e:
            await message.answer(f"❌ Gagal menyimpan ke channel DB. ({type(e).__name__})")
            return

        # 2) simpan mapping slug -> message_id channel
        slug = make_slug()
        try:
            db_put(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))
        except sqlite3.IntegrityError:
            # sangat jarang, tapi kalau slug tabrakan, coba ulang sekali
            slug = make_slug()
            db_put(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))

        # 3) balas link publik
        if BOT_USERNAME:
            link = f"https://t.me/{BOT_USERNAME}?start={slug}"
            await message.answer(f"✅ Tersimpan!\n🔗 Link publik:\n{link}")
        else:
            await message.answer(f"✅ Tersimpan!\nSlug: {slug}\nSet ENV BOT_USERNAME biar jadi link t.me otomatis.")

    @dp.message()
    async def fallback(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if uid in OWNER_IDS:
            await message.answer("Kirim file (document/video/audio/photo) untuk disimpan.")
        else:
            await message.answer("Buka link file yang kamu punya ya (t.me/<bot>?start=...).")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
