import os
import asyncio
import secrets
import sqlite3
from contextlib import closing

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart

# =========================
# LOAD CONFIG
# =========================
load_dotenv("config.env")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
CHANNEL_ID_RAW = (os.getenv("CHANNEL_ID") or "").strip()
BOT_USERNAME = (os.getenv("BOT_USERNAME") or "").strip().lstrip("@")
OWNER_IDS_RAW = (os.getenv("OWNER_IDS") or "").strip()
DB_PATH = (os.getenv("DB_PATH") or "files.db").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set di config.env")
if not CHANNEL_ID_RAW:
    raise RuntimeError("CHANNEL_ID belum di-set di config.env")
if not OWNER_IDS_RAW:
    raise RuntimeError("OWNER_IDS belum di-set di config.env")

try:
    CHANNEL_ID = int(CHANNEL_ID_RAW)
except ValueError:
    raise RuntimeError("CHANNEL_ID harus angka (contoh: -1003642090936)")

OWNER_IDS = set()
for part in OWNER_IDS_RAW.split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

if not BOT_USERNAME:
    print("PERINGATAN: BOT_USERNAME belum di-set. Link share tidak akan dibuat otomatis.")

# =========================
# HELPERS
# =========================
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

# =========================
# BOT
# =========================
async def main():
    db_init()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start_handler(message: Message):
        parts = (message.text or "").split(maxsplit=1)

        # start biasa
        if len(parts) == 1:
            await message.answer(
                "📦 Kirim file ke bot ini (khusus owner).\n"
                "Kalau kamu punya link file, buka dari link tersebut ya."
            )
            return

        # start dengan slug (public access)
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
            slug = make_slug()
            db_put(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))

        # 3) balas link publik
        if BOT_USERNAME:
            link = f"https://t.me/{BOT_USERNAME}?start={slug}"
            await message.answer(f"✅ Tersimpan!\n🔗 Link publik:\n{link}")
        else:
            await message.answer(f"✅ Tersimpan!\nSlug: {slug}\nSet BOT_USERNAME di config.env biar jadi link t.me otomatis.")

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
