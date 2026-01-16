import os
import asyncio
import secrets
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart
import asyncpg

BOT_TOKEN = os.getenv("BOT_TOKEN", "8527557467:AAFL4iWJGClLshtfHj1W1GfAKbS3CclaH08").strip()
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003642090936").strip())  # contoh: -1003642090936
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:MJyOIpRWztHmdzSueQuMGTJGuMUGILQz@trolley.proxy.rlwy.net:27772/railway").strip()   # dari Railway Postgres
BOT_USERNAME = os.getenv("BOT_USERNAME", "hepini_file_bot").strip().lstrip("@")  # contoh: bico_storage_bot

# Owner IDs (comma-separated), contoh: "5577603728,6016383456"
OWNER_IDS = set()
_raw_owner = os.getenv("OWNER_IDS", "5577603728").strip()
for part in _raw_owner.split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL belum di-set (pakai Railway Postgres)")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set")
if not BOT_USERNAME:
    # tetap bisa jalan, tapi link start tidak bisa dibuat otomatis tanpa username
    print("PERINGATAN: BOT_USERNAME belum di-set. Link share tidak akan dibuat.")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS files (
  slug TEXT PRIMARY KEY,
  channel_id BIGINT NOT NULL,
  channel_message_id BIGINT NOT NULL,
  uploaded_by BIGINT NOT NULL,
  original_chat_id BIGINT NOT NULL,
  original_message_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO files (slug, channel_id, channel_message_id, uploaded_by, original_chat_id, original_message_id)
VALUES ($1, $2, $3, $4, $5, $6);
"""

GET_SQL = """
SELECT channel_id, channel_message_id
FROM files
WHERE slug = $1;
"""

def make_slug() -> str:
    # pendek tapi cukup aman untuk dibagi publik
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:12]

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)

    @dp.message(CommandStart())
    async def start_handler(message: Message, command: CommandStart):
        args = (message.text or "").split(maxsplit=1)
        if len(args) == 1:
            await message.answer(
                "📦 Kirim file ke bot ini (khusus owner). "
                "Kalau kamu punya link file, buka dari link tersebut ya."
            )
            return

        slug = args[1].strip()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(GET_SQL, slug)

        if not row:
            await message.answer("❌ File tidak ditemukan / link sudah tidak valid.")
            return

        ch_id = int(row["channel_id"])
        ch_msg_id = int(row["channel_message_id"])

        try:
            # Copy file dari channel database ke user yang request
            await bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=ch_id,
                message_id=ch_msg_id,
            )
        except Exception as e:
            await message.answer(f"❌ Gagal mengirim file. ({type(e).__name__})")

    @dp.message(F.content_type.in_({
        "document", "video", "audio", "voice", "photo", "animation", "sticker"
    }) | F.video_note)
    async def upload_handler(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if uid not in OWNER_IDS:
            await message.answer("⛔ Kamu tidak punya akses upload.")
            return

        try:
            # simpan ke channel database (storage)
            copied = await bot.copy_message(
                chat_id=CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception as e:
            await message.answer(f"❌ Gagal menyimpan ke channel DB. ({type(e).__name__})")
            return

        slug = make_slug()

        async with pool.acquire() as conn:
            await conn.execute(
                INSERT_SQL,
                slug,
                int(CHANNEL_ID),
                int(copied.message_id),
                int(uid),
                int(message.chat.id),
                int(message.message_id),
            )

        if BOT_USERNAME:
            link = f"https://t.me/{BOT_USERNAME}?start={slug}"
            await message.answer(f"✅ Tersimpan!\n🔗 Link publik:\n{link}")
        else:
            await message.answer(
                f"✅ Tersimpan!\nSlug: {slug}\n"
                "Set ENV BOT_USERNAME supaya bot bisa bikin link t.me otomatis."
            )

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
