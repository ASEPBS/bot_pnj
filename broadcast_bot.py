import os
import asyncio
import time
from typing import Optional, Tuple

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import Message

import psycopg
from psycopg.rows import tuple_row

BOT_TOKEN = os.getenv("BOT_TOKEN", "8527557467:AAEGWLZkeMOFj9ICUUvk3kNm6A8U8ZRhuLc").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:MJyOIpRWztHmdzSueQuMGTJGuMUGILQz@trolley.proxy.rlwy.net:27772/railway").strip()

# contoh: "5577603728,6016383456"
OWNER_IDS = set()
for part in os.getenv("OWNER_IDS", "5577603728,6016383456").split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL belum di-set")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS subscribers (
  chat_id BIGINT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO subscribers (chat_id, username, first_name, last_name, last_seen)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (chat_id)
DO UPDATE SET
  username = EXCLUDED.username,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  last_seen = NOW();
"""

COUNT_SQL = "SELECT COUNT(*) FROM subscribers;"
SELECT_ALL_SQL = "SELECT chat_id FROM subscribers ORDER BY created_at ASC;"

def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS

class RateLimiter:
    """
    Global limiter: target ~30 msg/sec (Telegram broadcast limit ~30/s).
    We'll use ~25/s to be safe + still fast.
    """
    def __init__(self, per_sec: float = 25.0):
        self.min_interval = 1.0 / per_sec
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            wait_for = self.min_interval - (now - self._last)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last = time.monotonic()

async def ensure_db():
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
            await cur.execute(CREATE_TABLE_SQL)
        await conn.commit()

async def upsert_subscriber(chat_id: int, username: Optional[str], first_name: Optional[str], last_name: Optional[str]):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
            await cur.execute(UPSERT_SQL, (chat_id, username, first_name, last_name))
        await conn.commit()

async def get_subscriber_count() -> int:
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute(COUNT_SQL)
            row = await cur.fetchone()
            return int(row[0]) if row else 0

async def iter_chat_ids(batch_size: int = 1000):
    """
    Stream chat_ids without loading everything into memory.
    """
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute(SELECT_ALL_SQL)
            while True:
                rows = await cur.fetchmany(batch_size)
                if not rows:
                    break
                for (chat_id,) in rows:
                    yield int(chat_id)

async def main():
    await ensure_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    limiter = RateLimiter(per_sec=25.0)  # safe & fast

    @dp.message(CommandStart())
    async def on_start(message: Message):
        u = message.from_user
        if u:
            await upsert_subscriber(
                chat_id=message.chat.id,
                username=u.username,
                first_name=u.first_name,
                last_name=u.last_name,
            )
        await message.answer("✅ Bot aktif. Kamu sudah terdaftar untuk update/broadcast.")

    @dp.message(Command("stats"))
    async def stats(message: Message):
        if not message.from_user or not is_owner(message.from_user.id):
            return
        total = await get_subscriber_count()
        await message.answer(f"📊 Total subscriber tersimpan: {total}")

    @dp.message(Command("broadcast"))
    async def broadcast(message: Message):
        if not message.from_user or not is_owner(message.from_user.id):
            return

        text = (message.text or "").split(maxsplit=1)
        if len(text) < 2 or not text[1].strip():
            await message.answer("Cara pakai: /broadcast isi pesan kamu")
            return

        payload = text[1].strip()

        total = await get_subscriber_count()
        await message.answer(f"🚀 Mulai broadcast ke {total} user...")

        sent = 0
        failed = 0

        async for chat_id in iter_chat_ids(batch_size=1000):
            await limiter.wait()
            try:
                await bot.send_message(chat_id, payload)
                sent += 1
            except Exception as e:
                # Handle 429 (Too Many Requests) -> aiogram biasanya raise TelegramRetryAfter
                # Tapi agar robust, kita cek attribute retry_after jika ada.
                retry_after = getattr(e, "retry_after", None)
                if retry_after:
                    await asyncio.sleep(float(retry_after) + 0.5)
                    try:
                        await bot.send_message(chat_id, payload)
                        sent += 1
                        continue
                    except Exception:
                        failed += 1
                else:
                    failed += 1

            # Optional: progress tiap 500 kirim
            if (sent + failed) % 500 == 0:
                await message.answer(f"Progress: terkirim {sent}, gagal {failed} / {total}")

        await message.answer(f"✅ Broadcast selesai.\nTerkirim: {sent}\nGagal: {failed}\nTotal target: {total}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
