import os
import asyncio
import time

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import Message

import psycopg
from psycopg.rows import tuple_row
from dotenv import load_dotenv

# =========================
# LOAD CONFIG
# =========================
load_dotenv("config.env")

BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
DATABASE_URL = os.getenv("DATABASE_URL")

OWNER_IDS = {
    int(x.strip())
    for x in os.getenv("OWNER_IDS", "").split(",")
    if x.strip()
}

BROADCAST_RATE = float(os.getenv("BROADCAST_RATE", "25"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set di config.env")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL belum di-set di config.env")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set di config.env")

# =========================
# DATABASE SQL
# =========================
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS subscribers (
  chat_id BIGINT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT = """
INSERT INTO subscribers (chat_id, username, first_name, last_name)
VALUES (%s, %s, %s, %s)
ON CONFLICT (chat_id)
DO UPDATE SET
  username = EXCLUDED.username,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name;
"""

SELECT_ALL = "SELECT chat_id FROM subscribers ORDER BY created_at ASC;"
COUNT_ALL = "SELECT COUNT(*) FROM subscribers;"

# =========================
# UTIL
# =========================
def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.delay = 1 / rate_per_sec
        self.last = 0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            delta = self.delay - (now - self.last)
            if delta > 0:
                await asyncio.sleep(delta)
            self.last = time.monotonic()

# =========================
# DATABASE HELPERS
# =========================
async def db_exec(query, params=None):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
        await conn.commit()

async def db_fetchval(query):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute(query)
            row = await cur.fetchone()
            return row[0] if row else 0

async def db_iter_chat_ids(batch=1000):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute(SELECT_ALL)
            while True:
                rows = await cur.fetchmany(batch)
                if not rows:
                    break
                for (cid,) in rows:
                    yield int(cid)

# =========================
# MAIN BOT
# =========================
async def main():
    await db_exec(CREATE_TABLE)

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    limiter = RateLimiter(BROADCAST_RATE)

    @dp.message(CommandStart())
    async def start_cmd(msg: Message):
        u = msg.from_user
        await db_exec(
            UPSERT,
            (msg.chat.id, u.username if u else None, u.first_name if u else None, u.last_name if u else None)
        )
        await msg.answer("✅ Kamu sudah terdaftar.")

    @dp.message(Command("stats"))
    async def stats_cmd(msg: Message):
        if not msg.from_user or not is_owner(msg.from_user.id):
            return
        total = await db_fetchval(COUNT_ALL)
        await msg.answer(f"📊 Total user: {total}")

    @dp.message(Command("broadcast"))
    async def broadcast_cmd(msg: Message):
        if not msg.from_user or not is_owner(msg.from_user.id):
            return

        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            await msg.answer("Gunakan: /broadcast isi pesan")
            return

        text = parts[1]
        total = await db_fetchval(COUNT_ALL)
        await msg.answer(f"🚀 Broadcast ke {total} user dimulai...")

        sent = failed = 0

        async for chat_id in db_iter_chat_ids():
            await limiter.wait()
            try:
                await bot.send_message(chat_id, text)
                sent += 1
            except Exception as e:
                retry = getattr(e, "retry_after", None)
                if retry:
                    await asyncio.sleep(float(retry))
                    try:
                        await bot.send_message(chat_id, text)
                        sent += 1
                        continue
                    except Exception:
                        failed += 1
                else:
                    failed += 1

        await msg.answer(f"✅ Selesai\nTerkirim: {sent}\nGagal: {failed}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
