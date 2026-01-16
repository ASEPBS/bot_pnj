import os
import asyncio
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime
import time

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =========================
# CONFIG
# =========================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
CHANNEL_ID = int((os.getenv("CHANNEL_ID") or "-1003642090936").strip())
BOT_USERNAME = (os.getenv("BOT_USERNAME") or "").strip().lstrip("@")

# Owner IDs (comma-separated)
OWNER_IDS = set()
_raw_owner = (os.getenv("OWNER_IDS") or "").strip()
for part in _raw_owner.split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

DB_PATH = (os.getenv("DB_PATH") or "files.db").strip()

# Broadcast tuning
BROADCAST_RATE = float((os.getenv("BROADCAST_RATE") or "20").strip())  # msg/sec (aman)
BROADCAST_BATCH = int((os.getenv("BROADCAST_BATCH") or "1000").strip())

# =========================
# REQUIRED JOIN CHANNELS (MAX 5)
# =========================
REQUIRED_CHANNELS = [
    {"id": "-1002268843879", "name": "HEPINI OFFICIAL", "url": "https://t.me/hepiniofc/1689"},
    {"id": "-1003692828104", "name": "Ruang Backup", "url": "https://t.me/hepini_ofcl/3"},
    # maksimal 5 item
]

# =========================
# VALIDATION
# =========================
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set.")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set.")
if len(REQUIRED_CHANNELS) > 5:
    raise RuntimeError("REQUIRED_CHANNELS maksimal 5 item.")

# =========================
# HELPERS
# =========================
def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS

def make_slug() -> str:
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:12]

def gate_text() -> str:
    if not REQUIRED_CHANNELS:
        return ""
    names = "\n".join([f"• {c['name']}" for c in REQUIRED_CHANNELS])
    return (
        "⚠️ Untuk melanjutkan, kamu wajib join dulu ke channel berikut:\n\n"
        f"{names}\n\n"
        "Setelah join, klik tombol **✅ Saya sudah join**."
    )

def join_keyboard(slug: str | None):
    kb = InlineKeyboardBuilder()
    for ch in REQUIRED_CHANNELS:
        kb.button(text=f"Join {ch['name']}", url=ch["url"])
    cb = f"check_join:{slug}" if slug else "check_join:"
    kb.button(text="✅ Saya sudah join", callback_data=cb)
    kb.adjust(1)
    return kb.as_markup()

async def is_joined_all(bot: Bot, user_id: int) -> bool:
    if not REQUIRED_CHANNELS:
        return True
    for ch in REQUIRED_CHANNELS:
        chat = ch["id"]
        try:
            member = await bot.get_chat_member(chat_id=chat, user_id=user_id)
            status = getattr(member, "status", None)
            if status in ("left", "kicked") or status is None:
                return False
        except Exception:
            return False
    return True

class RateLimiter:
    """Global rate limiter: target N msg/sec."""
    def __init__(self, per_sec: float):
        self.min_interval = 1.0 / max(per_sec, 1.0)
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            wait_for = self.min_interval - (now - self._last)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last = time.monotonic()

# =========================
# DB (SQLite)
# =========================
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
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
          user_id INTEGER PRIMARY KEY,
          username TEXT,
          first_name TEXT,
          last_name TEXT,
          last_start TEXT NOT NULL
        );
        """)
        conn.commit()

def db_upsert_user(user_id: int, username: str | None, first_name: str | None, last_name: str | None):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
        INSERT INTO users (user_id, username, first_name, last_name, last_start)
        VALUES (?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          username=excluded.username,
          first_name=excluded.first_name,
          last_name=excluded.last_name,
          last_start=excluded.last_start;
        """, (user_id, username, first_name, last_name, datetime.utcnow().isoformat()))
        conn.commit()

def db_count_users() -> int:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM users;")
        row = cur.fetchone()
        return int(row[0]) if row else 0

def db_iter_user_ids(batch_size: int = 1000):
    """Generator: ambil user_id batch-by-batch biar hemat RAM."""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT user_id FROM users ORDER BY user_id ASC;")
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                yield int(r["user_id"])

def db_put_file(slug: str, channel_id: int, channel_message_id: int, uploaded_by: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT INTO files (slug, channel_id, channel_message_id, uploaded_by) VALUES (?,?,?,?)",
            (slug, channel_id, channel_message_id, uploaded_by),
        )
        conn.commit()

def db_get_file(slug: str):
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
    limiter = RateLimiter(BROADCAST_RATE)

    async def send_file_to_user(user_chat_id: int, slug: str, origin_msg: Message | None = None):
        found = db_get_file(slug)
        if not found:
            if origin_msg:
                await origin_msg.answer("❌ File tidak ditemukan / link sudah tidak valid.")
            else:
                await bot.send_message(user_chat_id, "❌ File tidak ditemukan / link sudah tidak valid.")
            return

        ch_id, ch_msg_id = found
        try:
            await bot.copy_message(
                chat_id=user_chat_id,
                from_chat_id=ch_id,
                message_id=ch_msg_id,
            )
        except Exception as e:
            if origin_msg:
                await origin_msg.answer(f"❌ Gagal mengirim file. ({type(e).__name__})")
            else:
                await bot.send_message(user_chat_id, f"❌ Gagal mengirim file. ({type(e).__name__})")

    @dp.message(CommandStart())
    async def start_handler(message: Message):
        # simpan user ke DB setiap start
        if message.from_user:
            db_upsert_user(
                user_id=message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
            )

        parts = (message.text or "").split(maxsplit=1)
        slug = parts[1].strip() if len(parts) > 1 else None

        uid = message.from_user.id if message.from_user else 0

        # join gate untuk non-owner
        if uid not in OWNER_IDS:
            ok = await is_joined_all(bot, uid)
            if not ok:
                await message.answer(
                    gate_text(),
                    reply_markup=join_keyboard(slug),
                    parse_mode="Markdown"
                )
                return

        # start tanpa slug
        if not slug:
            await message.answer(
                "📦 Kirim file ke bot ini (khusus owner).\n"
                "Kalau kamu punya link file, buka dari link tersebut ya."
            )
            return

        # start dengan slug -> kirim file
        await send_file_to_user(message.chat.id, slug, origin_msg=message)

    @dp.callback_query(F.data.startswith("check_join"))
    async def check_join_cb(call: CallbackQuery):
        uid = call.from_user.id if call.from_user else 0
        data = call.data or "check_join:"
        slug = data.split(":", 1)[1].strip() if ":" in data else ""

        if uid in OWNER_IDS:
            await call.answer("✅ Owner bypass", show_alert=False)
            if slug:
                try:
                    await call.message.delete()
                except Exception:
                    pass
                await send_file_to_user(call.from_user.id, slug)
            else:
                try:
                    await call.message.edit_text("✅ Kamu owner.")
                except Exception:
                    pass
            return

        ok = await is_joined_all(call.bot, uid)
        if not ok:
            await call.answer("Masih belum join semua channel.", show_alert=True)
            return

        await call.answer("✅ Verifikasi berhasil!", show_alert=False)

        if slug:
            try:
                await call.message.delete()
            except Exception:
                pass
            await send_file_to_user(call.from_user.id, slug)
            return

        try:
            await call.message.edit_text("✅ Verifikasi berhasil.")
        except Exception:
            pass

    # =========================
    # ADMIN COMMANDS
    # =========================
    @dp.message(Command("users"))
    async def users_cmd(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if not is_owner(uid):
            return
        total = db_count_users()
        await message.answer(f"👤 Total user tersimpan: {total}")

    @dp.message(Command("broadcast"))
    async def broadcast_cmd(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if not is_owner(uid):
            return

        if not message.reply_to_message:
            await message.answer("Cara pakai:\nReply pesan/file yang mau dikirim, lalu ketik /broadcast")
            return

        total = db_count_users()
        if total <= 0:
            await message.answer("Database user masih kosong.")
            return

        await message.answer(
            f"🚀 Broadcast dimulai ke {total} user.\n"
            f"Rate: ~{int(BROADCAST_RATE)} msg/detik.\n"
            "Catatan: user yang block bot / invalid akan dilewati."
        )

        sent = 0
        failed = 0
        processed = 0

        # kita akan copy apa pun yang direply (text/file/sticker/video/dll)
        src_chat_id = message.chat.id
        src_msg_id = message.reply_to_message.message_id

        for target_id in db_iter_user_ids(batch_size=BROADCAST_BATCH):
            processed += 1
            await limiter.wait()

            try:
                await bot.copy_message(
                    chat_id=target_id,
                    from_chat_id=src_chat_id,
                    message_id=src_msg_id,
                )
                sent += 1
            except Exception as e:
                # Kalau kena limit 429, aiogram biasanya punya attribute retry_after
                retry_after = getattr(e, "retry_after", None)
                if retry_after:
                    await asyncio.sleep(float(retry_after) + 0.5)
                    try:
                        await bot.copy_message(
                            chat_id=target_id,
                            from_chat_id=src_chat_id,
                            message_id=src_msg_id,
                        )
                        sent += 1
                        continue
                    except Exception:
                        failed += 1
                else:
                    failed += 1

            # progress tiap 1000
            if processed % 1000 == 0:
                await message.answer(f"Progress: {processed}/{total} | terkirim {sent} | gagal {failed}")

        await message.answer(f"✅ Broadcast selesai.\nTerkirim: {sent}\nGagal: {failed}\nTotal target: {total}")

    # =========================
    # OWNER UPLOAD
    # =========================
    @dp.message(
        F.content_type.in_({"document", "video", "audio", "voice", "photo", "animation", "sticker"})
        | F.video_note
    )
    async def upload_handler(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if uid not in OWNER_IDS:
            await message.answer("⛔ Kamu tidak punya akses upload.")
            return

        try:
            copied = await bot.copy_message(
                chat_id=CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception as e:
            await message.answer(f"❌ Gagal menyimpan ke channel DB. ({type(e).__name__})")
            return

        slug = make_slug()
        try:
            db_put_file(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))
        except sqlite3.IntegrityError:
            slug = make_slug()
            db_put_file(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))

        if BOT_USERNAME:
            link = f"https://t.me/{BOT_USERNAME}?start={slug}"
            await message.answer(f"✅ Tersimpan!\n🔗 Link publik:\n{link}")
        else:
            await message.answer("✅ Tersimpan! (Set BOT_USERNAME untuk link otomatis)")

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
