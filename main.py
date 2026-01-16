import os
import asyncio
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =========================
# CONFIG (kamu boleh tetap hardcode seperti ini)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8495830935:AAFQP9hOq31jFUdvTZs4YGQlEdJM_S05uq8").strip()
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003642090936").strip())  # channel DB storage
BOT_USERNAME = os.getenv("BOT_USERNAME", "hepini_storage_bot").strip().lstrip("@")

OWNER_IDS = set()
_raw_owner = os.getenv("OWNER_IDS", "5577603728,6016383456").strip()
for part in _raw_owner.split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

DB_PATH = os.getenv("DB_PATH", "files.db").strip()

# =========================
# WAJIB JOIN CHANNEL (maks 5)
# isi sesuai kebutuhanmu
# id bisa "@username" atau -100xxxx
# url tombol join bisa t.me/xxx atau invite link private
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
    raise RuntimeError("BOT_TOKEN belum di-set (set via ENV / Railway Variables).")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set.")
if len(REQUIRED_CHANNELS) > 5:
    raise RuntimeError("REQUIRED_CHANNELS maksimal 5 item.")

# =========================
# HELPERS
# =========================
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
    kb.adjust(1)  # 1 tombol per baris (rapi seperti contoh)
    return kb.as_markup()

async def is_joined_all(bot: Bot, user_id: int) -> bool:
    """
    True jika user join semua REQUIRED_CHANNELS.
    NOTE:
    - Untuk private channel, bot wajib di-add ke channel tsb agar get_chat_member bisa.
    - Jika REQUIRED_CHANNELS kosong => dianggap sudah lolos.
    """
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
            # kalau bot tidak punya akses cek membership -> anggap belum join
            return False
    return True

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

        # cek join untuk non-owner
        if uid not in OWNER_IDS:
            ok = await is_joined_all(bot, uid)
            if not ok:
                # tampilkan gate + tombol join + tombol verifikasi (bawa slug)
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

        # start dengan slug (dan sudah lolos join)
        await send_file_to_user(message.chat.id, slug, origin_msg=message)

    @dp.callback_query(F.data.startswith("check_join"))
    async def check_join_cb(call: CallbackQuery):
        uid = call.from_user.id if call.from_user else 0

        data = call.data or "check_join:"
        slug = data.split(":", 1)[1].strip() if ":" in data else ""

        # owner bypass
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
                    await call.message.edit_text("✅ Kamu owner. Silakan akses link file.")
                except Exception:
                    pass
            return

        ok = await is_joined_all(call.bot, uid)
        if not ok:
            await call.answer("Masih belum join semua channel.", show_alert=True)
            return

        await call.answer("✅ Verifikasi berhasil!", show_alert=False)

        # kalau user datang dari link start=slug, langsung kirim file
        if slug:
            try:
                await call.message.delete()
            except Exception:
                pass
            await send_file_to_user(call.from_user.id, slug)
            return

        # kalau start biasa (tanpa slug)
        try:
            await call.message.edit_text("✅ Verifikasi berhasil. Sekarang kamu bisa akses file via link start.")
        except Exception:
            pass

    @dp.message(
        F.content_type.in_({"document", "video", "audio", "voice", "photo", "animation", "sticker"})
        | F.video_note
    )
    async def upload_handler(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if uid not in OWNER_IDS:
            await message.answer("⛔ Kamu tidak punya akses upload.")
            return

        # 1) copy file ke channel DB (storage)
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
            db_put_file(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))
        except sqlite3.IntegrityError:
            slug = make_slug()
            db_put_file(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))

        # 3) balas link publik
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
