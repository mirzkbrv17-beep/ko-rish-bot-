import asyncio
import io
import logging
import os
import random
import re
from datetime import date

import aiosqlite
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import (ChatAdminRequiredError, ChatWriteForbiddenError,
                             FloodWaitError, PhoneCodeExpiredError,
                             PhoneCodeInvalidError, SessionPasswordNeededError,
                             SlowModeWaitError, UserAlreadyParticipantError,
                             UserBannedInChannelError)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Channel, MessageEntityTextUrl

from translit import to_russian

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("postbot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
FERNET = Fernet(os.environ["FERNET_KEY"].encode())
REQUIRED = os.getenv("REQUIRED_CHANNEL", "@o11abu")
DB = os.getenv("DB_PATH", "bot.db")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DELAY = 3            # har forward orasida soniya
TARGET_VIEWS = 100_000
PAGE = 10

# ---------- Premium emoji ----------
# ID lar botning o'zida /emoji orqali sozlanadi (faqat ADMIN_ID). Sozlanmasa oddiy emoji chiqadi.
PE_DEFAULTS = {"ok": "✅", "no": "❌", "user": "👤", "chan": "📢", "grp": "👥",
               "play": "▶️", "stop": "⏹", "bell": "🔕", "abc": "🔤", "stat": "📊",
               "lock": "🔐", "fire": "🔥"}
PE_LABELS = {"ok": "Muvaffaqiyat", "no": "Xato", "user": "Profil", "chan": "Kanal",
             "grp": "Guruhlar", "play": "Boshlash", "stop": "To'xtatish", "bell": "Mention",
             "abc": "Lotin→Kirill", "stat": "Holat", "lock": "Avto-obuna", "fire": "Sarlavha"}
EMOJI: dict[str, str] = {}


def pe(key: str) -> str:
    """Matn ichida premium emoji (HTML)."""
    cid = EMOJI.get(key)
    fb = PE_DEFAULTS[key]
    return f'<tg-emoji emoji-id="{cid}">{fb}</tg-emoji>' if cid else fb


def btn(text: str, data: str = None, key: str = None, url: str = None):
    kw = {}
    cid = EMOJI.get(key) if key else None
    if cid:
        kw["icon_custom_emoji_id"] = cid  # tugma uchun premium emoji (Bot API 9.4+)
    elif key:
        text = f"{PE_DEFAULTS[key]} {text}"
    return InlineKeyboardButton(text=text, callback_data=data, url=url, **kw)


# ---------- DB ----------
async def db_init():
    async with aiosqlite.connect(DB) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY, session TEXT, channel TEXT,
            running INTEGER DEFAULT 0, ignore_mentions INTEGER DEFAULT 0,
            autojoin INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS sel(
            user_id INTEGER, group_id INTEGER, PRIMARY KEY(user_id, group_id));
        CREATE TABLE IF NOT EXISTS emoji(key TEXT PRIMARY KEY, cid TEXT);
        CREATE TABLE IF NOT EXISTS sent(
            user_id INTEGER, post_id INTEGER, group_id INTEGER, day TEXT,
            PRIMARY KEY(user_id, post_id, group_id, day));
        """)
        try:  # eski bazalar uchun migratsiya
            await db.execute("ALTER TABLE users ADD COLUMN autojoin INTEGER DEFAULT 1")
        except Exception:
            pass
        await db.commit()


async def q(sql, args=(), one=False, commit=False):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, args)
        if commit:
            await db.commit()
            return None
        rows = await cur.fetchall()
        return (rows[0] if rows else None) if one else rows


async def get_user(uid):
    return await q("SELECT * FROM users WHERE id=?", (uid,), one=True)


async def set_user(uid, **fields):
    await q("INSERT OR IGNORE INTO users(id) VALUES(?)", (uid,), commit=True)
    for k, v in fields.items():
        await q(f"UPDATE users SET {k}=? WHERE id=?", (v, uid), commit=True)


# ---------- Telethon clients ----------
CLIENTS: dict[int, TelegramClient] = {}
WORKERS: dict[int, asyncio.Task] = {}
GCACHE: dict[int, list] = {}
LOGIN: dict[int, dict] = {}
IGNORE: dict[int, bool] = {}
AUTOJOIN: dict[int, bool] = {}
SENT_IDS: dict[tuple, set] = {}   # (uid, group_id) -> o'zimiz yuborgan xabar id lari
JOINS: dict[int, list] = {}       # uid -> [kun, bugungi obunalar soni]
JOIN_DAILY_LIMIT = int(os.getenv("JOIN_DAILY_LIMIT", "15"))
INVITE_RE = re.compile(r"(?:t|telegram)\.me/(?:\+|joinchat/)([\w-]{10,})", re.I)
PUB_RE = re.compile(r"(?:t|telegram)\.me/([A-Za-z]\w{3,})", re.I)
USER_RE = re.compile(r"(?<![\w/])@([A-Za-z]\w{4,})")
SKIP_NAMES = {"joinchat", "addlist", "c", "s", "share", "proxy", "socks", "login"}

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()


def _blob(msg) -> str:
    parts = [msg.raw_text or ""]
    for ent, _ in (msg.get_entities_text() or []):
        if isinstance(ent, MessageEntityTextUrl):
            parts.append(ent.url)
    for row in (msg.buttons or []):
        for b in row:
            if getattr(b, "url", None):
                parts.append(b.url)
    return " ".join(parts)


def _joins_left(uid) -> int:
    today = date.today().isoformat()
    if JOINS.get(uid, [None])[0] != today:
        JOINS[uid] = [today, 0]
    return JOIN_DAILY_LIMIT - JOINS[uid][1]


async def join_targets(uid, client, msg):
    """Xabardagi kanal/guruh havolalari va @username larga avtomatik obuna bo'lish."""
    blob = _blob(msg)
    targets = [("i", h) for h in INVITE_RE.findall(blob)]
    seen = set()
    for n in PUB_RE.findall(blob) + USER_RE.findall(blob):
        if n.lower() in SKIP_NAMES or n.lower() in seen:
            continue
        seen.add(n.lower())
        targets.append(("u", n))
    for kind, val in targets[:5]:
        if _joins_left(uid) <= 0:
            break
        try:
            if kind == "i":
                await client(ImportChatInviteRequest(val))
            else:
                ent = await client.get_entity(val)
                if not isinstance(ent, Channel):   # bot/foydalanuvchilarga kirmaymiz
                    continue
                await client(JoinChannelRequest(ent))
            JOINS[uid][1] += 1
            await notify(uid, f"{pe('ok')} Avtomatik obuna bo'lindi: <code>{val}</code>")
        except UserAlreadyParticipantError:
            pass
        except FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 300))
            break
        except Exception as e:
            log.warning("autojoin %s: %s", val, e)
        await asyncio.sleep(random.uniform(4, 9))


async def autojoin_from_recent(uid, client, gid):
    """Guruhdagi oxirgi xabarlardan (bot/zikr) obuna talabini topib, obuna bo'ladi."""
    if not AUTOJOIN.get(uid):
        return
    try:
        async for m in client.iter_messages(gid, limit=15):
            if m.out:
                continue
            if m.mentioned or getattr(m.sender, "bot", False):
                await join_targets(uid, client, m)
    except Exception as e:
        log.warning("recent: %s", e)


def attach_handlers(uid: int, client: TelegramClient):
    @client.on(events.NewMessage(incoming=True))
    async def on_msg(ev):
        if not ev.is_group:
            return
        m = ev.message
        try:
            ours = m.is_reply and m.reply_to_msg_id in SENT_IDS.get((uid, ev.chat_id), ())
            # "Kanalga obuna bo'ling" kabi talab: bizni zikr qilgan yoki postimizga javob bo'lgan xabar
            if AUTOJOIN.get(uid) and (m.mentioned or ours):
                await join_targets(uid, client, m)
            # Zikrlarni o'qilgan deb belgilab, e'tiborsiz qoldirish
            if IGNORE.get(uid) and m.mentioned:
                await client.send_read_acknowledge(ev.chat_id, clear_mentions=True)
        except Exception as e:
            log.warning("on_msg: %s", e)

    @client.on(events.MessageDeleted())
    async def on_deleted(ev):
        # Forward qilgan postimiz admin tomonidan o'chirilsa -> guruh talabiga obuna bo'lish
        try:
            ids = SENT_IDS.get((uid, ev.chat_id))
            if not ev.chat_id or not ids:
                return
            hit = ids & set(ev.deleted_ids)
            if hit:
                ids -= hit
                await autojoin_from_recent(uid, client, ev.chat_id)
        except Exception as e:
            log.warning("on_deleted: %s", e)


async def get_client(uid: int) -> TelegramClient | None:
    if uid in CLIENTS and CLIENTS[uid].is_connected():
        return CLIENTS[uid]
    u = await get_user(uid)
    if not u or not u["session"]:
        return None
    sess = FERNET.decrypt(u["session"].encode()).decode()
    client = TelegramClient(StringSession(sess), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        return None
    await client.get_dialogs()  # entity keshini to'ldirish
    IGNORE[uid] = bool(u["ignore_mentions"])
    AUTOJOIN[uid] = bool(u["autojoin"])
    attach_handlers(uid, client)
    CLIENTS[uid] = client
    return client


async def load_groups(uid: int, refresh=False):
    if refresh or uid not in GCACHE:
        client = await get_client(uid)
        GCACHE[uid] = [(d.id, d.title or str(d.id))
                       async for d in client.iter_dialogs() if d.is_group]
    return GCACHE[uid]


# ---------- Tarqatish (worker) ----------
async def notify(uid, text):
    try:
        await bot.send_message(uid, text)
    except Exception:
        pass


COUNT: dict[int, list] = {}   # uid -> [kun, bugungi forwardlar soni]


def _count(uid, n=1):
    today = date.today().isoformat()
    if COUNT.get(uid, [None])[0] != today:
        COUNT[uid] = [today, 0]
    COUNT[uid][1] += n
    return COUNT[uid][1]


async def worker(uid: int):
    """Eng kam ko'rishli pog'onani (100, 200, 300 ...) to'ldirguncha qayta-qayta forward qiladi."""
    log.info("worker start %s", uid)
    batch_ids: list[int] = []
    threshold = 0
    rounds = 0
    while True:
        u = await get_user(uid)
        if not u or not u["running"] or not u["channel"]:
            return
        client = await get_client(uid)
        if client is None:
            await notify(uid, f"{pe('no')} Profil sessiyasi eskirgan. Profilni qayta ulang.")
            await set_user(uid, running=0)
            return
        chan = int(u["channel"])

        # 1) Postlarni yangilash: batch bo'sh bo'lsa yoki har 10 aylanishda to'liq o'qiladi
        try:
            if not batch_ids or rounds % 10 == 0:
                posts = [m async for m in client.iter_messages(chan)
                         if (m.message or m.media) and not m.action and m.views is not None]
                views = {m.id: m.views for m in posts}
                if not views or min(views.values()) >= TARGET_VIEWS:
                    await set_user(uid, running=0)
                    await notify(uid, f"{pe('ok')} Kanaldagi barcha postlar {TARGET_VIEWS:,} ko'rishga yetdi. Tarqatish to'xtatildi.")
                    return
                lowest = min(views.values())
                threshold = min((lowest // 100 + 1) * 100, TARGET_VIEWS)
                batch_ids = [i for i, v in views.items() if v < threshold]
            else:
                msgs = await client.get_messages(chan, ids=batch_ids)
                batch_ids = [m.id for m in msgs if m and m.views is not None and m.views < threshold]
        except Exception as e:
            await notify(uid, f"{pe('no')} Kanalni o'qib bo'lmadi: {e}")
            await asyncio.sleep(300)
            continue
        rounds += 1
        if not batch_ids:        # pog'ona to'ldi -> keyingi pog'onani hisoblash
            rounds = 0
            continue

        # 2) Shu pog'onadagi barcha postlarni barcha guruhlarga forward qilish
        groups = [r["group_id"] for r in await q("SELECT group_id FROM sel WHERE user_id=?", (uid,))]
        for pid in sorted(batch_ids):
            for gid in groups:
                u = await get_user(uid)
                if not u or not u["running"]:
                    log.info("worker stop %s", uid)
                    return
                try:
                    res = await client.forward_messages(gid, pid, from_peer=chan)
                    sm = res[0] if isinstance(res, list) else res
                    if sm:
                        SENT_IDS.setdefault((uid, gid), set()).add(sm.id)
                    _count(uid)
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                except SlowModeWaitError:
                    pass
                except (ChatWriteForbiddenError, UserBannedInChannelError, ChatAdminRequiredError):
                    await autojoin_from_recent(uid, client, gid)
                except Exception as e:
                    log.warning("forward %s -> %s: %s", pid, gid, e)
                await asyncio.sleep(DELAY)


def start_worker(uid):
    t = WORKERS.get(uid)
    if t and not t.done():
        return
    WORKERS[uid] = asyncio.create_task(worker(uid))


# ---------- Majburiy obuna ----------
async def is_subscribed(uid: int) -> bool:
    try:
        m = await bot.get_chat_member(REQUIRED, uid)
        return m.status in ("member", "administrator", "creator")
    except Exception:
        return False


def sub_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("Kanalga obuna bo'lish", url=f"https://t.me/{REQUIRED.lstrip('@')}", key="chan")],
        [btn("Tekshirish", "check_sub", key="ok")]])


class SubMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = event.from_user
        if isinstance(event, CallbackQuery) and event.data == "check_sub":
            return await handler(event, data)
        if user and not await is_subscribed(user.id):
            text = f"{pe('lock')} Botdan foydalanish uchun avval kanalimizga obuna bo'ling:"
            if isinstance(event, CallbackQuery):
                await event.answer("Avval obuna bo'ling!", show_alert=True)
                await event.message.answer(text, reply_markup=sub_kb())
            else:
                await event.answer(text, reply_markup=sub_kb())
            return
        return await handler(event, data)


# ---------- Menyu ----------
async def main_kb(uid):
    u = await get_user(uid)
    running = bool(u and u["running"])
    ign = bool(u and u["ignore_mentions"])
    aj = bool(u and u["autojoin"])
    extra = [[InlineKeyboardButton(text="🎨 Emoji sozlash", callback_data="emoji")]] \
        if ADMIN_ID and uid == ADMIN_ID else []
    return InlineKeyboardMarkup(inline_keyboard=extra + [
        [btn("Profil ulash", "login", "user"), btn("Kanal ulash", "chan", "chan")],
        [btn("Guruhlar", "gp:0", "grp")],
        [btn("To'xtatish" if running else "Boshlash", "toggle_run", "stop" if running else "play")],
        [btn(f"Mention e'tiborsiz: {'YOQIQ' if ign else 'O‘CHIQ'}", "toggle_ign", "bell")],
        [btn(f"Avto-obuna: {'YOQIQ' if aj else 'O‘CHIQ'}", "toggle_aj", "lock")],
        [btn("Lotin → Rus (Kirill)", "translit", "abc"), btn("Holat", "stat", "stat")],
    ])


async def show_menu(target, uid):
    text = (f"{pe('fire')} <b>Post tarqatuvchi bot</b>\n\n"
            "1) Profilni ulang\n2) Kanalni ulang\n3) Guruhlarni belgilang\n4) «Boshlash» ni bosing")
    kb = await main_kb(uid)
    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=kb)
        except Exception:
            await target.message.answer(text, reply_markup=kb)
    else:
        await target.answer(text, reply_markup=kb)


class Login(StatesGroup):
    phone = State()
    code = State()
    password = State()


class ChanS(StatesGroup):
    link = State()


class Em(StatesGroup):
    wait = State()


def emoji_kb():
    rows, row = [], []
    for k, label in PE_LABELS.items():
        mark = "✔" if k in EMOJI else PE_DEFAULTS[k]
        row.append(InlineKeyboardButton(text=f"{mark} {label}", callback_data=f"em:{k}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Menyu", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


EM_TEXT = ("🎨 <b>Premium emoji sozlash</b>\nO'rnini tanlang, so'ng premium emoji yuboring. "
           "✔ — sozlangan.")


@router.message(Command("emoji"))
async def emoji_cmd(m: Message, state: FSMContext):
    if not ADMIN_ID or m.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await m.answer(EM_TEXT, reply_markup=emoji_kb())


@router.callback_query(F.data == "emoji")
async def emoji_open(c: CallbackQuery, state: FSMContext):
    if not ADMIN_ID or c.from_user.id != ADMIN_ID:
        return await c.answer()
    await state.clear()
    await c.message.edit_text(EM_TEXT, reply_markup=emoji_kb())
    await c.answer()


@router.callback_query(F.data.startswith("em:"))
async def emoji_pick(c: CallbackQuery, state: FSMContext):
    if not ADMIN_ID or c.from_user.id != ADMIN_ID:
        return await c.answer()
    key = c.data.split(":")[1]
    await state.set_state(Em.wait)
    await state.update_data(key=key)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Oddiy emojiga qaytarish", callback_data=f"emx:{key}"),
        InlineKeyboardButton(text="⬅️ Orqaga", callback_data="emoji")]])
    await c.message.edit_text(f"«{PE_LABELS[key]}» uchun premium emoji yuboring (bitta):", reply_markup=kb)
    await c.answer()


@router.callback_query(F.data.startswith("emx:"))
async def emoji_reset(c: CallbackQuery, state: FSMContext):
    if not ADMIN_ID or c.from_user.id != ADMIN_ID:
        return await c.answer()
    key = c.data.split(":")[1]
    EMOJI.pop(key, None)
    await q("DELETE FROM emoji WHERE key=?", (key,), commit=True)
    await state.clear()
    await c.message.edit_text(EM_TEXT, reply_markup=emoji_kb())
    await c.answer("Qaytarildi")


@router.message(Em.wait)
async def emoji_save(m: Message, state: FSMContext):
    if not ADMIN_ID or m.from_user.id != ADMIN_ID:
        return
    ents = [e for e in (m.entities or []) if e.type == "custom_emoji"]
    if not ents:
        return await m.answer("Premium emoji yuboring (oddiy emoji emas).")
    key = (await state.get_data())["key"]
    cid = ents[0].custom_emoji_id
    EMOJI[key] = cid
    await q("INSERT OR REPLACE INTO emoji VALUES(?,?)", (key, cid), commit=True)
    await state.clear()
    await m.answer(f"{pe(key)} Saqlandi.", reply_markup=emoji_kb())


class Tr(StatesGroup):
    text = State()


@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await set_user(m.from_user.id)
    await show_menu(m, m.from_user.id)


@router.callback_query(F.data == "check_sub")
async def check_sub(c: CallbackQuery):
    if await is_subscribed(c.from_user.id):
        await set_user(c.from_user.id)
        await show_menu(c, c.from_user.id)
    else:
        await c.answer("Hali obuna bo'lmagansiz!", show_alert=True)


@router.callback_query(F.data == "menu")
async def menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_menu(c, c.from_user.id)


# ----- Profil ulash -----
QR_KB = InlineKeyboardMarkup(inline_keyboard=[[
    InlineKeyboardButton(text="📷 QR orqali ulash", callback_data="login_qr")]])

@router.callback_query(F.data == "login")
async def login_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(Login.phone)
    await c.message.answer(
        f"{pe('user')} Telefon raqamingizni xalqaro formatda yuboring (masalan: <code>+998901234567</code>).\n\n"
        "⚠️ Kod kelganda uni <b>raqamlar orasiga bo'sh joy qo'yib</b> yozing (masalan: <code>1 2 3 4 5</code>), "
        "aks holda Telegram kodni bekor qiladi.\n\n"
        "Kod kelmasa, QR tugmasidan foydalaning 👇",
        reply_markup=QR_KB)
    await c.answer()


@router.message(Login.phone)
async def login_phone(m: Message, state: FSMContext):
    phone = re.sub(r"[^\d+]", "", m.text or "")
    if not phone.startswith("+"):
        phone = "+" + phone
    old = LOGIN.pop(m.from_user.id, None)
    if old:
        try:
            await old["client"].disconnect()
        except Exception:
            pass
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
    except Exception as e:
        await m.answer(f"{pe('no')} Xato: {e}")
        await client.disconnect()
        return
    LOGIN[m.from_user.id] = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
    await state.set_state(Login.code)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📩 SMS bilan qayta yuborish", callback_data="resend_code")],
        [InlineKeyboardButton(text="📷 QR orqali ulash", callback_data="login_qr")]])
    await m.answer(
        "📲 Kod yuborildi. U <b>Telegram ilovangizdagi «Telegram» (rasmiy) chatiga</b> keladi, "
        "SMS emas. Boshqa qurilmadagi Telegramni ham tekshiring.\n\n"
        "Kodni nuqta bilan yozing: <code>1.2.3.4.5</code>", reply_markup=kb)


@router.callback_query(F.data == "login_qr")
async def login_qr(c: CallbackQuery, state: FSMContext):
    import qrcode
    uid = c.from_user.id
    await c.answer()
    old = LOGIN.pop(uid, None)
    if old:
        try:
            await old["client"].disconnect()
        except Exception:
            pass
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        qr = await client.qr_login()
        for _ in range(5):
            buf = io.BytesIO()
            qrcode.make(qr.url).save(buf, format="PNG")
            await c.message.answer_photo(
                BufferedInputFile(buf.getvalue(), "qr.png"),
                caption="📷 Ulamoqchi bo'lgan akkaunt Telegramida:\n"
                        "<b>Sozlamalar → Qurilmalar → Qurilma ulash</b> ni bosing va shu QR ni skanerlang.\n"
                        "⏳ 30 soniya ichida.")
            try:
                await qr.wait(30)
                break
            except asyncio.TimeoutError:
                await qr.recreate()
            except SessionPasswordNeededError:
                LOGIN[uid] = {"client": client, "phone": "", "hash": ""}
                await state.set_state(Login.password)
                await c.message.answer("🔑 Ikki bosqichli parolni kiriting:")
                return
        else:
            await client.disconnect()
            await c.message.answer(f"{pe('no')} QR skanerlanmadi. Qaytadan urinib ko'ring.")
            return
    except SessionPasswordNeededError:
        LOGIN[uid] = {"client": client, "phone": "", "hash": ""}
        await state.set_state(Login.password)
        await c.message.answer("🔑 Ikki bosqichli parolni kiriting:")
        return
    except Exception as e:
        await client.disconnect()
        await c.message.answer(f"{pe('no')} Xato: {e}")
        return
    await finish_login(c.message, state, client, uid)


@router.callback_query(F.data == "resend_code")
async def resend_code(c: CallbackQuery):
    d = LOGIN.get(c.from_user.id)
    if not d:
        return await c.answer("Avval telefon raqam yuboring", show_alert=True)
    try:
        sent = await d["client"].send_code_request(d["phone"], force_sms=True)
        d["hash"] = sent.phone_code_hash
        await c.message.answer("📩 Kod SMS orqali qayta yuborildi (kelishi 1-2 daqiqa olishi mumkin).")
    except Exception as e:
        await c.message.answer(f"{pe('no')} Xato: {e}")
    await c.answer()


async def finish_login(m: Message, state: FSMContext, client, uid: int = None):
    uid = uid or m.from_user.id
    enc = FERNET.encrypt(client.session.save().encode()).decode()
    await set_user(uid, session=enc)
    if uid in CLIENTS:
        await CLIENTS[uid].disconnect()
    IGNORE[uid] = bool((await get_user(uid))["ignore_mentions"])
    attach_handlers(uid, client)
    CLIENTS[uid] = client
    await client.get_dialogs()
    GCACHE.pop(uid, None)
    LOGIN.pop(uid, None)
    await state.clear()
    await m.answer(f"{pe('ok')} Profil ulandi!")
    await show_menu(m, uid)


@router.message(Login.code)
async def login_code(m: Message, state: FSMContext):
    d = LOGIN.get(m.from_user.id)
    if not d:
        await state.clear()
        return
    code = re.sub(r"\D", "", m.text or "")
    try:
        await d["client"].sign_in(d["phone"], code, phone_code_hash=d["hash"])
    except SessionPasswordNeededError:
        await state.set_state(Login.password)
        await m.answer("🔑 Ikki bosqichli parolni kiriting:")
        return
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        await m.answer(f"{pe('no')} Kod noto'g'ri yoki eskirgan. Qaytadan /start.")
        await state.clear()
        return
    await finish_login(m, state, d["client"])


@router.message(Login.password)
async def login_pass(m: Message, state: FSMContext):
    d = LOGIN.get(m.from_user.id)
    try:
        await d["client"].sign_in(password=m.text)
    except Exception as e:
        await m.answer(f"{pe('no')} Parol xato: {e}")
        return
    try:
        await m.delete()
    except Exception:
        pass
    await finish_login(m, state, d["client"])


# ----- Kanal ulash -----
@router.callback_query(F.data == "chan")
async def chan_start(c: CallbackQuery, state: FSMContext):
    if not await get_client(c.from_user.id):
        await c.answer("Avval profilni ulang!", show_alert=True)
        return
    await state.set_state(ChanS.link)
    await c.message.answer(f"{pe('chan')} Kanal havolasi yoki @username ini yuboring:")
    await c.answer()


@router.message(ChanS.link)
async def chan_link(m: Message, state: FSMContext):
    client = await get_client(m.from_user.id)
    link = (m.text or "").strip().replace("https://t.me/", "@").replace("t.me/", "@")
    try:
        ent = await client.get_entity(link)
    except Exception as e:
        await m.answer(f"{pe('no')} Kanal topilmadi: {e}")
        return
    if not isinstance(ent, Channel) or getattr(ent, "megagroup", False):
        await m.answer(f"{pe('no')} Bu kanal emas. Kanal username'ini to'g'ri yuboring.")
        return
    from telethon.utils import get_peer_id
    await set_user(m.from_user.id, channel=get_peer_id(ent))
    await state.clear()
    await m.answer(f"{pe('ok')} Kanal ulandi: <b>{getattr(ent, 'title', link)}</b>")
    await show_menu(m, m.from_user.id)


# ----- Guruhlar -----
async def groups_markup(uid, page):
    groups = await load_groups(uid)
    chosen = {r["group_id"] for r in await q("SELECT group_id FROM sel WHERE user_id=?", (uid,))}
    pages = max(1, (len(groups) + PAGE - 1) // PAGE)
    page = max(0, min(page, pages - 1))
    rows = []
    for gid, title in groups[page * PAGE:(page + 1) * PAGE]:
        mark = "✅" if gid in chosen else "⬜️"
        rows.append([InlineKeyboardButton(text=f"{mark} {title[:40]}", callback_data=f"gt:{gid}:{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"gp:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"gp:{page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="Hammasini belgilash", callback_data=f"ga:{page}"),
                 InlineKeyboardButton(text="Tozalash", callback_data=f"gc:{page}")])
    rows.append([InlineKeyboardButton(text="🔄 Yangilash", callback_data="gr"),
                 InlineKeyboardButton(text="⬅️ Menyu", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows), len(chosen), len(groups)


async def render_groups(c: CallbackQuery, page):
    if not await get_client(c.from_user.id):
        await c.answer("Avval profilni ulang!", show_alert=True)
        return
    kb, n, total = await groups_markup(c.from_user.id, page)
    text = f"{pe('grp')} Guruhlar: {n}/{total} belgilangan\nKerakli guruhlarni belgilang:"
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except Exception:
        await c.message.answer(text, reply_markup=kb)
    await c.answer()


@router.callback_query(F.data.startswith("gp:"))
async def g_page(c: CallbackQuery):
    await render_groups(c, int(c.data.split(":")[1]))


@router.callback_query(F.data == "gr")
async def g_refresh(c: CallbackQuery):
    await load_groups(c.from_user.id, refresh=True)
    await render_groups(c, 0)


@router.callback_query(F.data.startswith("gt:"))
async def g_toggle(c: CallbackQuery):
    _, gid, page = c.data.split(":")
    uid, gid = c.from_user.id, int(gid)
    if await q("SELECT 1 FROM sel WHERE user_id=? AND group_id=?", (uid, gid), one=True):
        await q("DELETE FROM sel WHERE user_id=? AND group_id=?", (uid, gid), commit=True)
    else:
        await q("INSERT OR IGNORE INTO sel VALUES(?,?)", (uid, gid), commit=True)
    await render_groups(c, int(page))


@router.callback_query(F.data.startswith("ga:"))
async def g_all(c: CallbackQuery):
    uid = c.from_user.id
    for gid, _ in await load_groups(uid):
        await q("INSERT OR IGNORE INTO sel VALUES(?,?)", (uid, gid), commit=True)
    await render_groups(c, int(c.data.split(":")[1]))


@router.callback_query(F.data.startswith("gc:"))
async def g_clear(c: CallbackQuery):
    await q("DELETE FROM sel WHERE user_id=?", (c.from_user.id,), commit=True)
    await render_groups(c, int(c.data.split(":")[1]))


@router.callback_query(F.data == "noop")
async def noop(c: CallbackQuery):
    await c.answer()


# ----- Boshlash / to'xtatish, mention, holat -----
@router.callback_query(F.data == "toggle_run")
async def toggle_run(c: CallbackQuery):
    uid = c.from_user.id
    u = await get_user(uid)
    if u["running"]:
        await set_user(uid, running=0)
        await c.answer("To'xtatildi")
    else:
        sel = await q("SELECT 1 FROM sel WHERE user_id=?", (uid,), one=True)
        if not u["session"] or not u["channel"] or not sel:
            await c.answer("Profil, kanal va kamida 1 ta guruh kerak!", show_alert=True)
            return
        await set_user(uid, running=1)
        start_worker(uid)
        await c.answer("Boshlandi!")
    await show_menu(c, uid)


@router.callback_query(F.data == "toggle_ign")
async def toggle_ign(c: CallbackQuery):
    uid = c.from_user.id
    u = await get_user(uid)
    new = 0 if u["ignore_mentions"] else 1
    await set_user(uid, ignore_mentions=new)
    IGNORE[uid] = bool(new)
    await c.answer("Yoqildi" if new else "O'chirildi")
    await show_menu(c, uid)


@router.callback_query(F.data == "toggle_aj")
async def toggle_aj(c: CallbackQuery):
    uid = c.from_user.id
    u = await get_user(uid)
    new = 0 if u["autojoin"] else 1
    await set_user(uid, autojoin=new)
    AUTOJOIN[uid] = bool(new)
    await c.answer("Yoqildi" if new else "O'chirildi")
    await show_menu(c, uid)


@router.callback_query(F.data == "stat")
async def stat(c: CallbackQuery):
    uid = c.from_user.id
    n = (await q("SELECT COUNT(*) n FROM sel WHERE user_id=?", (uid,), one=True))["n"]
    c = COUNT.get(uid)
    s = c[1] if c and c[0] == date.today().isoformat() else 0
    u = await get_user(uid)
    await c.message.answer(
        f"{pe('stat')} <b>Holat</b>\nProfil: {'ulangan' if u['session'] else 'yo‘q'}\n"
        f"Kanal: {'ulangan' if u['channel'] else 'yo‘q'}\nGuruhlar: {n}\n"
        f"Bugun forward qilingan: {s}\nHolat: {'ishlayapti' if u['running'] else 'to‘xtagan'}")
    await c.answer()


# ----- Lotin -> Rus -----
@router.callback_query(F.data == "translit")
async def tr_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(Tr.text)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Menyu", callback_data="menu")]])
    await c.message.answer(f"{pe('abc')} Ingliz (lotin) alifbosida matn, she'r yoki gap yuboring — "
                           "rus alifbosida yozib beraman.", reply_markup=kb)
    await c.answer()


@router.message(Tr.text)
async def tr_text(m: Message):
    if m.text:
        await m.answer(f"<code>{to_russian(m.text)}</code>")


async def health_server():
    # Render Web Service port talab qiladi
    app = web.Application()
    app.add_routes([web.get("/", lambda r: web.Response(text="ok"))])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000"))).start()


async def main():
    await health_server()
    await db_init()
    for r in await q("SELECT key, cid FROM emoji"):
        EMOJI[r["key"]] = r["cid"]
    dp = Dispatcher(storage=MemoryStorage())
    router.message.outer_middleware(SubMiddleware())
    router.callback_query.outer_middleware(SubMiddleware())
    dp.include_router(router)
    # Qayta ishga tushganda faol workerlarni tiklash
    for r in await q("SELECT id FROM users WHERE running=1"):
        start_worker(r["id"])
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
