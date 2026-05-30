# Updated bot — hnnumber10.py
# Changes applied:
#  1. Async Motor driver replaces blocking pymongo
#  2. Admin-check decorator replaces repeated inline blocks
#  3. Admin /lookup command — search by chat_id OR username
#  4. /addbalance and /deductbalance with confirmation buttons (inline, inside user lookup)
#  5. 📈 Growth Stats button in admin keyboard (text table + bar chart image, both included)

import asyncio
import io
import logging
import os
import random
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from functools import wraps

import dns.resolver
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ── async MongoDB ──────────────────────────────────────────────────────────────
import motor.motor_asyncio                          # pip install motor
from pymongo import UpdateOne                       # still used for bulk_write helpers

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, CopyTextButton,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

logging.disable(logging.CRITICAL)

dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers = ["8.8.8.8"]

# ===== DATABASE =====
USERNAME         = "Hnbotdata"
PASSWORD         = "cUtWlmGNb3FUxfSO"
DB_NAME          = "HNDATA"
encoded_password = urllib.parse.quote(PASSWORD)
connection_string = (
    f"mongodb+srv://{USERNAME}:{encoded_password}"
    f"@cluster0.ubpi5td.mongodb.net/?appName=Cluster0"
)

# Motor async client  (no blocking — safe inside the event loop)
motor_client     = motor.motor_asyncio.AsyncIOMotorClient(connection_string, serverSelectionTimeoutMS=5000)
_db              = motor_client[DB_NAME]
col_users        = _db["user"]
col_numbers      = _db["numbers"]
col_used_numbers = _db["used_numbers"]
col_sms          = _db["sms"]
col_withdrawals  = _db["withdrawals"]
col_sms_count    = _db["sms_count"]

async def ping_db():
    """Called once at startup to verify connectivity."""
    try:
        await motor_client.admin.command("ping")
        print("✅ Connected to MongoDB Atlas!")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        raise SystemExit(1)

# ===== IN-MEMORY CACHES =====
_cache_blacklist:    set   = set()
_cache_blacklist_ts: float = 0.0
_cache_rl_enabled:   bool  = False
_cache_rl_ts:        float = 0.0
_cache_used_set:     set   = set()
_cache_used_ts:      float = 0.0
_CACHE_TTL:          float = 10.0

# ===== MEMBERSHIP CACHE =====
_member_cache: dict[int, float] = {}
MEMBER_CACHE_TTL = 1900

def _member_cache_is_valid(user_id: int) -> bool:
    ts = _member_cache.get(user_id)
    return ts is not None and (time.time() - ts) < MEMBER_CACHE_TTL

def _member_cache_save(user_id: int):
    _member_cache[user_id] = time.time()

def _member_cache_clear(user_id: int):
    _member_cache.pop(user_id, None)

# ===== BOT CONFIG =====
#BOT_TOKEN  = "8724320995:AAE15nUzPVme3eD56MOCU47JiBkeYZIJWs8"
BOT_TOKEN  = "8505386765:AAHPr2PQIrfPqpgHHjG6nDJ79HfQm7pYsfo" #Number Bot2
ADMIN_IDS  = {8011214296, 6069725948, 7257510080}
REQUIRED_CHANNELS = [
    {"name": "👑 Main Channel",   "url": "https://t.me/+jOxyfVFOxccyMTk1", "chat_id": "-1002244166097", "username": None},
    {"name": "🟢 Number Channel", "url": "https://t.me/hntechnumber",       "chat_id": "-1003951088847", "username": "hntechnumber"},
    {"name": "✅ Support Group",  "url": "https://t.me/TachbartaOtp",       "chat_id": "-1003067567631", "username": "TachbartaOtp"},
]
SUPPORT_HANDLE = "@tanjiro27kamado"
SUPPORT_BOT    = "@tanjiro27kamado"
CHANNEL_LINK   = "https://t.me/+jOxyfVFOxccyMTk1"
SMS_GROUP_LINK = "https://t.me/+prLS1SvL5FAwOWU8"
BATCH_SIZE      = 3
RATE_LIMIT_SECS = 1
COUNTRY_PREFIXES = {
    "93":  ("Afghanistan", "🇦🇫"),
    "355": ("Albania", "🇦🇱"),
    "213": ("Algeria", "🇩🇿"),
}  # More will be added Soon

# ===== ADMIN-CHECK DECORATOR (fix #2) =========================================
def admin_only(func):
    """Decorator for CallbackQueryHandlers — rejects non-admins immediately."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query   = update.callback_query
        user_id = query.from_user.id
        if user_id not in ADMIN_IDS:
            await query.answer("🚫 Not authorized.", show_alert=True)
            return
        return await func(update, context)
    return wrapper

def admin_query_with_sub_check(func):
    """
    Decorator for user-facing CallbackQueryHandlers that need:
      1. subscription check (non-admins)
      2. rate limit (non-admins)
    """
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query   = update.callback_query
        user_id = query.from_user.id
        if user_id not in ADMIN_IDS:
            verified = await check_subscription(context.bot, user_id)
            if not verified:
                await query.answer("❌ Please join all required channels first.", show_alert=True)
                return
            wait = check_rate_limit(user_id)
            if wait > 0:
                await query.answer(f"⏳ Please wait {wait}s before trying again.", show_alert=True)
                return
            update_rate_timestamp(user_id)
        await query.answer()
        return await func(update, context)
    return wrapper

# ===== ASYNC CACHE HELPERS ====================================================
async def _get_used_set_cached() -> set:
    global _cache_used_set, _cache_used_ts
    if time.time() - _cache_used_ts > _CACHE_TTL:
        _cache_used_set = {doc["_id"] async for doc in col_used_numbers.find({}, {"_id": 1})}
        _cache_used_ts  = time.time()
    return _cache_used_set

def _invalidate_used_cache():
    global _cache_used_ts
    _cache_used_ts = 0.0

async def load_blacklist() -> set:
    global _cache_blacklist, _cache_blacklist_ts
    if time.time() - _cache_blacklist_ts > _CACHE_TTL:
        doc = await col_users.find_one({"_id": "__blacklist__"})
        _cache_blacklist    = set(doc.get("ids", [])) if doc else set()
        _cache_blacklist_ts = time.time()
    return _cache_blacklist

async def save_blacklist(bl: set):
    global _cache_blacklist, _cache_blacklist_ts
    await col_users.update_one(
        {"_id": "__blacklist__"},
        {"$set": {"ids": list(bl)}},
        upsert=True,
    )
    _cache_blacklist    = bl
    _cache_blacklist_ts = time.time()

async def is_rate_limit_on() -> bool:
    global _cache_rl_enabled, _cache_rl_ts
    if time.time() - _cache_rl_ts > _CACHE_TTL:
        doc = await col_users.find_one({"_id": "__rate_limit__"})
        _cache_rl_enabled = doc.get("enabled", False) if doc else False
        _cache_rl_ts      = time.time()
    return _cache_rl_enabled

async def set_rate_limit(enabled: bool):
    global _cache_rl_enabled, _cache_rl_ts
    await col_users.update_one(
        {"_id": "__rate_limit__"},
        {"$set": {"enabled": enabled}},
        upsert=True,
    )
    _cache_rl_enabled = enabled
    _cache_rl_ts      = time.time()

# ===== RATE LIMIT (in-memory) =================================================
_rate_timestamps: dict[int, float] = {}

def check_rate_limit(user_id: int) -> float:
    # Note: is_rate_limit_on() is now async — callers that need this must await it separately
    last    = _rate_timestamps.get(user_id, 0)
    elapsed = time.time() - last
    if elapsed < RATE_LIMIT_SECS:
        return round(RATE_LIMIT_SECS - elapsed, 1)
    return 0

def update_rate_timestamp(user_id: int):
    _rate_timestamps[user_id] = time.time()

# ===== SMS WATCH ==============================================================
_sms_watch: dict[int, dict] = {}
SMS_POLL_INTERVAL = 2
SMS_WATCH_TIMEOUT = 1800

def sms_watch_set(user_id: int, numbers: list[str]):
    _sms_watch[user_id] = {
        "numbers":     [n.lstrip("+") for n in numbers],
        "latest_seen": datetime.now(timezone.utc),
        "started_at":  time.time(),
    }

def sms_watch_clear(user_id: int):
    _sms_watch.pop(user_id, None)

def format_sms_entry(doc: dict) -> str:
    otp = doc.get("otp") or "NA"
    return (
        f"📨 *New SMS Received!*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📱 Number   : `{doc.get('number', 'N/A')}`\n"
        f"🔑 OTP      : `{otp}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{doc.get('full_sms', 'N/A')}"
    )

async def save_sms_count(chat_id: int, username: str, sms_doc: dict):
    await col_sms_count.insert_one({
        "chat_id":  chat_id,
        "username": username,
        "time":     sms_doc.get("time", ""),
        "number":   sms_doc.get("number", ""),
        "cli":      sms_doc.get("cli", ""),
        "otp":      sms_doc.get("otp", "NA"),
        "full_sms": sms_doc.get("full_sms", ""),
        "panel":    sms_doc.get("panel", ""),
    })

async def sms_poll_loop(bot):
    while True:
        await asyncio.sleep(SMS_POLL_INTERVAL)
        now     = time.time()
        expired = [uid for uid, w in _sms_watch.items() if now - w["started_at"] > SMS_WATCH_TIMEOUT]
        for uid in expired:
            sms_watch_clear(uid)
        for user_id, watch in list(_sms_watch.items()):
            try:
                latest_seen_str = watch["latest_seen"].strftime("%Y-%m-%d %H:%M:%S")
                new_docs = await col_sms.find(
                    {"number": {"$in": watch["numbers"]}, "time": {"$gt": latest_seen_str}},
                    sort=[("time", 1)],
                ).to_list(length=None)
                if new_docs:
                    try:
                        watch["latest_seen"] = datetime.strptime(
                            new_docs[-1]["time"], "%Y-%m-%d %H:%M:%S"
                        ).replace(tzinfo=timezone.utc)
                    except Exception:
                        watch["latest_seen"] = datetime.now(timezone.utc)
                    for doc in new_docs:
                        try:
                            await bot.send_message(
                                chat_id=user_id,
                                text=format_sms_entry(doc),
                                parse_mode="Markdown",
                            )
                        except Exception:
                            pass
                        try:
                            user_doc = await col_users.find_one({"chat_id": user_id})
                            username = user_doc.get("username", "N/A") if user_doc else "N/A"
                            await save_sms_count(chat_id=user_id, username=username, sms_doc=doc)
                        except Exception:
                            pass
            except Exception:
                pass

# ===== BAN HELPERS ============================================================
async def is_banned(user_id: int) -> bool:
    return str(user_id) in await load_blacklist()

async def ban_user(user_id: int) -> str:
    if user_id in ADMIN_IDS:
        return "admin"
    bl = await load_blacklist()
    if str(user_id) in bl:
        return "already"
    bl.add(str(user_id))
    await save_blacklist(bl)
    return "ok"

async def unban_user(user_id: int) -> bool:
    bl = await load_blacklist()
    if str(user_id) not in bl:
        return False
    bl.discard(str(user_id))
    await save_blacklist(bl)
    return True

# ===== USER HELPERS ===========================================================
async def save_user(user, referred_by: int = None):
    uid = str(user.id)
    existing = await col_users.find_one({"_id": uid})
    if not existing:
        doc = {
            "_id":               uid,
            "username":          f"@{user.username}" if user.username else "N/A",
            "chat_id":           user.id,
            "joined_date":       datetime.now().strftime("%Y-%m-%d"),
            "balance":           0.0,
            "referral_count_l1": 0,
            "referral_count_l2": 0,
            "referred_by":       referred_by if referred_by and referred_by != user.id else None,
            "referral_credited": False,
            "withdrawal_pending": False,
        }
        await col_users.insert_one(doc)
    else:
        await col_users.update_one(
            {"_id": uid, "balance": {"$exists": False}},
            {"$set": {
                "balance": 0.0,
                "referral_count_l1": 0,
                "referral_count_l2": 0,
                "referred_by": None,
                "referral_credited": False,
                "withdrawal_pending": False,
            }},
        )

async def credit_referral(new_user_id: int, bot=None):
    uid      = str(new_user_id)
    new_user = await col_users.find_one({"_id": uid})
    if not new_user or new_user.get("referral_credited", False):
        return
    l1_id = new_user.get("referred_by")
    if not l1_id or l1_id == new_user_id:
        await col_users.update_one({"_id": uid}, {"$set": {"referral_credited": True}})
        return
    new_username = new_user.get("username", "A user")
    await col_users.update_one(
        {"chat_id": l1_id},
        {"$inc": {"balance": 0.01, "referral_count_l1": 1}},
    )
    if bot:
        l1_doc_after = await col_users.find_one({"chat_id": l1_id})
        new_bal = l1_doc_after.get("balance", 0.0) if l1_doc_after else 0.0
        try:
            await bot.send_message(
                chat_id=l1_id,
                text=(
                    f"🎉 *Referral Bonus!*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 {new_username} just joined using your link!\n\n"
                    f"💰 You earned: *+$0.01*\n"
                    f"💵 New balance: *${new_bal:.3f}*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"Keep sharing your link to earn more! 🚀"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            pass
    l1_doc = await col_users.find_one({"chat_id": l1_id})
    if l1_doc:
        l2_id = l1_doc.get("referred_by")
        if l2_id and l2_id != new_user_id and l2_id != l1_id:
            await col_users.update_one(
                {"chat_id": l2_id},
                {"$inc": {"balance": 0.003, "referral_count_l2": 1}},
            )
            if bot:
                l2_doc_after = await col_users.find_one({"chat_id": l2_id})
                new_bal_l2   = l2_doc_after.get("balance", 0.0) if l2_doc_after else 0.0
                l1_username  = l1_doc.get("username", "Your referral")
                try:
                    await bot.send_message(
                        chat_id=l2_id,
                        text=(
                            f"🎉 *Indirect Referral Bonus!*\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"👤 {l1_username} invited {new_username}!\n\n"
                            f"💰 You earned: *+$0.003*\n"
                            f"💵 New balance: *${new_bal_l2:.3f}*\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"Encourage your referrals to share too! 🚀"
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
    await col_users.update_one({"_id": uid}, {"$set": {"referral_credited": True}})

# ===== SUBSCRIPTION CHECK =====================================================
async def check_subscription(bot, user_id: int) -> bool:
    if _member_cache_is_valid(user_id):
        return True
    for ch in REQUIRED_CHANNELS:
        target = ch.get("chat_id") or (f"@{ch['username']}" if ch.get("username") else None)
        if target is None:
            continue
        try:
            member = await bot.get_chat_member(target, user_id)
            if member.status in ("left", "kicked", "banned"):
                _member_cache_clear(user_id)
                return False
        except Exception:
            _member_cache_clear(user_id)
            return False
    _member_cache_save(user_id)
    return True

# ===== KEYBOARDS ==============================================================
def verify_keyboard():
    buttons = [[InlineKeyboardButton(ch["name"], url=ch["url"])] for ch in REQUIRED_CHANNELS]
    buttons.append([InlineKeyboardButton("✅ Verify Membership", callback_data="verify_check")])
    return InlineKeyboardMarkup(buttons)

def main_keyboard(is_admin: bool = False):
    rows = [
        [KeyboardButton("📱 NUMBER"),         KeyboardButton("🛠 SUPPORT")],
        [KeyboardButton("💰 Balance"),         KeyboardButton("👥 Invite")],
    ]
    if is_admin:
        rows += [
            [KeyboardButton("⛔ Remove Country"),  KeyboardButton("🟢 Add Country")],
            [KeyboardButton("⛔ Erase Progress"),  KeyboardButton("🔁 Status")],
            [KeyboardButton("🔝 User TOP"),         KeyboardButton("⛔ Ban User")],
            [KeyboardButton("🔼 Unban User"),       KeyboardButton("🚸 Rate Limit")],
            [KeyboardButton("🔍 Lookup User"),      KeyboardButton("📈 Growth Stats")],  # NEW
            [KeyboardButton("☢️ Broadcast")],
        ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ===== COUNTRY / NUMBER LOGIC =================================================
def detect_country(number: str):
    digits = number.lstrip("+").strip()
    for length in (3, 2, 1):
        prefix = digits[:length]
        if prefix in COUNTRY_PREFIXES:
            return COUNTRY_PREFIXES[prefix]
    return None

def country_key(name: str, flag: str) -> str:
    return f"{flag} {name}" if flag else name

async def _numbers_load() -> dict:
    return {
        doc["_id"]: {"code": doc.get("code", ""), "numbers": doc.get("numbers", [])}
        async for doc in col_numbers.find({})
    }

async def _numbers_delete(country_key_: str):
    await col_numbers.delete_one({"_id": country_key_})

async def _used_delete_many(numbers: list):
    if numbers:
        await col_used_numbers.delete_many({"_id": {"$in": numbers}})
        _invalidate_used_cache()

async def _used_delete_all():
    await col_used_numbers.delete_many({})
    _invalidate_used_cache()

async def get_available(country: str) -> list[str]:
    doc      = await col_numbers.find_one({"_id": country})
    nums     = doc.get("numbers", []) if doc else []
    used_set = await _get_used_set_cached()
    return [n for n in nums if n not in used_set]

async def mark_used(numbers: list[str], user_id: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    ops = [
        UpdateOne({"_id": n}, {"$set": {"user": user_id, "time": now}}, upsert=True)
        for n in numbers
    ]
    if ops:
        await col_used_numbers.bulk_write(ops, ordered=False)
    _invalidate_used_cache()

async def add_numbers(raw_numbers: list[str]) -> tuple[int, int]:
    added = skipped = 0
    existing = {doc["_id"]: doc async for doc in col_numbers.find({})}
    updates  = {}
    for num in raw_numbers:
        num = num.strip()
        if not num:
            continue
        result = detect_country(num)
        if not result:
            skipped += 1
            continue
        name, flag = result
        key = country_key(name, flag)
        if key not in updates:
            if key in existing:
                updates[key] = {"code": existing[key].get("code", ""), "numbers": list(existing[key]["numbers"])}
            else:
                code = ""
                for length in (3, 2, 1):
                    p = num.lstrip("+")[:length]
                    if p in COUNTRY_PREFIXES:
                        code = f"+{p}"
                        break
                updates[key] = {"code": code, "numbers": []}
        if num not in updates[key]["numbers"]:
            updates[key]["numbers"].append(num)
            added += 1
        else:
            skipped += 1
    if updates:
        ops = [UpdateOne({"_id": k}, {"$set": v}, upsert=True) for k, v in updates.items()]
        await col_numbers.bulk_write(ops, ordered=False)
    return added, skipped

def extract_numbers(text: str) -> list[str]:
    numbers = []
    for line in text.splitlines():
        clean = line.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
        if clean.lstrip("+").isdigit() and len(clean) >= 7:
            numbers.append(clean)
    return numbers

async def build_user_top() -> str:
    users    = {doc["_id"]: {k: v for k, v in doc.items() if k != "_id"}
                async for doc in col_users.find({})}
    used_col = {doc["_id"]: doc async for doc in col_used_numbers.find({})}
    db       = await _numbers_load()
    user_country: dict[str, dict[str, int]] = {}
    for num, info in used_col.items():
        uid = str(info.get("user", ""))
        if not uid:
            continue
        result = detect_country(num)
        if result:
            name, flag = result
            ck = f"{flag} {name}"
        else:
            ck = "🌐 Unknown"
            for c, v in db.items():
                if num in v.get("numbers", []):
                    ck = c
                    break
        user_country.setdefault(uid, {})[ck] = user_country[uid].get(ck, 0) + 1
    if not user_country:
        return "📭 No usage data yet."
    ranked = sorted(user_country.items(), key=lambda x: sum(x[1].values()), reverse=True)
    MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "      🔝  U S E R  T O P     ",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]
    for i, (uid, country_counts) in enumerate(ranked, 1):
        medal    = MEDALS.get(i, f"#{i} ")
        uinfo    = users.get(uid, {})
        username = uinfo.get("username", "N/A")
        chat_id  = uinfo.get("chat_id", uid)
        total    = sum(country_counts.values())
        nums_detail = "  ".join(f"{c}: `{n}`" for c, n in country_counts.items())
        lines += [
            f"{medal} *{username}*",
            f"┣ 🆔 `{chat_id}`",
            f"┣ 📱 {nums_detail}",
            f"┗ 📊 Total: *{total}*",
            "",
        ]
    lines.append(f"👥 Total ranked users: *{len(ranked)}*")
    return "\n".join(lines)

# ===== GROWTH STATS (fix #5) ==================================================
async def build_growth_stats() -> tuple[str, bytes]:
    """
    Returns (text_table: str, chart_png_bytes: bytes).
    Both last-7-days and last-30-days windows are calculated.
    To use only the text table, ignore the bytes return value.
    To use only the chart image, ignore the text return value.
    """
    today = datetime.now(timezone.utc).date()

    # Fetch all join dates for real users
    join_dates: list[str] = [
        doc["joined_date"]
        async for doc in col_users.find(
            {"_id": {"$not": {"$regex": "^__"}}, "joined_date": {"$exists": True}},
            {"joined_date": 1},
        )
    ]

    def count_by_day(days: int) -> dict[str, int]:
        start = today - timedelta(days=days - 1)
        buckets: dict[str, int] = {}
        for i in range(days):
            d = start + timedelta(days=i)
            buckets[d.strftime("%Y-%m-%d")] = 0
        for jd in join_dates:
            if jd in buckets:
                buckets[jd] += 1
        return buckets

    data_7  = count_by_day(7)
    data_30 = count_by_day(30)

    total_7  = sum(data_7.values())
    total_30 = sum(data_30.values())

    # ── Text table ────────────────────────────────────────────────────────────
    lines = [
        "📈 *User Growth Stats*",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🗓 Last 7 days  → *{total_7}* new users",
        f"🗓 Last 30 days → *{total_30}* new users",
        "",
        "📅 *Last 7 Days (daily):*",
    ]
    for date_str, count in data_7.items():
        bar = "█" * count if count <= 20 else "█" * 20 + f"(+{count-20})"
        lines.append(f"  `{date_str}` {bar or '·'} {count}")

    lines += [
        "",
        "📅 *Last 30 Days (daily):*",
    ]
    for date_str, count in data_30.items():
        bar = "█" * count if count <= 20 else "█" * 20 + f"(+{count-20})"
        lines.append(f"  `{date_str}` {bar or '·'} {count}")

    text_table = "\n".join(lines)

    # ── Bar chart image ────────────────────────────────────────────────────────
    fig, axes = plt.subplots(2, 1, figsize=(10, 8), facecolor="#1e1e2e")
    fig.suptitle("User Growth", color="white", fontsize=14, fontweight="bold")

    for ax, data, label in [
        (axes[0], data_7,  "Last 7 Days"),
        (axes[1], data_30, "Last 30 Days"),
    ]:
        dates  = list(data.keys())
        counts = list(data.values())
        bars   = ax.bar(dates, counts, color="#7c3aed", edgecolor="#a78bfa", linewidth=0.6)
        ax.set_facecolor("#2a2a3e")
        ax.set_title(label, color="white", fontsize=11)
        ax.tick_params(colors="white", labelsize=7)
        ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        for spine in ax.spines.values():
            spine.set_edgecolor("#555")
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        for bar, val in zip(bars, counts):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.05,
                    str(val),
                    ha="center", va="bottom", color="white", fontsize=7,
                )

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    buf.seek(0)
    chart_bytes = buf.read()

    return text_table, chart_bytes

# ===== USER LOOKUP (fix #3) ===================================================
async def lookup_user(query_str: str) -> dict | None:
    """
    Find a user by chat_id (integer) or by @username (case-insensitive).
    Returns the raw MongoDB document or None.
    """
    # Try chat_id first
    try:
        cid = int(query_str)
        doc = await col_users.find_one({"chat_id": cid, "_id": {"$not": {"$regex": "^__"}}})
        if doc:
            return doc
    except ValueError:
        pass
    # Try username (strip @ if present, case-insensitive)
    uname = query_str.strip()
    if not uname.startswith("@"):
        uname = "@" + uname
    doc = await col_users.find_one(
        {"username": {"$regex": f"^{uname}$", "$options": "i"},
         "_id": {"$not": {"$regex": "^__"}}}
    )
    return doc

def format_user_lookup(doc: dict) -> str:
    banned_marker = "🚫 YES" if False else "✅ NO"  # placeholder — resolved below
    return ""  # built inline where ban status is available

def build_lookup_message(doc: dict, banned: bool) -> str:
    return (
        f"🔍 *User Lookup Result*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Username     : {doc.get('username', 'N/A')}\n"
        f"🆔 Chat ID      : `{doc.get('chat_id', 'N/A')}`\n"
        f"📅 Joined       : {doc.get('joined_date', 'N/A')}\n"
        f"💵 Balance      : *${doc.get('balance', 0.0):.3f}*\n"
        f"👥 L1 Referrals : {doc.get('referral_count_l1', 0)}\n"
        f"👥 L2 Referrals : {doc.get('referral_count_l2', 0)}\n"
        f"⏳ Pending WD   : {'Yes' if doc.get('withdrawal_pending') else 'No'}\n"
        f"🚫 Banned       : {'🚫 YES' if banned else '✅ NO'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

def lookup_action_keyboard(chat_id: int, banned: bool) -> InlineKeyboardMarkup:
    ban_btn = (
        InlineKeyboardButton("🔼 Unban", callback_data=f"lu_unban_{chat_id}")
        if banned
        else InlineKeyboardButton("⛔ Ban",   callback_data=f"lu_ban_{chat_id}")
    )
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add Balance",    callback_data=f"lu_addbal_{chat_id}"),
            InlineKeyboardButton("➖ Deduct Balance", callback_data=f"lu_dedbal_{chat_id}"),
        ],
        [ban_btn],
    ])

# ===== BALANCE ADJUSTMENT HELPERS (fix #4) ====================================
def balance_confirm_keyboard(action: str, chat_id: int, amount: float) -> InlineKeyboardMarkup:
    """action: 'add' or 'ded'"""
    sign = "+" if action == "add" else "-"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"✅ Confirm {sign}${amount:.2f}",
                callback_data=f"lu_balconfirm_{action}_{chat_id}_{amount}",
            ),
            InlineKeyboardButton("❌ Cancel", callback_data=f"lu_balcancel_{chat_id}"),
        ]
    ])

# ===== HANDLERS ===============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS and await is_banned(user.id):
        await update.message.reply_text(
            f"🚫 *You are banned.*\n\nContact {SUPPORT_HANDLE} to appeal.",
            parse_mode="Markdown",
        )
        return
    referred_by = None
    if context.args:
        payload = context.args[0]
        if payload.startswith("ref_"):
            try:
                ref_id = int(payload[4:])
                if ref_id != user.id:
                    referred_by = ref_id
            except ValueError:
                pass
    await save_user(user, referred_by=referred_by)
    is_admin = user.id in ADMIN_IDS
    if not is_admin:
        verified = await check_subscription(context.bot, user.id)
        if not verified:
            await update.message.reply_text(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "  📲 HN NUMBER BOT  \n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚠️ You must join *all* required channels\n"
                "before using this bot.\n\n"
                "1️⃣  Click each channel link below\n"
                "2️⃣  Join the channel\n"
                "3️⃣  Press ✅ *Verify Membership*",
                parse_mode="Markdown",
                reply_markup=verify_keyboard(),
            )
            return
    name = user.first_name or "there"
    await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"      HN NUMBER BOT  \n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Welcome back, *{name}*!\n\n"
        f"Choose an option from the menu below 👇",
        parse_mode="Markdown",
        reply_markup=main_keyboard(is_admin=is_admin),
    )

async def verify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _member_cache_clear(query.from_user.id)
    try:
        verified = await check_subscription(context.bot, query.from_user.id)
        if verified:
            await query.message.delete()
            await credit_referral(query.from_user.id, bot=context.bot)
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text="✅ *Verified!* You now have full access.\n\nChoose an option from the menu below 👇",
                parse_mode="Markdown",
                reply_markup=main_keyboard(is_admin=False),
            )
        else:
            await query.answer(
                "❌ You haven't joined all required channels yet.\nPlease join all channels and try again.",
                show_alert=True,
            )
    except Exception:
        await query.answer("❌ Please join all required channels first.", show_alert=True)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type != "private":
        return
    user = update.effective_user
    text = (update.message.text or "").strip()

    if user.id not in ADMIN_IDS and await is_banned(user.id):
        await update.message.reply_text(
            f"🚫 *You are banned.*\n\nContact {SUPPORT_HANDLE} to appeal.",
            parse_mode="Markdown",
        )
        return

    if user.id in ADMIN_IDS:
        # ── awaiting lookup query ──────────────────────────────────────────
        if context.user_data.get("awaiting_lookup"):
            context.user_data["awaiting_lookup"] = False
            doc = await lookup_user(text)
            if not doc:
                await update.message.reply_text(
                    f"❌ No user found for `{text}`.\n"
                    f"Try a numeric chat ID or @username.",
                    parse_mode="Markdown",
                    reply_markup=main_keyboard(is_admin=True),
                )
                return
            cid     = doc.get("chat_id", 0)
            banned  = str(cid) in await load_blacklist()
            msg     = build_lookup_message(doc, banned)
            await update.message.reply_text(
                msg,
                parse_mode="Markdown",
                reply_markup=lookup_action_keyboard(cid, banned),
            )
            return
        # ── awaiting balance amount (add or deduct) ───────────────────────
        if context.user_data.get("awaiting_balance_input"):
            state = context.user_data.pop("awaiting_balance_input")
            action, target_cid = state["action"], state["chat_id"]
            try:
                amount = float(text)
                if amount <= 0:
                    raise ValueError
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid amount. Send a positive number, e.g. `5.00`",
                    parse_mode="Markdown",
                )
                return
            sign = "+" if action == "add" else "-"
            await update.message.reply_text(
                f"⚠️ *Confirm Balance Adjustment*\n\n"
                f"🆔 Chat ID : `{target_cid}`\n"
                f"💵 Amount  : *{sign}${amount:.2f}*\n\n"
                f"Proceed?",
                parse_mode="Markdown",
                reply_markup=balance_confirm_keyboard(action, target_cid, amount),
            )
            return
        # ── ban / unban / numbers / broadcast (unchanged flow) ─────────────
        if context.user_data.get("awaiting_ban_id"):
            context.user_data["awaiting_ban_id"] = False
            try:
                target_id = int(text)
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid chat ID. Must be a plain number, e.g. `123456789`",
                    parse_mode="Markdown",
                )
                return
            result = await ban_user(target_id)
            if result == "admin":
                msg = "⛔ *Cannot ban an admin.*"
            elif result == "already":
                msg = f"⚠️ User `{target_id}` is *already banned*."
            else:
                msg = f"✅ User `{target_id}` has been *banned*."
            await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=main_keyboard(is_admin=True))
            return

        if context.user_data.get("awaiting_unban_id"):
            context.user_data["awaiting_unban_id"] = False
            try:
                target_id = int(text)
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid chat ID. Must be a plain number, e.g. `123456789`",
                    parse_mode="Markdown",
                )
                return
            if await unban_user(target_id):
                await update.message.reply_text(f"✅ User `{target_id}` has been *unbanned*.", parse_mode="Markdown", reply_markup=main_keyboard(is_admin=True))
            else:
                await update.message.reply_text(f"⚠️ User `{target_id}` was *not* in the ban list.", parse_mode="Markdown", reply_markup=main_keyboard(is_admin=True))
            return

        if context.user_data.get("awaiting_numbers"):
            numbers = extract_numbers(text)
            if numbers:
                added, skipped = await add_numbers(numbers)
                context.user_data["awaiting_numbers"] = False
                await update.message.reply_text(
                    f"✅ *Upload Complete*\n\n➕ Added: `{added}`\n📛 Skipped: `{skipped}`",
                    parse_mode="Markdown",
                    reply_markup=main_keyboard(is_admin=True),
                )
            else:
                await update.message.reply_text("❌ No valid numbers found. Try again or send a .txt file.")
            return

        if context.user_data.get("awaiting_broadcast"):
            context.user_data["awaiting_broadcast"] = False
            context.user_data["broadcast_message"]  = text
            await update.message.reply_text(
                f"☢️ *Broadcast Confirmation*\n\n"
                f"Do you want to send this message to your users?\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n{text}\n━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("🟢 YES", callback_data="broadcast_confirm"),
                        InlineKeyboardButton("🔴 NO",  callback_data="broadcast_cancel"),
                    ]
                ]),
            )
            return

    # ── Non-admin rate limit ───────────────────────────────────────────────────
    if user.id not in ADMIN_IDS:
        rl_on = await is_rate_limit_on()
        if rl_on:
            wait = check_rate_limit(user.id)
            if wait > 0:
                await update.message.reply_text(f"⏳ Please wait *{wait}s* before sending another command.", parse_mode="Markdown")
                return
        update_rate_timestamp(user.id)

    is_admin = user.id in ADMIN_IDS
    text_up  = text.upper()

    if "NUMBER" in text_up and not any(x in text_up for x in ["REMOVE", "ADD", "ERASE"]):
        if user.id not in ADMIN_IDS:
            verified = await check_subscription(context.bot, user.id)
            if not verified:
                await update.message.reply_text(
                    "🔒 *Access Restricted*\n\n"
                    "⚠️ You must join *all* required channels\n"
                    "before using this bot.\n\n"
                    "1️⃣  Click each channel link below\n"
                    "2️⃣  Join the channel\n"
                    "3️⃣  Press ✅ *Verify Membership*",
                    parse_mode="Markdown",
                    reply_markup=verify_keyboard(),
                )
                return
        await show_countries(update)
    elif "CHANNEL" in text_up:
        await update.message.reply_text(
            "👑 *Main Channel*\n\nTap the button below to open:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👑 Open Main Channel", url=CHANNEL_LINK)]]),
        )
    elif "GROUP" in text_up:
        await update.message.reply_text(
            "💬 *OTP GROUP*\n\nTap the button below to open:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💬 OTP GROUP", url=SMS_GROUP_LINK)]]),
        )
    elif "SUPPORT" in text_up:
        await update.message.reply_text(
            "🛠 *Support*\n\nTap the button below to contact our admin:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛠 Contact Support", url=f"https://t.me/{SUPPORT_HANDLE.lstrip('@')}")]]),
        )
    elif "INVITE" in text_up:
        uid  = str(user.id)
        udoc = await col_users.find_one({"_id": uid}) or {}
        l1c  = udoc.get("referral_count_l1", 0)
        l2c  = udoc.get("referral_count_l2", 0)
        top3 = await col_users.find(
            {"referral_count_l1": {"$gt": 0}},
            {"username": 1, "referral_count_l1": 1},
        ).sort("referral_count_l1", -1).limit(3).to_list(length=3)
        medals   = ["🥇", "🥈", "🥉"]
        lb_lines = [f"{medals[i]} {tdoc.get('username','N/A')} — {tdoc.get('referral_count_l1',0)} referrals" for i, tdoc in enumerate(top3)]
        if not lb_lines:
            lb_lines = ["No referrals yet — be the first! 🚀"]
        all_ranked = await col_users.find({"referral_count_l1": {"$gt": 0}}, {"chat_id": 1}).sort("referral_count_l1", -1).to_list(length=None)
        rank       = next((i + 1 for i, d in enumerate(all_ranked) if d.get("chat_id") == user.id), None)
        rank_str   = f"#{rank}" if rank else "Unranked"
        invite_link = f"https://t.me/HN_NUMBER_BOT?start=ref_{user.id}"
        lines = [
            "👥 REFERRAL LEADERBOARD",
            "━━━━━━━━━━━━━━━━━━━━",
        ] + lb_lines + [
            "━━━━━━━━━━━━━━━━━━━━",
            f"📊 Your Rank: {rank_str}",
            f"👥 Your Referrals: {l1c} direct | {l2c} indirect",
            "",
            "💰 Earnings per invite:",
            "├ Direct referral: +$0.01",
            "└ Their referral: +$0.003",
            "",
            "🔗 Your Invite Link:", invite_link,
            "",
            "📢 Share this link with friends!",
            "Every verified user you invite earns you $0.01",
            "And when they invite someone you earn $0.003 bonus!",
        ]
        await update.message.reply_text("\n".join(lines), parse_mode=None, reply_markup=main_keyboard(is_admin=is_admin))
    elif "BALANCE" in text_up:
        uid  = str(user.id)
        udoc = await col_users.find_one({"_id": uid}) or {}
        bal  = udoc.get("balance", 0.0)
        l1c  = udoc.get("referral_count_l1", 0)
        l2c  = udoc.get("referral_count_l2", 0)
        l1_earn = round(l1c * 0.01, 3)
        l2_earn = round(l2c * 0.003, 3)
        joined  = udoc.get("joined_date", "N/A")
        min_wd  = 0.2
        lines   = [
            "💰 YOUR BALANCE",
            "━━━━━━━━━━━━━━━━━━━━",
            f"💵 Total Earnings:     ${bal:.2f}",
            f"├ 🥇 Level 1 Earnings: ${l1_earn:.3f}",
            f"└ 🥈 Level 2 Earnings: ${l2_earn:.3f}",
            "",
            "👥 Your Referrals",
            f"├ Direct (L1): {l1c} users",
            f"└ Indirect (L2): {l2c} users",
            "",
            f"📅 Member since: {joined}",
            "━━━━━━━━━━━━━━━━━━━━",
            f"💸 Minimum withdrawal: ${min_wd:.2f}",
        ]
        if bal >= min_wd:
            lines += [f"👉 To withdraw contact {SUPPORT_BOT}", f"    and provide your Chat ID: `{user.id}`"]
        else:
            needed    = round(min_wd - bal, 2)
            more_refs = max(0, int((needed / 0.01) + 0.9999))
            lines += [f"⏳ You need ${needed:.2f} more to withdraw", f"    ({more_refs} more Level 1 referrals needed)"]
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=main_keyboard(is_admin=is_admin))
    elif is_admin and "REMOVE COUNTRY" in text_up:
        await admin_remove_country_menu(update)
    elif is_admin and "ADD COUNTRY" in text_up:
        context.user_data["awaiting_numbers"] = True
        await update.message.reply_text(
            "📂 *Add Numbers*\n\nSend a `.txt` file or paste numbers directly (one per line).\nCountry is auto-detected from the prefix.",
            parse_mode="Markdown",
        )
    elif is_admin and "ERASE PROGRESS" in text_up:
        await admin_erase_progress_menu(update)
    elif is_admin and "STATUS" in text_up:
        await admin_status(update)
    elif is_admin and "USER TOP" in text_up:
        report = await build_user_top()
        await update.message.reply_text(report, parse_mode="Markdown", reply_markup=main_keyboard(is_admin=True))
    elif is_admin and "LOOKUP USER" in text_up:
        context.user_data["awaiting_lookup"] = True
        await update.message.reply_text(
            "🔍 *User Lookup*\n\nSend the user's *Chat ID* (number) or *@username*:",
            parse_mode="Markdown",
        )
    elif is_admin and "GROWTH STATS" in text_up:
        await admin_growth_stats(update, context)
    elif is_admin and "UNBAN" in text_up and "USER" in text_up:
        context.user_data["awaiting_unban_id"] = True
        await update.message.reply_text("🔼 *Unban User*\n\nSend the *Chat ID* of the user you want to unban:", parse_mode="Markdown")
    elif is_admin and "BAN USER" in text_up:
        context.user_data["awaiting_ban_id"] = True
        await update.message.reply_text("⛔ *Ban User*\n\nSend the *Chat ID* of the user you want to ban:", parse_mode="Markdown")
    elif is_admin and "BROADCAST" in text_up:
        context.user_data["awaiting_broadcast"] = True
        await update.message.reply_text("☢️ *Broadcast*\n\nSend the message you want to broadcast to all users:", parse_mode="Markdown")
    elif is_admin and "RATE LIMIT" in text_up:
        current     = await is_rate_limit_on()
        state_label = "🟢 ON" if current else "🔴 OFF"
        await update.message.reply_text(
            f"🚸 *Rate Limit*\n\nCurrent status: *{state_label}*\n\n"
            f"When ON: users must wait {RATE_LIMIT_SECS}s between commands.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🟢 ON",  callback_data="ratelimit_on"),
                    InlineKeyboardButton("🔴 OFF", callback_data="ratelimit_off"),
                ]
            ]),
        )

# ===== ADMIN GROWTH STATS HANDLER (fix #5) ====================================
async def admin_growth_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Fetching growth data...", parse_mode="Markdown")
    text_table, chart_bytes = await build_growth_stats()

    # ── Text table (comment out the next two lines to disable) ─────────────────
    await update.message.reply_text(text_table, parse_mode="Markdown", reply_markup=main_keyboard(is_admin=True))

    # ── Bar chart image (comment out the next two lines to disable) ───────────
    await update.message.reply_photo(
        photo=io.BytesIO(chart_bytes),
        caption="📈 User Growth Chart",
        reply_markup=main_keyboard(is_admin=True),
    )

# ===== ADMIN MENUS ============================================================
async def admin_remove_country_menu(update: Update):
    db = await _numbers_load()
    if not db:
        await update.message.reply_text("📭 No countries in database.")
        return
    buttons = [
        [InlineKeyboardButton(f"⛔ {c}  ({len(v.get('numbers', []))} nums)", callback_data=f"rm_{c}")]
        for c, v in db.items()
    ]
    await update.message.reply_text(
        "⛔ *Remove Country*\n\nSelect a country to delete all its numbers:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def admin_erase_progress_menu(update: Update):
    used_col = await col_used_numbers.find({}).to_list(length=None)
    db       = await _numbers_load()
    if not used_col:
        await update.message.reply_text("📭 No usage progress to erase.")
        return
    country_usage: dict[str, int] = {}
    for doc in used_col:
        num    = doc["_id"]
        result = detect_country(num)
        if result:
            name, flag = result
            ck = country_key(name, flag)
        else:
            ck = "Unknown"
            for c, v in db.items():
                if num in v.get("numbers", []):
                    ck = c
                    break
        country_usage[ck] = country_usage.get(ck, 0) + 1
    buttons = [
        [InlineKeyboardButton(f"🗑 {c}  ({n} used)", callback_data=f"eraseprog_{c}")]
        for c, n in country_usage.items()
    ]
    buttons.append([InlineKeyboardButton("🗑 ERASE ALL PROGRESS", callback_data="eraseprog_ALL")])
    await update.message.reply_text(
        "⛔ *Erase Progress*\n\nSelect a country to clear its used-numbers record\n(numbers become available again):",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def admin_status(update: Update):
    users    = {doc["_id"]: doc async for doc in col_users.find({})}
    users    = {k: v for k, v in users.items() if not k.startswith("__")}
    db       = await _numbers_load()
    used_set = await _get_used_set_cached()
    total    = sum(len(v.get("numbers", [])) for v in db.values())
    avail    = total - len(used_set)
    rl_state = "🟢 ON" if await is_rate_limit_on() else "🔴 OFF"
    bl_count = len(await load_blacklist())
    lines = [
        "📊 *Bot Status*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"👥 Total Users:    `{len(users)}`",
        f"📱 Total Numbers:  `{total}`",
        f"✅ Available:      `{avail}`",
        f"📛 Used:           `{len(used_set)}`",
        f"🚫 Banned Users:   `{bl_count}`",
        f"🚸 Rate Limit:     {rl_state}",
        "",
        "🌍 *Per Country:*",
    ]
    for c, v in db.items():
        nums = v.get("numbers", [])
        av   = sum(1 for n in nums if n not in used_set)
        pct  = f"{av/len(nums)*100:.0f}%" if nums else "—"
        lines.append(f"  {c}: `{av}/{len(nums)}` ({pct})")
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=main_keyboard(is_admin=True),
    )

async def show_countries(update: Update):
    db = await _numbers_load()
    if not db:
        await update.message.reply_text(
            "📭 *No numbers available right now.*\nCheck back soon!",
            parse_mode="Markdown",
            reply_markup=main_keyboard(is_admin=update.effective_user.id in ADMIN_IDS),
        )
        return
    country_list = list(db.keys())
    buttons = [
        [InlineKeyboardButton(country_list[i + j], callback_data=f"country_{country_list[i + j]}")
         for j in range(3) if i + j < len(country_list)]
        for i in range(0, len(country_list), 3)
    ]
    await update.message.reply_text(
        "🌍 *Select a Country*\n\nChoose from the list below:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

# ===== CALLBACK HANDLERS ======================================================
@admin_query_with_sub_check
async def country_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    country = query.data[len("country_"):]
    await send_batch(query.message, context, query.from_user.id, country, edit=True)

@admin_query_with_sub_check
async def change_country_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    db    = await _numbers_load()
    if not db:
        await query.edit_message_text("📭 No numbers available at the moment.")
        return
    country_list = list(db.keys())
    buttons = [
        [InlineKeyboardButton(country_list[i + j], callback_data=f"country_{country_list[i + j]}")
         for j in range(3) if i + j < len(country_list)]
        for i in range(0, len(country_list), 3)
    ]
    await query.edit_message_text(
        "🌍 *Select a Country*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

@admin_query_with_sub_check
async def new_batch_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    country = query.data[len("newbatch_"):]
    await send_batch(query.message, context, query.from_user.id, country, edit=True)

@admin_only
async def rm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    country = query.data[len("rm_"):]
    if await col_numbers.find_one({"_id": country}):
        await _numbers_delete(country)
        await query.edit_message_text(f"✅ All numbers for *{country}* have been deleted.", parse_mode="Markdown")
    else:
        await query.edit_message_text("❌ Country not found.")

@admin_only
async def erase_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    target = query.data[len("eraseprog_"):]
    db     = await _numbers_load()
    if target == "ALL":
        await _used_delete_all()
        await query.edit_message_text("✅ *All* used-number progress has been erased.", parse_mode="Markdown")
        return
    used_docs = await col_used_numbers.find({}).to_list(length=None)
    to_remove = []
    for doc in used_docs:
        num    = doc["_id"]
        result = detect_country(num)
        if result:
            name, flag = result
            ck = country_key(name, flag)
        else:
            ck = "Unknown"
            for c, v in db.items():
                if num in v.get("numbers", []):
                    ck = c
                    break
        if ck == target:
            to_remove.append(num)
    await _used_delete_many(to_remove)
    await query.edit_message_text(
        f"✅ Progress for *{target}* erased. `{len(to_remove)}` numbers are available again.",
        parse_mode="Markdown",
    )

@admin_only
async def rate_limit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    enable = query.data == "ratelimit_on"
    await set_rate_limit(enable)
    state = "🟢 ON" if enable else "🔴 OFF"
    await query.edit_message_text(
        f"🚸 *Rate Limit* is now *{state}*\n\n"
        f"{'Users must wait ' + str(RATE_LIMIT_SECS) + 's between commands.' if enable else 'No delay between commands.'}",
        parse_mode="Markdown",
    )

@admin_only
async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "broadcast_cancel":
        await query.edit_message_text("🔴 *Broadcast cancelled.*", parse_mode="Markdown")
        return
    msg_text = context.user_data.pop("broadcast_message", None)
    if not msg_text:
        await query.edit_message_text("❌ No message found. Please try again.", parse_mode="Markdown")
        return
    users = {doc["_id"]: doc async for doc in col_users.find({})}
    users = {k: v for k, v in users.items() if not k.startswith("__")}

    async def send_one(chat_id):
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg_text, parse_mode="Markdown")
            return True
        except Exception:
            return False

    results = await asyncio.gather(*[send_one(info["chat_id"]) for info in users.values() if info.get("chat_id")])
    sent    = sum(1 for r in results if r)
    failed  = sum(1 for r in results if not r)
    await query.edit_message_text(
        f"✅ *Broadcast Complete*\n\n📨 Sent: `{sent}`\n❌ Failed: `{failed}`",
        parse_mode="Markdown",
    )

# ===== LOOKUP ACTION CALLBACKS (fix #3 & #4) ==================================
async def lookup_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles lu_ban_, lu_unban_, lu_addbal_, lu_dedbal_, lu_balconfirm_, lu_balcancel_"""
    query   = update.callback_query
    user_id = query.from_user.id
    if user_id not in ADMIN_IDS:
        await query.answer("🚫 Not authorized.", show_alert=True)
        return
    await query.answer()
    data = query.data  # e.g. "lu_ban_12345"

    # ── Ban / Unban from lookup ────────────────────────────────────────────────
    if data.startswith("lu_ban_"):
        target_cid = int(data.split("_", 2)[2])
        result = await ban_user(target_cid)
        if result == "admin":
            await query.edit_message_text("⛔ *Cannot ban an admin.*", parse_mode="Markdown")
        elif result == "already":
            await query.edit_message_text(f"⚠️ User `{target_cid}` is *already banned*.", parse_mode="Markdown")
        else:
            # Refresh lookup card
            doc    = await col_users.find_one({"chat_id": target_cid})
            if doc:
                msg = build_lookup_message(doc, banned=True)
                await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=lookup_action_keyboard(target_cid, banned=True))
            else:
                await query.edit_message_text(f"✅ User `{target_cid}` banned.", parse_mode="Markdown")
        return

    if data.startswith("lu_unban_"):
        target_cid = int(data.split("_", 2)[2])
        ok = await unban_user(target_cid)
        doc = await col_users.find_one({"chat_id": target_cid})
        if ok and doc:
            msg = build_lookup_message(doc, banned=False)
            await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=lookup_action_keyboard(target_cid, banned=False))
        else:
            await query.edit_message_text(f"⚠️ User `{target_cid}` was not in the ban list.", parse_mode="Markdown")
        return

    # ── Add / Deduct balance triggers ─────────────────────────────────────────
    if data.startswith("lu_addbal_") or data.startswith("lu_dedbal_"):
        action     = "add" if data.startswith("lu_addbal_") else "ded"
        target_cid = int(data.split("_", 2)[2].split("_")[-1])
        # Store state so handle_message picks it up
        context.user_data["awaiting_balance_input"] = {"action": action, "chat_id": target_cid}
        sign = "add to" if action == "add" else "deduct from"
        await query.message.reply_text(
            f"💵 How much USD do you want to *{sign}* balance of `{target_cid}`?\n\nSend a number, e.g. `5.00`",
            parse_mode="Markdown",
        )
        return

    # ── Cancel balance adjustment ──────────────────────────────────────────────
    if data.startswith("lu_balcancel_"):
        context.user_data.pop("awaiting_balance_input", None)
        target_cid = int(data.split("_", 2)[2])
        doc    = await col_users.find_one({"chat_id": target_cid})
        banned = str(target_cid) in await load_blacklist()
        if doc:
            msg = build_lookup_message(doc, banned)
            await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=lookup_action_keyboard(target_cid, banned))
        else:
            await query.edit_message_text("❌ Cancelled.", parse_mode="Markdown")
        return

    # ── Confirm balance adjustment ─────────────────────────────────────────────
    if data.startswith("lu_balconfirm_"):
        # format: lu_balconfirm_{action}_{chat_id}_{amount}
        parts      = data.split("_")
        # parts: ['lu', 'balconfirm', action, chat_id, amount]
        action     = parts[2]
        target_cid = int(parts[3])
        amount     = float(parts[4])
        delta      = amount if action == "add" else -amount
        await col_users.update_one({"chat_id": target_cid}, {"$inc": {"balance": delta}})
        doc_after  = await col_users.find_one({"chat_id": target_cid})
        new_bal    = doc_after.get("balance", 0.0) if doc_after else 0.0
        sign       = "+" if delta > 0 else ""
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=target_cid,
                text=(
                    f"💵 *Balance Updated*\n\n"
                    f"{'➕ Added' if action == 'add' else '➖ Deducted'}: *${amount:.2f}*\n"
                    f"💰 New balance: *${new_bal:.3f}*"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            pass
        # Refresh lookup card
        banned = str(target_cid) in await load_blacklist()
        if doc_after:
            msg = build_lookup_message(doc_after, banned)
            await query.edit_message_text(
                f"✅ Balance adjusted ({sign}${amount:.2f}). New balance: *${new_bal:.3f}*\n\n" + msg,
                parse_mode="Markdown",
                reply_markup=lookup_action_keyboard(target_cid, banned),
            )
        else:
            await query.edit_message_text(f"✅ Balance adjusted ({sign}${amount:.2f}).", parse_mode="Markdown")
        return

# ===== SEND BATCH =============================================================
async def send_batch(message, context, user_id: int, country: str, edit: bool = False):
    _invalidate_used_cache()
    available = await get_available(country)
    if not available:
        text   = f"📭 *No numbers available* for {country} right now.\nTry another country."
        kwargs = {"parse_mode": "Markdown"}
        if edit:
            await message.edit_text(text, **kwargs)
        else:
            await message.reply_text(text, reply_markup=main_keyboard(is_admin=user_id in ADMIN_IDS), **kwargs)
        return
    batch = random.sample(available, min(BATCH_SIZE, len(available)))
    await mark_used(batch, user_id)
    sms_watch_set(user_id, batch)
    text = f"  ✅ *Your Numbers* — {country}\n"
    copy_buttons = [
        [InlineKeyboardButton(f" +{n}", copy_text=CopyTextButton(text=f"+{n}"))]
        for n in batch
    ]
    buttons = InlineKeyboardMarkup(
        copy_buttons + [
            [
                InlineKeyboardButton("🌍 Change Country", callback_data="change_country"),
                InlineKeyboardButton("🔄 New Batch",      callback_data=f"newbatch_{country}"),
            ],
            [InlineKeyboardButton("📩 OTP Group", url=SMS_GROUP_LINK)],
        ]
    )
    kwargs = {"parse_mode": "Markdown", "reply_markup": buttons}
    if edit:
        await message.edit_text(text, **kwargs)
    else:
        await message.reply_text(text, **kwargs)
    await context.bot.send_message(
        chat_id=user_id,
        text="\u200b",
        reply_markup=main_keyboard(is_admin=user_id in ADMIN_IDS),
    )

# ===== DOCUMENT HANDLER =======================================================
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type != "private":
        return
    user = update.effective_user
    if user.id not in ADMIN_IDS or not context.user_data.get("awaiting_numbers"):
        return
    doc = update.message.document
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("⚠️ Please send a `.txt` file.", parse_mode="Markdown")
        return
    file    = await context.bot.get_file(doc.file_id)
    content = await file.download_as_bytearray()
    text    = content.decode("utf-8", errors="ignore")
    numbers = extract_numbers(text)
    if not numbers:
        await update.message.reply_text("❌ No valid numbers found in the file.")
        context.user_data["awaiting_numbers"] = False
        return
    added, skipped = await add_numbers(numbers)
    context.user_data["awaiting_numbers"] = False
    await update.message.reply_text(
        f"✅ *Upload Complete*\n\n➕ Added: `{added}`\n📛 Skipped: `{skipped}`",
        parse_mode="Markdown",
        reply_markup=main_keyboard(is_admin=True),
    )

# ===== COMMAND HANDLERS =======================================================
async def admin_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("🚫 Not authorized.")
        return
    context.user_data["awaiting_numbers"] = True
    await update.message.reply_text(
        "📂 *Add Numbers*\n\nSend a `.txt` file or paste numbers directly (one per line).",
        parse_mode="Markdown",
    )

async def admin_rm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("🚫 Not authorized.")
        return
    await admin_remove_country_menu(update)

async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("🚫 Not authorized.")
        return
    await admin_status(update)

async def admin_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /paid <chat_id> <amount>
    Logs a withdrawal payment, resets balance to 0, and notifies the user.
    Kept alongside the new inline balance tools for convenience.
    """
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("🚫 Not authorized.")
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "⚠️ Usage: `/paid <chat_id> <amount>`\nExample: `/paid 123456789 1.05`",
            parse_mode="Markdown",
        )
        return
    try:
        target_chat_id = int(args[0])
        amount         = float(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid chat_id or amount.")
        return
    user_doc = await col_users.find_one({"chat_id": target_chat_id})
    if not user_doc:
        await update.message.reply_text(f"❌ No user found with chat_id `{target_chat_id}`.", parse_mode="Markdown")
        return
    await col_withdrawals.insert_one({
        "user_chat_id":  target_chat_id,
        "amount":        amount,
        "paid_at":       datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "paid_by_admin": update.effective_user.id,
    })
    await col_users.update_one(
        {"chat_id": target_chat_id},
        {"$set": {"balance": 0.0, "withdrawal_pending": False}},
    )
    try:
        await context.bot.send_message(
            chat_id=target_chat_id,
            text=f"✅ Your withdrawal of *${amount:.2f}* has been processed. Thank you!",
            parse_mode="Markdown",
        )
    except Exception:
        pass
    await update.message.reply_text(
        f"✅ Payment of *${amount:.2f}* logged for `{target_chat_id}`. Balance reset to $0.00.",
        parse_mode="Markdown",
    )

async def admin_lookup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/lookup <chat_id or @username>"""
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("🚫 Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/lookup <chat_id or @username>`", parse_mode="Markdown")
        return
    doc = await lookup_user(context.args[0])
    if not doc:
        await update.message.reply_text(f"❌ No user found for `{context.args[0]}`.", parse_mode="Markdown")
        return
    cid    = doc.get("chat_id", 0)
    banned = str(cid) in await load_blacklist()
    await update.message.reply_text(
        build_lookup_message(doc, banned),
        parse_mode="Markdown",
        reply_markup=lookup_action_keyboard(cid, banned),
    )

# ===== MAIN ===================================================================
def main():
    print("🤖 Starting bot...")
    app = Application.builder().token(BOT_TOKEN).build()

    async def on_startup(app):
        await ping_db()
        # Patch missing fields on existing users (async)
        await col_users.update_many(
            {"balance": {"$exists": False}},
            {"$set": {
                "balance": 0.0,
                "referral_count_l1": 0,
                "referral_count_l2": 0,
                "referred_by": None,
                "referral_credited": False,
                "withdrawal_pending": False,
            }},
        )
        asyncio.create_task(sms_poll_loop(app.bot))

    app.post_init = on_startup

    # Commands
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("add",    admin_add))
    app.add_handler(CommandHandler("rm",     admin_rm))
    app.add_handler(CommandHandler("stats",  admin_stats_cmd))
    app.add_handler(CommandHandler("paid",   admin_paid))
    app.add_handler(CommandHandler("lookup", admin_lookup_cmd))   # NEW

    # Callbacks
    app.add_handler(CallbackQueryHandler(verify_callback,         pattern="^verify_check$"))
    app.add_handler(CallbackQueryHandler(country_callback,        pattern="^country_"))
    app.add_handler(CallbackQueryHandler(change_country_callback, pattern="^change_country$"))
    app.add_handler(CallbackQueryHandler(new_batch_callback,      pattern="^newbatch_"))
    app.add_handler(CallbackQueryHandler(rm_callback,             pattern="^rm_"))
    app.add_handler(CallbackQueryHandler(erase_progress_callback, pattern="^eraseprog_"))
    app.add_handler(CallbackQueryHandler(rate_limit_callback,     pattern="^ratelimit_"))
    app.add_handler(CallbackQueryHandler(broadcast_callback,      pattern="^broadcast_"))
    app.add_handler(CallbackQueryHandler(lookup_action_callback,  pattern="^lu_"))   # NEW

    # Messages
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🤖 Bot is running.")
    app.run_polling(drop_pending_updates=True)

main()
