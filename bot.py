import asyncio
import aiohttp
import html
import json
import logging
import re
import random
import sys
from datetime import date, timedelta, datetime
import time
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.filters.state import StateFilter
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import BaseMiddleware
from collections import defaultdict, deque
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    FSInputFile,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    BufferedInputFile
)

from config import (
    TIMEZONE, PUBLISH_HOUR, PUBLISH_MINUTE, PUBLISH_WARNING_HOURS,
    ADMIN_IDS, OWNER_ID,
    BOT_TOKEN,
    DATABASE_PATH,
    FF_API_KEY,
    FF_API_PROVIDER,
    FF_API_BASE,
    FF_GUILD_ID,
    FF_REGION,
    GUILD_CHAT_ID, MONITORING_CHAT_ID,
    LOW_ACTIVITY_LIMIT,
    HIGH_ACTIVITY_LIMIT,
    get_rank, REFERRAL_COINS_PER_INVITE, ACTIVITY_COINS_PER_100, ACTIVITY_REPORT_INTERVAL_HOURS,
    DIAMOND_EXCHANGE_COINS, DIAMOND_EXCHANGE_AMOUNT, UNBAN_EXCHANGE_COINS,
    SHOP_LITE_COINS, SHOP_CLASSIC_COINS, SHOP_PRO_COINS, SHOP_KB_COINS, SHOP_BO_COINS,
    SHOP_LVL_BOT_COINS, SHOP_ALL_LOCKS_COINS, SHOP_UNMUTE_COINS, SHOP_UNWARN_COINS,
    SUMMON_COOLDOWN_SECONDS, WELCOME_ENABLED, RULES_TITLE, AI_ENABLED, AI_API_KEY, AI_BASE_URL, AI_MODEL, RANKS, BACKUP_CHAT_ID, BACKUP_INTERVAL_HOURS, AI_FALLBACK_MODELS, AI_GEMINI_API_KEY, AI_GROQ_API_KEY, AI_MISTRAL_API_KEY, AI_OPENROUTER_API_KEY, AI_CLOUDFLARE_API_TOKEN, AI_CLOUDFLARE_ACCOUNT_ID, CLEANUP_CHAT_ID, CLEANUP_INTERVAL_HOURS
)

from database import Database
from ff_api import FreeFireAPI
from parser import (
    ActivityEntry,
    format_number,
    parse_activity_text,
    total_activity, parse_monitoring_text
)
from scheduler import WeeklyScheduler


# =========================================================
# LOGGING
# =========================================================

import os
from pathlib import Path
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log", encoding="utf-8")
    ]
)

logger = logging.getLogger(__name__)


async def safe_edit(message: Message, text: str, **kwargs):
    """Edit a bot message without turning a harmless Telegram "message is not modified"
    response into a failed callback. Returns False when there was nothing to change.
    """
    try:
        await message.edit_text(text, **kwargs)
        return True
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return False
        raise


# =========================================================
# GLOBAL
# =========================================================
db = Database(DATABASE_PATH)
dp = Dispatcher(storage=MemoryStorage())

# Approved KV notices are published to both guild chats.
KV_PUBLISH_CHAT_IDS = (-1004361184660, -1004283613073)

# Runtime message registry used by the manual chat-cleaning command. Telegram's
# Bot API does not provide arbitrary history pagination, so we only delete
# messages that have been observed since this bot instance started.
_CLEANUP_TRACK_LIMIT = 1000
_cleanup_messages = defaultdict(lambda: deque(maxlen=_CLEANUP_TRACK_LIMIT))
_cleanup_bot_messages = defaultdict(lambda: deque(maxlen=_CLEANUP_TRACK_LIMIT))
_cleanup_candidate_messages = defaultdict(lambda: deque(maxlen=_CLEANUP_TRACK_LIMIT))
_cleanup_message_objects = defaultdict(lambda: deque(maxlen=_CLEANUP_TRACK_LIMIT))


def _is_user_command_message(message: Message) -> bool:
    text=(message.text or "").strip()
    if not text:
        return False
    if text.startswith("/"):
        return True
    low=text.lower()
    # Do not delete ordinary sentences. Match only exact known aliases or
    # command phrases that are intentionally supported by the bot.
    aliases=globals().get("V71_NO_SLASH", {})
    if low in aliases:
        return True
    for phrase in sorted(aliases, key=len, reverse=True):
        if low.startswith(str(phrase).lower()+" "):
            return True
    return bool(re.match(r"^(?:созыв|калл|@all|призывать всех|призвать\s+@)[\s:]", low))

async def _delete_tracked_message(chat_id:int, message_id:int):
    try:
        if chat_id == int(CLEANUP_CHAT_ID) if "CLEANUP_CHAT_ID" in globals() else False:
            pinned=await _get_pinned_ids(chat_id)
            if message_id in pinned:
                return
        await bot.delete_message(chat_id,message_id)
    except Exception:
        pass

class CleanupMessageTracker(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if not isinstance(event, Message):
            return await handler(event, data)

        chat_id = event.chat.id
        try:
            _cleanup_messages[chat_id].append(event.message_id)
            _cleanup_message_objects[chat_id].append(event)
            classifier = globals().get("_cleanup_is_command_or_trash")
            if classifier and classifier(event):
                _cleanup_candidate_messages[chat_id].append(event.message_id)
        except Exception:
            pass

        # User commands are removed immediately only when a handler actually sent
        # a bot reply. This avoids deleting ordinary text that happens to look like
        # a command and keeps FSM input untouched.
        before = len(_cleanup_bot_messages.get(chat_id, ()))
        try:
            result = await handler(event, data)
        finally:
            try:
                after = len(_cleanup_bot_messages.get(chat_id, ()))
                if after > before and _is_user_command_message(event):
                    asyncio.create_task(_delete_tracked_message(chat_id, event.message_id))
            except Exception:
                pass
        return result


dp.message.middleware(CleanupMessageTracker())



# V8.1.10 EARLY CHAT-SCOPED MARRIAGE DISPATCHER
# Registered immediately after Dispatcher creation so generic text aliases cannot steal marriage commands.
def _v810_name(u):
    uid = getattr(u, "id", None)
    if uid is not None:
        try:
            p = db.get_player_by_telegram(int(uid))
            if p and p["nick"]:
                return p["nick"]
        except Exception:
            pass
    return getattr(u, "full_name", None) or getattr(u, 'username', None) or "Участник"

async def _v810_marriage_target(message):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    m = re.search(r'@([A-Za-z0-9_]{3,})', (message.text or ''))
    if m:
        try:
            return await bot.get_chat('@' + m.group(1))
        except Exception:
            return None
    return None

@dp.message(F.text.regexp(re.compile(r'^(брак\s+(?:да|нет)|!брак(?:\s+@?[A-Za-z0-9_]{3,})?|брак)$', re.I)))
async def _v810_marriage_command(message: Message):
    text=(message.text or '').strip()
    low=text.lower()
    chat_id=message.chat.id
    uid=message.from_user.id
    if low in ('брак да','брак нет'):
        pending=db.pending_chat_marriage_for_target(chat_id, uid)
        if not pending:
            await message.answer('💍 У тебя нет ожидающего предложения брака.')
            return
        if low=='брак да':
            if db.active_chat_marriage(chat_id,uid) or db.active_chat_marriage(chat_id,pending['user1_id']):
                await message.answer('💔 Нельзя заключить второй брак. Пользователь изменяет.')
                return
            db.accept_chat_marriage(chat_id,pending['id'])
            await message.answer(f"💍 <b>БРАК ЗАКЛЮЧЁН!</b> ❤️\n{_v810_name(message.from_user)} ❤️ {_v810_name(message.from_user)}", parse_mode='HTML')
        else:
            db.decline_chat_marriage(chat_id,pending['id'])
            await message.answer('❌ Предложение брака отклонено.')
        return
    target=await _v810_marriage_target(message)
    if not target:
        await message.answer('💍 Чтобы предложить брак, ответь словом «Брак» на сообщение пользователя или напиши !брак @username.')
        return
    if target.id==uid:
        await message.answer('❌ На себе жениться нельзя.')
        return
    if db.active_chat_marriage(chat_id,uid) or db.active_chat_marriage(chat_id,target.id):
        await message.answer('💔 Нельзя заключить второй брак. Пользователь изменяет.')
        return
    if db.pending_chat_marriage_for_target(chat_id,target.id):
        await message.answer('💍 У этого пользователя уже есть ожидающее предложение брака.')
        return
    mid=db.create_chat_marriage(chat_id,uid,target.id,uid)
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='💍 Принять',callback_data=f'v810_marry_yes_{mid}'),InlineKeyboardButton(text='❌ Отклонить',callback_data=f'v810_marry_no_{mid}')]])
    await message.answer(f'💍 <b>ПРЕДЛОЖЕНИЕ БРАКА</b>\n\n{_v810_name(message.from_user)} ❤️ {_v810_name(target)}\n\nЧтобы принять, напиши <code>брак да</code>.',parse_mode='HTML',reply_markup=kb)

@dp.callback_query(F.data.regexp(re.compile(r'^v810_marry_(?:yes|no)_\d+$')))
async def _v810_marriage_callback(callback: CallbackQuery):
    if not callback.message or not callback.from_user: return
    mid=int(callback.data.rsplit('_',1)[1]); chat_id=callback.message.chat.id
    row=db.marriage_by_id(mid)
    if not row or row['chat_id']!=chat_id or row['status']!='pending':
        await callback.answer('Предложение недействительно.',show_alert=True); return
    if callback.data.startswith('v810_marry_yes_'):
        if callback.from_user.id!=row['user2_id']:
            await callback.answer('Принять брак может только получатель предложения.',show_alert=True); return
        if db.active_chat_marriage(chat_id,row['user1_id']) or db.active_chat_marriage(chat_id,row['user2_id']):
            await callback.answer('Нельзя заключить второй брак.',show_alert=True); return
        db.accept_chat_marriage(chat_id,mid)
        await callback.message.edit_text('💍 <b>БРАК ЗАКЛЮЧЁН!</b> ❤️',parse_mode='HTML'); await callback.answer('Брак заключён')
    else:
        if callback.from_user.id!=row['user2_id']:
            await callback.answer('Отклонить предложение может только получатель.',show_alert=True); return
        db.decline_chat_marriage(chat_id,mid); await callback.message.edit_text('❌ Предложение брака отклонено.'); await callback.answer('Отклонено')

@dp.message(F.text.regexp(re.compile(r'^браки$', re.I)))
async def _v810_marriages(message: Message):
    rows=db.list_chat_active_marriages(message.chat.id,limit=100,offset=0)
    if not rows:
        await message.answer('💍 В этом чате пока нет активных браков.'); return
    out=['💍 <b>БРАКИ В ЧАТЕ</b>','']
    for n,row in enumerate(rows,1): out.append(f'{n}. {_v810_name(type("U",(),{"id":row["user1_id"]})())} ❤️ {_v810_name(type("U",(),{"id":row["user2_id"]})())}')
    await message.answer('\n'.join(out),parse_mode='HTML')

@dp.message(F.text.regexp(re.compile(r'^мой\s+брак$', re.I)))
async def _v810_my_marriage(message: Message):
    row=db.active_chat_marriage(message.chat.id,message.from_user.id)
    if not row:
        await message.answer('💍 У тебя нет активного брака.'); return
    partner=row['user2_id'] if row['user1_id']==message.from_user.id else row['user1_id']
    await message.answer(f'💍 <b>МОЙ БРАК</b>\n\n❤️ Партнёр: {mention_user(partner, registered_display_name(partner))}',parse_mode='HTML')

@dp.message(F.text.regexp(re.compile(r'^!?развод$', re.I)))
async def _v810_divorce(message: Message):
    row=db.active_chat_marriage(message.chat.id,message.from_user.id)
    if not row:
        await message.answer('💔 Активного брака нет.'); return
    db.divorce_chat_marriage(message.chat.id,message.from_user.id)
    await message.answer('💔 Брак расторгнут.')
bot = None

ff_client = FreeFireAPI(FF_API_KEY, FF_REGION, FF_API_PROVIDER, FF_API_BASE)

pending_uploads = {}
selected_weeks = {}
USERS_PAGE_SIZE = 8


class LifetimeActivityStates(StatesGroup):
    waiting_value = State()

class AddPlayerStates(StatesGroup):
    waiting_uid = State()
    waiting_telegram = State()
    waiting_nick = State()

class RefreshStates(StatesGroup):
    waiting_player = State()

class WeekSelectStates(StatesGroup):
    waiting_date = State()

class WeeklyActivityStates(StatesGroup):
    waiting_value = State()

class CoinEditStates(StatesGroup):
    waiting_value = State()

class RegistrationStates(StatesGroup):
    waiting_uid = State()

class GuildApplicationStates(StatesGroup):
    waiting_uid = State()
    waiting_why = State()
    waiting_found = State()
    waiting_extra = State()

# Temporary per-admin confirmation data.
pending_lifetime_activity = {}
pending_publish = {}
pending_weekly_activity = {}


# =========================================================
# HELPERS
# =========================================================

# Role hierarchy is stored in SQLite (admin_roles), never hard-coded by Telegram ID.
# Existing legacy role 2 is intentionally preserved.
ADMIN_ROLE_NAMES = {
    8: "👑 Лидер",
    7: "⭐ Заместитель",
    6: "🛡 Админ чата",
    5: "🔨 Глав Проверяюший",
    4: "🔍 Проверяющий",
    3: "Помощник",
    2: "⚡ ССМШИК",
    1: "👤 Участник",
}

# Level 0 is reserved for users who are not registered in the guild.
# Registered participants without an administrative role remain level 1.

# Strict Telegram-ID allowlist for administrative access.
# Legacy rows in admin_roles are preserved for data integrity, but do not grant access.
ADMIN_ACCESS = {
    8930370348: 8,
    8712560202: 7,
    6013589459: 6,
}

def get_admin_rank(user_id: int) -> int:
    uid = int(user_id)
    if uid in ADMIN_ACCESS:
        return int(ADMIN_ACCESS[uid])
    try:
        if db.get_player_by_telegram(uid) is None:
            return 0
    except Exception:
        # Keep startup resilient; an existing Telegram user remains participant.
        pass
    return int(db.get_admin_role(uid) or 1)

def migrate_role_hierarchy_v2():
    """Shift legacy admin levels 2..7 to the new 2..8 hierarchy once."""
    try:
        marker = db.conn.execute(
            "SELECT value FROM migration_meta WHERE key=?",
            ("role_hierarchy_v2",),
        ).fetchone()
        if marker:
            return
        rows = db.conn.execute(
            "SELECT telegram_id, role_level FROM admin_roles WHERE role_level >= 2 AND role_level <= 7"
        ).fetchall()
        for row in rows:
            db.conn.execute(
                "UPDATE admin_roles SET role_level=?, updated_at=? WHERE telegram_id=?",
                (int(row["role_level"]) + 1, db._now(), int(row["telegram_id"])),
            )
        db.conn.execute(
            "INSERT INTO migration_meta(key,value) VALUES(?,?)",
            ("role_hierarchy_v2", "1"),
        )
        db.conn.commit()
        db.log("role_hierarchy_v2_migrated", None, {"rows": len(rows)})
    except Exception:
        logger.exception("Не удалось выполнить миграцию иерархии ролей V2")
        raise

migrate_role_hierarchy_v2()

def bootstrap_roles_from_env():
    """Synchronize configured roles without deleting legacy database rows."""
    for uid, level in ADMIN_ACCESS.items():
        db.set_admin_role(uid, level)
        db.log("admin_allowlist_bootstrap", uid, json.dumps({"role": level}, ensure_ascii=False))


bootstrap_roles_from_env()


# =========================================================
# OWNER BACKUPS
# =========================================================
BACKUP_DIR = Path("backups")
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_PREFIX = "guild_activity_"


def owner_only(user_id: int) -> bool:
    return bool(OWNER_ID) and int(user_id) == int(OWNER_ID)


def backup_files():
    return sorted(
        BACKUP_DIR.glob(f"{BACKUP_PREFIX}*.db"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def create_backup() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = BACKUP_DIR / f"{BACKUP_PREFIX}{stamp}.db"
    db.backup_to(str(target))
    for old_file in backup_files()[3:]:
        try:
            old_file.unlink()
        except OSError:
            logger.warning("Не удалось удалить старый бэкап %s", old_file)
    return target


def backup_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Создать и отправить", callback_data="owner_backup_create")],
        [InlineKeyboardButton(text="📦 Последний бэкап", callback_data="owner_backup_latest")],
        [InlineKeyboardButton(text="📚 Список копий", callback_data="owner_backup_list")],
        [InlineKeyboardButton(text="♻️ Восстановить", callback_data="owner_backup_restore_list")],
        [InlineKeyboardButton(text="⬅️ В админку", callback_data="menu_admin")],
    ])


def require_rank(user_id: int, minimum: int) -> bool:
    return get_admin_rank(user_id) >= minimum


def activity_admin(user_id: int) -> bool:
    """Настройка/изменение активности гильдии: только Лидер, Зам и Админ чата."""
    return require_rank(user_id, 6)


def moderation_admin(user_id: int) -> bool:
    """Модерация и управляющие команды: только три руководителя 6–8."""
    return management_admin(user_id)

def is_admin(user_id: int, minimum_rank: int = 6) -> bool:
    """Administrative access is limited to the three guild administrators (ranks 6–8).

    Legacy admin_roles rows are preserved, but cannot grant administrative-panel access
    unless the Telegram ID is in the explicit three-admin allowlist.
    """
    uid = int(user_id)
    return uid in ADMIN_ACCESS and int(ADMIN_ACCESS[uid]) >= int(minimum_rank)

def management_admin(user_id: int) -> bool:
    return is_admin(user_id, 6)

def rank_name(level: int) -> str:
    return ADMIN_ROLE_NAMES.get(int(level), "👤 Участник")

def can_manage_rank(actor_id: int, target_rank: int) -> bool:
    actor_rank = get_admin_rank(actor_id)
    # Leader can manage everyone below leader. Deputy can manage up to chat admins.
    if actor_rank >= 8:
        return target_rank < 8
    if actor_rank >= 7:
        return 1 <= target_rank <= 6
    return False

def resolve_user_id_for_admin(message: Message, token: str | None = None) -> int | None:
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id
    if not token:
        return None
    token = token.strip().lstrip("@")
    if token.isdigit():
        return int(token)
    try:
        row = db.get_player_by_telegram_username(token)
        if row:
            return int(row["telegram_id"])
    except Exception:
        pass
    return None

async def resolve_moderation_target(message: Message, token: str | None = None):
    """Resolve a moderation target from reply, Telegram ID, or a registered @username."""
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    if not token:
        return None
    raw = token.strip().lstrip("@")
    if raw.isdigit():
        uid = int(raw)
        row = db.get_player_by_telegram(uid)
        # Build a lightweight User-compatible object only where needed.
        try:
            chat_member = await bot.get_chat_member(message.chat.id, uid)
            return chat_member.user
        except Exception:
            return type("ResolvedUser", (), {"id": uid, "full_name": row["nick"] if row else str(uid), "username": row["telegram_username"] if row else None})()
    row = db.get_player_by_telegram_username(raw)
    if not row or not row["telegram_id"]:
        return None
    uid = int(row["telegram_id"])
    try:
        chat_member = await bot.get_chat_member(message.chat.id, uid)
        return chat_member.user
    except Exception:
        return type("ResolvedUser", (), {"id": uid, "full_name": row["nick"], "username": row["telegram_username"]})()


def format_duration(seconds: int) -> str:
    if seconds % 86400 == 0: return f"{seconds//86400} дн."
    if seconds % 3600 == 0: return f"{seconds//3600} ч."
    if seconds % 60 == 0: return f"{seconds//60} мин."
    return f"{seconds} сек."


def next_rank_info(activity: int):
    activity = int(activity or 0)
    for threshold, name in RANKS:
        if activity < threshold:
            return threshold, name, max(0, threshold - activity)
    return None, RANKS[-1][1], 0


def get_status(activity: int):
    if activity < LOW_ACTIVITY_LIMIT:
        return "🔴", "низкая"
    if activity < HIGH_ACTIVITY_LIMIT:
        return "🟡", "нормальная"
    return "🟢", "высокая"


def get_medal(position: int):
    if position == 1:
        return "🥇"
    if position == 2:
        return "🥈"
    if position == 3:
        return "🥉"
    return f"{position}."


def format_date(value: str):
    try:
        year, month, day = map(int, value.split("-"))
        return f"{day:02d}.{month:02d}.{year}"
    except:
        return value


def week_label(start: str, end: str):
    return f"{format_date(start)} — {format_date(end)}"


def get_default_week():
    """Текущая игровая неделя. Переключение происходит только в понедельник 04:10 МСК."""
    try:
        now = datetime.now(ZoneInfo(TIMEZONE))
    except Exception:
        now = datetime.now()
    current_date = now.date()
    monday = current_date - timedelta(days=current_date.weekday())
    cutoff = now.replace(hour=PUBLISH_HOUR, minute=PUBLISH_MINUTE, second=0, microsecond=0)
    # В понедельник 00:00–03:59 всё ещё продолжается прошлая неделя.
    if now.weekday() == 0 and now < cutoff:
        monday -= timedelta(days=7)
    return monday, monday + timedelta(days=6)


def get_selected_week(user_id: int):
    if user_id in selected_weeks:
        monday = selected_weeks[user_id]
        return monday, monday + timedelta(days=6)
    return get_default_week()

def get_current_week_record():
    start, _ = get_default_week()
    return db.get_week(start.isoformat())


def extract_uid(text: str) -> str:
    match = re.search(r"\b(\d{8,12})\b", text)
    return match.group(1) if match else None


# =========================================================
# KEYBOARDS - ПОЛНАЯ ПАНЕЛЬ
# =========================================================

def panel_keyboard(user_id: int):
    """Main participant panel. RP actions are intentionally NOT a panel section: RP is
    a chat action engine handled independently by reply/alias dispatchers."""
    buttons = [
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton(text="🏆 Рейтинг", callback_data="menu_top")],
        [InlineKeyboardButton(text="🔥 Активность", callback_data="menu_stats"), InlineKeyboardButton(text="🪙 Коины", callback_data="menu_coins")],
        [InlineKeyboardButton(text="👥 Участники", callback_data="users_page_0"), InlineKeyboardButton(text="📈 Прогресс", callback_data="menu_progress")],
        [InlineKeyboardButton(text="🏆 Турниры", callback_data="menu_tournaments"), InlineKeyboardButton(text="👑 Достижения", callback_data="menu_achievements")],
        [InlineKeyboardButton(text="🔥 Серия", callback_data="menu_streak"), InlineKeyboardButton(text="🪙 Коин-топ", callback_data="menu_coin_top")],
        [InlineKeyboardButton(text="🎮 FF профиль", callback_data="menu_ff"), InlineKeyboardButton(text="⚔️ КВ", callback_data="menu_kv")],
        [InlineKeyboardButton(text="🛒 Магазин", callback_data="menu_shop"), InlineKeyboardButton(text="🎁 Рефералы", callback_data="menu_ref")],
        [InlineKeyboardButton(text="📜 Правила гильдии", callback_data="menu_guild_rules"), InlineKeyboardButton(text="⚔️ Правила КВ", callback_data="menu_kv_rules")],
        [InlineKeyboardButton(text="📩 Заявка", callback_data="menu_apply"), InlineKeyboardButton(text="❓ Помощь", callback_data="menu_help")],
    ]
    if management_admin(user_id):
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="menu_admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_keyboard(user_id: int | None = None):
    """Главное меню администрации.

    Здесь только разделы. Старые callback'и не удаляются: каждый раздел открывает
    существующие рабочие экраны проекта.
    """
    uid = int(user_id or 0)
    if not management_admin(uid):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_main")]
        ])
    rows = [
        [InlineKeyboardButton(text="👥 Управление игроками", callback_data="admin_section_players"),
         InlineKeyboardButton(text="🧠 Анти накрутка", callback_data="admin_section_anticheat")],
        [InlineKeyboardButton(text="🔥 Активность", callback_data="admin_section_activity"),
         InlineKeyboardButton(text="🪙 Коины", callback_data="admin_section_coins")],
        [InlineKeyboardButton(text="🏆 Турниры", callback_data="admin_section_tournaments"),
         InlineKeyboardButton(text="📩 Заявки", callback_data="admin_section_applications")],
        [InlineKeyboardButton(text="📣 Созыв", callback_data="admin_section_summon"),
         InlineKeyboardButton(text="⚔️ КВ", callback_data="admin_section_kv")],
        [InlineKeyboardButton(text="👑 Ранги", callback_data="admin_section_ranks"),
         InlineKeyboardButton(text="🛡 Администрация", callback_data="admin_section_administration")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_back():
    return InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")


def _admin_section_markup(rows):
    rows = list(rows)
    rows.append([_admin_back()])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "admin_section_players")
async def callback_admin_section_players(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await safe_edit(callback.message, "👥 <b>УПРАВЛЕНИЕ ИГРОКАМИ</b>\n\nВыбери действие:",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="➕ Добавить игрока", callback_data="menu_adduser")],
            [InlineKeyboardButton(text="🗑 Удалить игрока", callback_data="menu_removeuser")],
            [InlineKeyboardButton(text="🔓 Отвязать Telegram", callback_data="menu_unbind")],
            [InlineKeyboardButton(text="🔄 Обновить профили", callback_data="menu_refresh")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_activity")
async def callback_admin_section_activity(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await safe_edit(callback.message, "🔥 <b>АКТИВНОСТЬ</b>\n\nВыбери действие:",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="🔥 Установить активность", callback_data="menu_activity_help")],
            [InlineKeyboardButton(text="📥 Импорт активности", callback_data="menu_import")],
            [InlineKeyboardButton(text="📈 За всё время", callback_data="menu_lifetime_activity")],
            [InlineKeyboardButton(text="📢 Публикация", callback_data="menu_publish")],
            [InlineKeyboardButton(text="🎁 Награды", callback_data="menu_rewards")],
            [InlineKeyboardButton(text="📊 Полная статистика", callback_data="menu_full_stats")],
            [InlineKeyboardButton(text="📆 Выбрать неделю", callback_data="menu_week")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_kv")
async def callback_admin_section_kv(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await safe_edit(callback.message, "⚔️ <b>УПРАВЛЕНИЕ КВ</b>\n\nВыбери раздел:",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="📩 Заявки КВ", callback_data="menu_kv_proposals")],
            [InlineKeyboardButton(text="👥 Составы КВ", callback_data="menu_kv_rosters")],
            [InlineKeyboardButton(text="📚 История КВ", callback_data="menu_kv_history")],
            [InlineKeyboardButton(text="📅 Назначенные КВ", callback_data="menu_kv_scheduled")],
            [InlineKeyboardButton(text="➕ Создать КВ", callback_data="admin_kv_create")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_administration")
async def callback_admin_section_administration(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await safe_edit(callback.message, "🛡 <b>АДМИНИСТРАЦИЯ</b>\n\nВыбери раздел:",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="👑 Ранги", callback_data="menu_ranks")],
            [InlineKeyboardButton(text="🛡 Модерация", callback_data="menu_moderation")],
            [InlineKeyboardButton(text="👥 Админы", callback_data="menu_admins")],
            [InlineKeyboardButton(text="📋 Команды админов", callback_data="admin_commands")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_coins")
async def callback_admin_section_coins(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🪙 Редактор коинов доступен только лидеру.", show_alert=True); return
    await safe_edit(callback.message, "🪙 <b>КОИНЫ</b>\n\nРедактор баланса доступен только лидеру.",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="🪙 Редактор коинов", callback_data="owner_coins")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_tournaments")
async def callback_admin_section_tournaments(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await callback.answer()
    await callback.message.answer("🏆 <b>ТУРНИРЫ</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Управление турнирами", callback_data="admin_tournaments")],
        [_admin_back()]
    ]))


@dp.callback_query(F.data == "admin_section_applications")
async def callback_admin_section_applications(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await safe_edit(callback.message, "📩 <b>ЗАЯВКИ В ГИЛЬДИЮ</b>\n\nОжидающие заявки:",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="📩 Открыть заявки", callback_data="menu_applications")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_summon")
async def callback_admin_section_summon(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await safe_edit(callback.message, "📣 <b>СОЗЫВ</b>\n\nСозыв запускается командами <code>созыв текст</code>, <code>калл текст</code>, <code>@all текст</code> или через <code>призвать @user</code>.",
        reply_markup=_admin_section_markup([
            [InlineKeyboardButton(text="📣 Открыть созыв", callback_data="menu_summon")],
        ]))
    await callback.answer()


@dp.callback_query(F.data == "admin_section_anticheat")
async def callback_admin_section_anticheat(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await callback.answer()
    await callback.message.answer("🧠 <b>АНТИ НАКРУТКА</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧠 Открыть", callback_data="admin_anticheat")],
        [_admin_back()]
    ]))


@dp.callback_query(F.data == "admin_section_ranks")
async def callback_admin_section_ranks(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await callback.answer()
    await callback.message.answer("👑 <b>РАНГИ</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Управление рангами", callback_data="menu_ranks")],
        [_admin_back()]
    ]))


@dp.callback_query(F.data == "admin_commands")
async def callback_admin_commands(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await command_admin_commands(callback.message)
    await callback.answer()


def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_main")]
    ])


def confirm_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data="activity_confirm"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="activity_cancel")
        ]
    ])


# =========================================================
# GAMIFICATION / TOURNAMENTS / ACHIEVEMENTS
# =========================================================

def progress_bar(value: int, maximum: int, width: int = 10) -> str:
    if maximum <= 0:
        return "░" * width
    filled = max(0, min(width, round((value / maximum) * width)))
    return "█" * filled + "░" * (width - filled)


def player_progress_text(player_id: str):
    p = db.get_player(player_id)
    if not p:
        return "❌ Игрок не найден."
    rows = db.get_player_progress(player_id, 8)
    if not rows:
        return "📈 <b>ПРОГРЕСС</b>\n\nПока нет завершённых недель."
    max_v = max(1, max(int(r['activity'] or 0) for r in rows))
    lines=["📈 <b>ПРОГРЕСС АКТИВНОСТИ</b>", "", f"👤 <b>{html.escape(p['nick'])}</b>", ""]
    for r in rows:
        lines.append(f"{format_date(r['week_start'])}: {progress_bar(int(r['activity'] or 0), max_v)} <b>{format_number(int(r['activity'] or 0))}</b>")
    return "\n".join(lines)


def achievements_text(player_id: str):
    p=db.get_player(player_id)
    if not p: return "❌ Игрок не найден."
    db.award_achievements(player_id)
    rows=db.get_player_achievements(player_id)
    streak=db.get_player_streak(player_id)
    total=int(p['total_activity'] or 0)
    lines=["👑 <b>ПРОФИЛЬ ДОСТИЖЕНИЙ</b>", "", f"👤 <b>{html.escape(p['nick'])}</b>", f"🔥 Lifetime: <b>{format_number(total)}</b>", f"🔥 Серия: <b>{streak} нед.</b>", ""]
    if rows:
        for a in rows:
            lines.append(f"{a['name']} — <i>{html.escape(a['description'])}</i>")
    else:
        lines.append("Пока нет достижений. Начни апать активность!")
    return "\n".join(lines)


def monthly_coin_top_text():
    now=datetime.now(ZoneInfo(TIMEZONE))
    ym=now.strftime('%Y-%m')
    rows=db.get_monthly_coin_ranking(ym,20)
    lines=["🪙 <b>МЕСЯЧНЫЙ РЕЙТИНГ КОИНОВ</b>", "", f"📅 {now.strftime('%m.%Y')}", ""]
    if not rows: lines.append("Пока нет заработанных коинов за этот месяц.")
    for i,r in enumerate(rows,1):
        label=f"@{html.escape(r['telegram_username'])}" if r['telegram_username'] else "Telegram"
        p=db.get_player_by_telegram(int(r['telegram_id']))
        name=p['nick'] if p else r['nick']
        lines.append(f"{get_medal(i)} <b>{html.escape(name)}</b> — {mention_user(int(r['telegram_id']), label)} — 🪙 <b>{format_number(int(r['earned'] or 0))}</b>")
    return "\n".join(lines)


def streak_text(user_id: int):
    p=db.get_player_by_telegram(user_id)
    if not p: return "❌ Ты не зарегистрирован."
    streak=db.get_player_streak(p['player_id'])
    next_goal=3 if streak<3 else 5 if streak<5 else 10 if streak<10 else None
    lines=["🔥 <b>СЕРИЯ АКТИВНОСТИ</b>", "", f"👤 <b>{html.escape(p['nick'])}</b>", f"🔥 Текущая серия: <b>{streak} нед.</b>"]
    if next_goal: lines.append(f"🎯 До следующего достижения: <b>{next_goal-streak}</b> нед.")
    else: lines.append("👑 Все основные серии закрыты!")
    return "\n".join(lines)


def tournaments_text(limit=10):
    rows=db.get_tournaments(limit=limit)
    lines=["🏆 <b>ТУРНИРЫ ГИЛЬДИИ</b>", ""]
    if not rows: lines.append("Пока турниров нет.")
    for t in rows:
        status="🟢 Идёт" if t['status']=='active' else "🏁 Завершён"
        lines.append(f"<b>#{t['tournament_id']} {html.escape(t['name'])}</b> — {status}")
        if t['start_date'] or t['end_date']: lines.append(f"   📅 {t['start_date'] or '?'} — {t['end_date'] or '?'}")
        players=db.get_tournament_players(t['tournament_id'],5)
        for i,r in enumerate(players,1): lines.append(f"   {get_medal(i)} {html.escape(r['nick'])} — ⚡️ {format_number(int(r['points']))}")
        if not players: lines.append("   Пока нет результатов")
    return "\n".join(lines)


def enhanced_player_card(player_id: str):
    p=db.get_player(player_id)
    if not p: return "❌ Игрок не найден."
    latest=get_current_week_record(); weekly=0; position=None
    if latest:
        row=db.get_week_player(latest['week_start'],player_id)
        if row: weekly=int(row['activity']); position=db.get_week_player_position(latest['week_start'],player_id)
    lifetime=int(p['total_activity'] or 0); rank=get_rank(lifetime); streak=db.get_player_streak(player_id); coins=db.get_coin_balance(int(p['telegram_id'])) if p['telegram_id'] else 0
    next_threshold,next_name,next_left=next_rank_info(lifetime)
    tg=mention_user(int(p['telegram_id']), '@'+p['telegram_username'] if p['telegram_username'] else 'Telegram') if p['telegram_id'] else 'не привязан'
    lines=["👤 <b>КАРТОЧКА ИГРОКА</b>","",f"🎮 <b>{html.escape(p['nick'])}</b>",f"📱 {tg}",f"🆔 UID: <code>{p['player_id']}</code>",f"🏆 Титул: <b>{html.escape(rank)}</b>","",f"🔥 Неделя: <b>{format_number(weekly)}</b>",f"📊 Статус: <b>{get_status(weekly)[1]}</b>",f"📚 Lifetime: <b>{format_number(lifetime)}</b>",f"🪙 Баланс: <b>{coins}</b>",f"🔥 Серия: <b>{streak} нед.</b>"]
    if position: lines.append(f"🏅 Место недели: <b>#{position}</b>")
    if next_threshold: lines.append(f"🚀 До {html.escape(next_name)}: <b>{format_number(next_left)}</b>")
    lines.append(f"👑 Достижений: <b>{len(db.get_player_achievements(player_id))}</b>")
    return "\n".join(lines)

# =========================================================
# BUILD PANEL
# =========================================================

def build_admin_panel(user_id: int | None = None):
    user_id = int(user_id or 0)
    player = db.get_player_by_telegram(user_id) if user_id else None
    latest = db.get_latest_week()
    lines = ["🤖 <b>VAKA • ПАНЕЛЬ ГИЛЬДИИ</b>", "", "⚡️ <i>Апни активность — поднимай титул — забирай коины.</i>"]
    if player:
        weekly = db.get_week_player(latest["week_start"], player["player_id"]) if latest else None
        weekly_value = int(weekly["activity"] or 0) if weekly else 0
        lifetime = int(player["total_activity"] or 0)
        next_threshold, next_name, next_left = next_rank_info(lifetime)
        next_line = f"🚀 До {html.escape(next_name)}: <b>{format_number(next_left)}</b>" if next_threshold else "🌌 Максимальный титул достигнут"
        streak = db.get_player_streak(player["player_id"])
        achievements = len(db.get_player_achievements(player["player_id"]))
        lines += ["", f"👤 <b>{html.escape(player['nick'])}</b>", f"🔥 Эта неделя: <b>{format_number(weekly_value)}</b>", f"🏆 Титул: <b>{html.escape(get_rank(lifetime))}</b>", f"📚 За всё время: <b>{format_number(lifetime)}</b>", f"🔥 Серия: <b>{streak} нед.</b>  •  👑 {achievements} достиж.", next_line, f"🪙 Баланс: <b>{db.get_coin_balance(user_id)}</b>"]
    else:
        lines += ["", "👤 Ты ещё не зарегистрирован.", "🎮 Зарегистрируй Free Fire UID, чтобы начать копить активность и коины."]
    if latest:
        lines += ["", f"📅 Неделя: <b>{week_label(latest['week_start'], latest['week_end'])}</b>", "🚀 Следи за топом и поднимайся выше!"]
    return "\n".join(lines)


def build_main_panel(user_id: int | None = None):
    """Canonical name for the participant main panel; legacy build_admin_panel remains intact."""
    return build_admin_panel(user_id)


def build_stats_text(mode="current"):
    count = db.get_players_count()
    latest = db.get_latest_week()
    if mode == "current" and latest:
        rows = db.get_week_players(latest["week_start"])
        total = sum(int(r["activity"]) for r in rows)
        low = sum(1 for r in rows if int(r["activity"]) < LOW_ACTIVITY_LIMIT)
        active = sum(1 for r in rows if int(r["activity"]) >= HIGH_ACTIVITY_LIMIT)
        avg = round(total / len(rows), 1) if rows else 0
        rank_counts = {}
        for r in rows:
            rank = get_rank(int(db.get_player(r["player_id"])["total_activity"] or 0))
            rank_counts[rank] = rank_counts.get(rank, 0) + 1
        lines = [
            "📊 <b>СТАТИСТИКА — ТЕКУЩАЯ НЕДЕЛЯ</b>", "",
            f"📅 {week_label(latest['week_start'], latest['week_end'])}",
            f"👥 Участников: <b>{len(rows)}</b>",
            f"🔥 Общая активность: <b>{format_number(total)}</b>",
            f"📈 Средняя активность: <b>{format_number(int(avg))}</b>",
            f"🟢 Активных: <b>{active}</b>",
            f"🔴 Низкая: <b>{low}</b>", "",
            "🏅 <b>ЗВАНИЯ</b>"
        ]
        lines.extend(f"• {html.escape(k)} — {v}" for k,v in sorted(rank_counts.items(), key=lambda x: -x[1]))
        return "\n".join(lines)
    if mode == "history":
        weeks = db.get_history(30)
        total = sum(int(w["total_activity"] or 0) for w in weeks)
        return "\n".join(["📚 <b>СТАТИСТИКА — ИСТОРИЯ</b>", "", f"📅 Недель: <b>{len(weeks)}</b>", f"🔥 Сумма недель: <b>{format_number(total)}</b>", f"👥 Игроков в базе: <b>{count}</b>"])
    lifetime = db.get_all_time_total()
    avg = int(lifetime / count) if count else 0
    return "\n".join(["📊 <b>СТАТИСТИКА — ЗА ВСЁ ВРЕМЯ</b>", "", f"👥 Участников: <b>{count}</b>", f"🔥 Всего нафармлено: <b>{format_number(lifetime)}</b>", f"📈 Среднее на игрока: <b>{format_number(avg)}</b>", f"📅 Недель в истории: <b>{len(db.get_history(1000))}</b>"])


# =========================================================
# TEXT ALIASES
# =========================================================

TEXT_ALIASES = {
    "кто я": "whoami",
    "кто это": "who",
    "кто админ": "admins",
    "админы": "admins",
    "кто здесь власть": "admins",
    "ид": "userid",
    "айди": "userid",
    "пинг": "ping",
    "бот": "ping",
    "вака": "vaka",
    "vaka": "vaka",
    "чат инфо": "chatinfo",
    "инфо чата": "chatinfo",
    "рандом": "random",
    "выбери": "choose",
    "данет": "yesno",
        "панель": "panel",
    "профиль": "profile",
    "мой профиль": "profile",
    "мой акк": "profile",
    "топ": "top",
    "рейтинг": "top",
    "активность": "activity",
    "статистика": "stats",
    "стата": "stats",
    "помощь": "help",
    "что умеешь": "help",
    "команды": "help",
    "регистрация": "register",
    "зарегистрироваться": "register",
    "привязать аккаунт": "register",
    "история": "history",
    "участники": "users",
    "юзеры": "users",
    "правила": "rules",
    "стата": "stats",
    "моя стата": "profile",
    "моя стата?": "profile",
    "ники": "users",
    "коины": "coins",
    "реферал": "ref",
    "магазин": "shop",
    "админка": "panel", "панель администратора": "panel",
    "правила гильдии": "rules", "преды": "warnings", "предупреждения": "warnings", "снять варн": "unwarn", "снять мут": "unmute", "разбан": "unban",
    "созвать": "summon", "созыв": "summon", "рефералы": "ref", "монеты": "coins",
"участники": "users", "ники": "users",
    "неделя": "week", "публикация": "publish", "обновить": "refresh", "логи": "logs", "наказания": "warnings"
}


async def handle_alias(message: Message, command: str):
    if command == "whoami":
        await command_whoami(message)
    elif command == "who":
        await command_who(message)
    elif command == "admins":
        await command_admins(message)
    elif command == "userid":
        await command_user_id(message)
    elif command == "ping":
        await command_ping(message)
    elif command == "chatinfo":
        await command_chatinfo(message)
    elif command == "random":
        await command_random(message)
    elif command == "choose":
        await command_choose(message)
    elif command == "yesno":
        await command_yesno(message)
    elif command == "vaka":
        await vaka_trigger(message)
    if command == "panel":
        await command_panel(message)
    elif command == "profile":
        await command_profile(message)
    elif command == "top":
        await command_top(message)
    elif command == "activity":
        await command_activity(message)
    elif command == "stats":
        await command_stats(message)
    elif command == "help":
        await command_help(message)
    elif command == "register":
        await command_register(message)
    elif command == "history":
        await command_history(message)
    elif command == "users":
        await command_users(message)
    elif command == "rules":
        await command_rules(message)
    elif command == "coins":
        await command_coins(message)
    elif command == "ref":
        await command_ref(message)
    elif command == "shop":
        await command_shop(message)
    elif command == "warnings":
        await command_warnings(message)
    elif command == "unwarn":
        await command_unwarn(message)
    elif command == "unmute":
        await command_unmute(message)
    elif command == "unban":
        await command_unban(message)
    elif command == "summon":
        await command_summon(message)
    elif command == "week":
        await command_week(message)
    elif command == "publish":
        await command_publish(message)
    elif command == "refresh":
        await command_refresh(message)
    elif command == "logs":
        await command_logs(message)
    elif command == "admins":
        await command_admins(message)
    elif command == "adminpanel":
        await command_rank_adminpanel(message)
    elif command == "setrole":
        await command_setrole(message)


# =========================================================
# COMMANDS (с сокращениями)
# =========================================================

# ----- START -----

@dp.message(CommandStart())
async def command_start(message: Message):
    user_id = message.from_user.id
    # Referral deep-link: /start ref_<telegram_id>
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2 and parts[1].startswith("ref_"):
        raw_ref = parts[1][4:]
        if raw_ref.isdigit() and int(raw_ref) != user_id and db.get_player_by_telegram(int(raw_ref)):
            db.set_referral_pending(user_id, int(raw_ref), None)
    existing = db.get_player_by_telegram(user_id)

    if existing:
        db.update_telegram_username(user_id, message.from_user.username)
        await message.answer(
            "🤖 <b>БОТ АКТИВНОСТИ ГИЛЬДИИ</b>\n\n"
            f"👤 Ты зарегистрирован как:\n"
            f"<b>{html.escape(existing['nick'])}</b>\n\n"
            f"🎮 Free Fire ID:\n<code>{existing['player_id']}</code>\n\n"
            "🔥 Активность выдаёт администрация.\n\n"
            "📌 <b>Команды (сокращения):</b>\n"
            "/panel (/pan) — панель\n"
            "/profile (/prof) — мой профиль\n"
            "/top (/t) — рейтинг\n"
            "/stats (/st) — статистика\n"
            "/history (/hist) — история\n"
            "/users (/us) — участники\n"
            "/ff UID — информация о Free Fire\n"
            "/register (/reg) UID — регистрация\n"
            "/help (/h) — помощь"
        )
        return

    await message.answer(
        GUILD_INFO_V71 + "\n\nВыбери действие:",
        reply_markup=guest_keyboard_v71()
    )


# ----- HELP (с сокращениями) -----

@dp.message(Command("help", "h"))
async def command_help(message: Message):
    user_id = message.from_user.id

    lines = [
        "🎮 <b>КОМАНДЫ БОТА</b>",
        "",
        "👤 <b>Личные команды:</b>",
        "/profile (/prof) — мой профиль",
        "/top (/t) — топ активности",
        "/stats (/st) — статистика гильдии",
        "/history (/hist) — история недель",
        "/users (/us) — список участников",
        "/ff UID — полный профиль Free Fire + BR/CS + Ban Check",
        "/ffstats UID [br|cs] — статистика Free Fire",
        "/guildinfo [ID] — информация о гильдии",
        "/bancheck UID — проверка бана",
        "/panel (/pan) — панель управления",
        "/help (/h) — эта справка",
        "",
        "📝 <b>Регистрация:</b>",
        "/register (/reg) UID — зарегистрироваться в гильдии",
        "",
        "💬 <b>Команды сообщества:</b>",
        "/rules — правила гильдии",
        "/ref — личная реферальная ссылка",
        "/coins — баланс коинов",
        "/shop — обмен коинов",
        "/achievements — достижения",
        "/progress — график прогресса",
        "/streak — серия активности",
        "/coinstop — месячный рейтинг коинов",
        "/tournaments — турниры гильдии",
        "/summon — созыв участников (админ)",
        "/warn /warnings /mute /unmute /ban /unban /kick /unwarn — модерация (админ)",
        "Текстом: «Моя стата», «Ники», «Правила», «Коины»" ,
        "🤖 Вопрос нейросети: <code>Вака как поднять активность?</code>"
    ]

    if is_admin(user_id):
        lines.extend([
            "",
            "👑 <b>Админские команды:</b>",
            "/activity (/act) UID +ОЧКИ — изменить активность (только ранг 5+)",
            "/set UID ОЧКИ — установить активность недели",
            "/total UID +ОЧКИ/-ОЧКИ/=ОЧКИ — изменить показатель за всё время",
            "/publish (/pub) — предпросмотр и подтверждение публикации",
            "/adduser TELEGRAM_ID UID NICK — добавить игрока",
            "/addlist — массовое добавление (много строк)",
            "/role / setrole — назначить уровень роли",
            "/removeplayer UID — удалить игрока",
            "/unbind UID — отвязать Telegram",
            "/refresh — обновить профили через API",
            "/logs — последние логи",
            "/week (/wk) YYYY-MM-DD — выбрать неделю"
        ])

    await message.answer("\n".join(lines))


# ----- PANEL -----

@dp.message(Command("panel", "pan"))
async def command_panel(message: Message):
    user_id = message.from_user.id
    if not db.get_player_by_telegram(user_id):
        await message.answer(
            GUILD_INFO_V71 + "\n\nВыбери действие:",
            reply_markup=guest_keyboard_v71()
        )
        return
    await message.answer(
        build_main_panel(user_id),
        reply_markup=panel_keyboard(user_id)
    )


# ----- PROFILE -----

@dp.message(Command("profile", "prof"))
async def command_profile(message: Message):
    user_id = message.from_user.id
    player = db.get_player_by_telegram(user_id)

    if not player:
        await message.answer(
            "❌ Ты не зарегистрирован.\n\n"
            "Используй:\n<code>/register UID</code> или <code>/reg UID</code>"
        )
        return

    await message.answer(enhanced_player_card(player["player_id"]))


async def send_player_profile(chat_id: int, player_id: str, edit_message=None):
    player = db.get_player(player_id)

    if not player:
        text = "❌ Участник не найден."
        if edit_message:
            await edit_message.edit_text(text, reply_markup=back_keyboard())
        else:
            await bot.send_message(chat_id, text)
        return

    latest = get_current_week_record()
    current_activity = None
    position = None

    if latest:
        current = db.get_week_player(latest["week_start"], player_id)
        if current:
            current_activity = current["activity"]
            position = db.get_week_player_position(latest["week_start"], player_id)

    total_activity = int(player["total_activity"] or 0)
    rank_activity = total_activity
    rank = get_rank(rank_activity)

    lines = [
        "👤 <b>ПРОФИЛЬ УЧАСТНИКА</b>",
        "",
        f"🎮 Ник: <b>{html.escape(player['nick'])}</b>",
        f"🆔 UID: <code>{player['player_id']}</code>",
        f"🎖 Звание: <b>{rank}</b>"
    ]

    if player["region"]:
        lines.append(f"🌍 Регион: {player['region']}")
    if player["level"]:
        lines.append(f"🏅 Уровень: {player['level']}")
    if player["guild_name"]:
        lines.append(f"🏰 Гильдия: <b>{html.escape(player['guild_name'])}</b>")

    if player["telegram_id"]:
        admin_level = get_admin_rank(int(player["telegram_id"]))
        if admin_level >= 2:
            lines.append(f"🛡 Роль в боте: <b>{rank_name(admin_level)}</b>")
        telegram = f"<code>{player['telegram_id']}</code>"
        if player["telegram_username"]:
            telegram += f" (@{html.escape(player['telegram_username'])})"
        lines.append(f"📱 Telegram: {telegram}")
    else:
        lines.append("📱 Telegram: не привязан")

    lines.extend([
        "",
        "📊 <b>СТАТИСТИКА</b>",
        "",
        f"🔥 За всё время: <b>{format_number(total_activity)}</b>",
        f"📅 Недель: <b>{player['weeks_count']}</b>"
    ])

    if player["weeks_count"]:
        avg = total_activity // player["weeks_count"]
        lines.append(f"📈 Среднее: <b>{format_number(avg)}</b>")

    if latest and current_activity is not None:
        status, status_name = get_status(current_activity)
        lines.extend([
            "",
            "📅 <b>ПОСЛЕДНЯЯ НЕДЕЛЯ</b>",
            "",
            week_label(latest["week_start"], latest["week_end"]),
            f"{status} Активность: <b>{format_number(current_activity)}</b>",
            f"📊 Статус: <b>{status_name}</b>"
        ])
        if position:
            lines.append(f"🏆 Место: <b>#{position}</b>")

    history = db.get_player_history(player_id, 5)
    if history:
        lines.extend(["", "📚 <b>ПОСЛЕДНИЕ НЕДЕЛИ</b>", ""])
        for row in history:
            icon, _ = get_status(row["activity"])
            lines.append(f"{icon} {format_date(row['week_start'])} — {format_number(row['activity'])}")

    text = "\n".join(lines)

    if edit_message:
        await edit_message.edit_text(text, reply_markup=back_keyboard())
    else:
        await bot.send_message(chat_id, text)


# ----- FF -----

@dp.message(Command("ff"))
async def command_ff(message: Message):
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("🎮 Использование:\n<code>/ff UID</code>")
        return

    uid = parts[1].strip()
    if not uid.isdigit():
        await message.answer("❌ UID должен состоять только из цифр.")
        return
    if len(uid) < 8 or len(uid) > 15:
        await message.answer("❌ UID должен содержать от 8 до 15 цифр.")
        return

    loading = await message.answer(f"⏳ Получаю полную информацию SiamBhau...\n\nUID: <code>{uid}</code>")

    try:
        profile = await ff_client.get_player_profile(uid, FF_REGION)
    except ValueError as e:
        await loading.edit_text(f"❌ <b>Ошибка</b>\n\n{html.escape(str(e))}")
        return
    except Exception as e:
        logger.exception("Ошибка /ff: %s", e)
        await loading.edit_text("⚠️ <b>SiamBhau API временно недоступен.</b>\n\nПопробуй ещё раз позже.")
        return

    if not profile:
        await loading.edit_text("❌ Игрок не найден через SiamBhau API.")
        return

    actual_region = (profile.region or FF_REGION).upper()
    lines = [
        "🎮 <b>ПРОФИЛЬ FREE FIRE</b>",
        "",
        f"👤 Ник: <b>{html.escape(profile.nickname)}</b>",
        f"🆔 UID: <code>{profile.uid}</code>",
        f"🏅 Уровень: <b>{profile.level}</b>",
        f"🌍 Регион: <b>{actual_region}</b>",
        f"❤️ Лайки: <b>{format_number(profile.likes)}</b>",
        f"⭐ BR Ранг: <b>{profile.rank_br}</b> ({format_number(profile.rank_br_points)} очков)",
        f"⭐ CS Ранг: <b>{profile.rank_cs}</b> ({format_number(profile.rank_cs_points)} очков)",
    ]

    if profile.guild_name:
        lines.extend([
            "",
            f"🏰 Гильдия: <b>{html.escape(profile.guild_name)}</b>",
            f"🆔 ID гильдии: <code>{profile.guild_id or '—'}</code>",
            f"📊 Участников: <b>{profile.guild_members}/{profile.guild_capacity or '—'}</b>",
            f"🏅 Уровень гильдии: <b>{profile.guild_level or '—'}</b>",
        ])

    lines.extend([
        "",
        f"📅 Создан: <b>{html.escape(profile.created_at)}</b>",
        f"🕐 Последний вход: <b>{html.escape(profile.last_login)}</b>",
    ])

    # FreeFireInfo exposes more than the short profile fields above. Show the
    # useful non-secret fields directly in /ff so the Premium response is not
    # unnecessarily discarded.
    basic = profile.raw_data.get("basicInfo") or profile.raw_data.get("basicinfo") or {}
    social = profile.raw_data.get("socialInfo") or {}
    pet = profile.raw_data.get("petInfo") or {}
    credit = profile.raw_data.get("creditScoreInfo") or {}
    if basic:
        extra = []
        for label, key in (("🎖 Макс. BR", "maxRank"), ("🎖 Макс. CS", "csMaxRank"),
                           ("🏅 Значков", "badgeCnt"), ("🎯 Pin", "pinId"),
                           ("📱 Версия", "releaseVersion"), ("🏷 Title", "title"),
                           ("🖼 Banner ID", "bannerId"), ("🧑 HeadPic ID", "headPic")):
            value = basic.get(key)
            if value not in (None, "", 0, "0"):
                extra.append(f"{label}: <code>{html.escape(str(value))}</code>")
        if extra:
            lines.extend(["", "🔎 <b>ДОПОЛНИТЕЛЬНЫЕ ДАННЫЕ</b>", *extra])
    if pet:
        pet_parts = []
        for label, key in (("ID", "id"), ("уровень", "level"), ("skin", "skinId"), ("skill", "selectedSkillId")):
            value = pet.get(key)
            if value not in (None, ""):
                pet_parts.append(f"{label}: {value}")
        if pet_parts:
            lines.append("🐾 Питомец: <b>" + html.escape(", ".join(pet_parts)) + "</b>")
    if social:
        signature = social.get("signature")
        language = social.get("language")
        gender = social.get("gender")
        if signature or language or gender:
            lines.append(f"💬 Соц. профиль: {html.escape(str(signature or '—'))}")
            if language:
                lines.append(f"🌐 Язык: <code>{html.escape(str(language))}</code>")
            if gender:
                lines.append(f"⚧ Пол: <code>{html.escape(str(gender))}</code>")
    if credit:
        score = credit.get("creditScore")
        if score is not None:
            lines.append(f"🛡 Credit Score: <b>{html.escape(str(score))}</b>")

    # Premium API: дополнительно получаем карьерные BR/CS stats и ban status.
    try:
        full = await ff_client.get_full_player_data(uid, actual_region)
        if full:
            br = full.get("br_stats") or {}
            cs = full.get("cs_stats") or {}
            ban = full.get("ban") or {}
            br_stats = br.get("stats") if isinstance(br, dict) else None
            cs_stats = cs.get("stats") if isinstance(cs, dict) else None

            if isinstance(br_stats, dict):
                lines.extend([
                    "",
                    "📊 <b>BR — КАРЬЕРА</b>",
                    f"🎯 Игр: <b>{format_number(br_stats.get('gamesPlayed', 0))}</b> | Побед: <b>{format_number(br_stats.get('wins', 0))}</b>",
                    f"☠️ Kills: <b>{format_number(br_stats.get('kills', 0))}</b> | HS: <b>{format_number(br_stats.get('headshots', 0))}</b>",
                    f"📈 K/D: <b>{br_stats.get('kd', 0)}</b> | WR: <b>{br_stats.get('winRate', 0)}%</b>",
                    f"🏆 Top 10: <b>{format_number(br_stats.get('top10', 0))}</b> | Longest: <b>{br_stats.get('longestKill', 0)}</b>",
                ])

            if isinstance(cs_stats, dict):
                lines.extend([
                    "",
                    "🎯 <b>CS — RANKED</b>",
                    f"🎯 Игр: <b>{format_number(cs_stats.get('gamesPlayed', 0))}</b> | Побед: <b>{format_number(cs_stats.get('wins', 0))}</b>",
                    f"☠️ Kills: <b>{format_number(cs_stats.get('kills', 0))}</b> | HS: <b>{format_number(cs_stats.get('headshots', 0))}</b>",
                    f"📈 K/D: <b>{cs_stats.get('kd', 0)}</b> | WR: <b>{cs_stats.get('winRate', 0)}%</b>",
                    f"🏆 MVP: <b>{format_number(cs_stats.get('mvp', 0))}</b>",
                ])

            if isinstance(ban, dict):
                ban_status = ban.get("ban_status") or ban.get("status") or "Неизвестно"
                lines.extend(["", f"🛡 <b>Ban Check:</b> {html.escape(str(ban_status))}"])
    except Exception:
        logger.exception("Extended /ff lookup failed for %s", uid)

    await loading.edit_text("\n".join(lines))

    # SiamBhau /banner/profile returns a real PNG. Send it as a Telegram photo
    # after the text profile; if the renderer is temporarily unavailable, the
    # profile itself still succeeds.
    try:
        banner = await ff_client.get_banner(uid, actual_region)
        if banner:
            await message.answer_photo(
                BufferedInputFile(banner, filename=f"ff_banner_{uid}.png"),
                caption=f"🖼 <b>ПРОФИЛЬНЫЙ БАННЕР</b>\n🎮 {html.escape(profile.nickname)}\n🆔 <code>{uid}</code>"
            )
        else:
            await message.answer("⚠️ Профиль получен, но SiamBhau не вернул баннер для этого UID.")
    except Exception:
        logger.exception("Banner lookup failed for %s", uid)
        await message.answer("⚠️ Профиль получен, но баннер сейчас недоступен.")


# ----- REGISTER (с сокращением /reg) -----

@dp.message(Command("register", "reg"))
async def command_register(message: Message):
    user_id = message.from_user.id

    existing_telegram = db.get_player_by_telegram(user_id)
    if existing_telegram:
        await message.answer(
            f"⚠️ Ты уже зарегистрирован.\n\n"
            f"👤 Ник: <b>{html.escape(existing_telegram['nick'])}</b>\n"
            f"🎮 UID: <code>{existing_telegram['player_id']}</code>"
        )
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer(
            "📝 <b>РЕГИСТРАЦИЯ</b>\n\n"
            "<code>/register UID</code> или <code>/reg UID</code>\n\n"
            "Например:\n"
            "<code>/register 1429409544</code>\n\n"
            "⚠️ Регистрация только для членов гильдии."
        )
        return

    uid = parts[1].strip()

    if not uid.isdigit():
        await message.answer("❌ UID должен состоять только из цифр.")
        return

    if len(uid) < 8 or len(uid) > 12:
        await message.answer("❌ UID должен содержать от 8 до 12 цифр.")
        return

    loading = await message.answer("⏳ Проверяю UID через Free Fire API...")

    try:
        profile = await ff_client.get_player_profile(uid, FF_REGION)
    except ValueError as e:
        await loading.edit_text(f"❌ <b>Ошибка</b>\n\n{str(e)}")
        return
    except Exception as e:
        logger.exception(f"Ошибка регистрации: {e}")
        await loading.edit_text(
            "⚠️ <b>Free Fire API временно недоступен</b>\n\n"
            "Попробуй позже."
        )
        return

    if not profile:
        await loading.edit_text(
            f"❌ <b>Игрок не найден</b>\n\n"
            f"UID <code>{uid}</code> не существует в регионе {FF_REGION}."
        )
        return

    # Проверка гильдии
    if FF_GUILD_ID:
        profile_guild_id = str(profile.guild_id) if profile.guild_id else None
        target_guild_id = str(FF_GUILD_ID)

        if profile_guild_id != target_guild_id:
            await loading.edit_text(
                "❌ <b>Ты не состоишь в нашей гильдии.</b>\n\n"
                f"🔍 Твоя гильдия: <b>{profile.guild_name or 'Неизвестно'}</b>\n"
                f"🆔 ID твоей гильдии: <code>{profile_guild_id or 'Нет'}</code>\n\n"
                f"💬 Для регистрации необходимо состоять в гильдии с ID: <code>{target_guild_id}</code>"
            )
            return

    # Проверяем, не зарегистрирован ли уже этот UID
    existing_player = db.get_player(uid)
    if existing_player and existing_player["telegram_id"]:
        await loading.edit_text(
            f"❌ <b>Этот Free Fire ID уже зарегистрирован.</b>\n\n"
            f"👤 <b>{html.escape(existing_player['nick'])}</b>\n"
            f"🎮 UID: <code>{uid}</code>\n\n"
            "Если это твой аккаунт, обратись к администратору."
        )
        return

    # Регистрируем
    try:
        db.register_player(
            player_id=uid,
            nick=profile.nickname,
            telegram_id=user_id,
            telegram_username=message.from_user.username,
            api_data=profile.raw_data if profile else None
        )
        referral = db.complete_referral(user_id)
        referral_bonus = 0
        if referral:
            referral_bonus = REFERRAL_COINS_PER_INVITE
            try:
                db.add_coins(int(referral["referrer_telegram_id"]), referral_bonus, "referral", json.dumps({"invited": user_id, "uid": uid}, ensure_ascii=False))
                db.log("referral_completed", user_id, json.dumps({"referrer": referral["referrer_telegram_id"], "bonus": referral_bonus, "uid": uid}, ensure_ascii=False))
            except Exception:
                logger.exception("Не удалось начислить реферальные коины")
    except ValueError as e:
        await loading.edit_text(f"❌ {html.escape(str(e))}")
        return

    rank = get_rank(0)

    lines = [
        "✅ <b>РЕГИСТРАЦИЯ ЗАВЕРШЕНА</b>",
        "",
        f"👤 Ник: <b>{html.escape(profile.nickname)}</b>",
        f"🎮 UID: <code>{uid}</code>",
        f"🏅 Уровень: {profile.level}",
        f"🎖 Звание: <b>{rank}</b>"
    ]

    if profile.guild_name:
        lines.append(f"🏰 Гильдия: <b>{html.escape(profile.guild_name)}</b>")

    lines.append(f"📱 Telegram ID: <code>{user_id}</code>")
    if referral_bonus:
        lines.append(f"\n🎁 Реферальный бонус отправлен пригласившему: <b>+{referral_bonus} 🪙</b>")

    await loading.edit_text("\n".join(lines))


# ----- TOP (с сокращением /t) -----

@dp.message(Command("top", "t"))
async def command_top(message: Message):
    latest = get_current_week_record()

    if not latest:
        await message.answer("📭 Статистика пустая.")
        return

    players = db.get_week_players(latest["week_start"])
    if not players:
        await message.answer("📭 Нет данных за эту неделю.")
        return

    lines = [
        "🏆 <b>ТОП АКТИВНОСТИ</b>",
        "",
        f"📅 {week_label(latest['week_start'], latest['week_end'])}",
        ""
    ]

    for index, player in enumerate(players[:20], start=1):
        rank = get_rank(int(player["total_activity"] or 0))
        tg_part = ""
        if player["telegram_id"]:
            tg_label = f"@{html.escape(player['telegram_username'])}" if player['telegram_username'] else "Telegram"
            tg_part = f" — {mention_user(player['telegram_id'], tg_label)}"
        lines.append(
            f"{get_medal(index)} <b>{html.escape(player['nick'])}</b>{tg_part}\n"
            f"   🔥 {format_number(player['activity'])} | 📚 {format_number(int(player['total_activity'] or 0))} | 🎖 {rank}"
        )

    if len(players) > 20:
        lines.append(f"\n... и ещё {len(players) - 20} участников")

    await message.answer("\n".join(lines))


# ----- STATS (с сокращением /st) -----

@dp.message(Command("stats", "st"))
async def command_stats(message: Message):
    await message.answer(build_stats_text(), reply_markup=back_keyboard())


# ----- HISTORY (с сокращением /hist) -----

@dp.message(Command("history", "hist"))
async def command_history(message: Message):
    await send_history(message)


async def send_history(target):
    weeks = db.get_history(30)

    if not weeks:
        text = "📭 История пустая."
        if isinstance(target, CallbackQuery):
            await target.message.edit_text(text, reply_markup=back_keyboard())
            await target.answer()
        else:
            await target.answer(text, reply_markup=back_keyboard())
        return

    lines = ["📚 <b>ИСТОРИЯ НЕДЕЛЬ</b>", ""]

    for index, week in enumerate(weeks, start=1):
        published = "✅" if week["published"] else "⏳"
        lines.append(
            f"{index}. {published} <b>{week_label(week['week_start'], week['week_end'])}</b>\n"
            f"   👥 {week['players_count']} | 🔥 {format_number(week['total_activity'])}"
        )

    if isinstance(target, CallbackQuery):
        await target.message.edit_text("\n".join(lines), reply_markup=back_keyboard())
        await target.answer()
    else:
        await target.answer("\n".join(lines), reply_markup=back_keyboard())


# ----- USERS (с сокращением /us) -----

@dp.message(Command("users", "us"))
async def command_users(message: Message):
    await show_users_page(message, 0)


async def show_users_page(message_or_callback, page: int):
    total = db.get_players_count()
    total_pages = max(1, (total + USERS_PAGE_SIZE - 1) // USERS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))

    players = db.get_players_page(USERS_PAGE_SIZE, page * USERS_PAGE_SIZE)

    lines = [
        "👥 <b>УЧАСТНИКИ ГИЛЬДИИ</b>",
        "",
        f"Страница <b>{page + 1}/{total_pages}</b>",
        ""
    ]

    buttons = []
    latest = get_current_week_record()

    for player in players:
        row = db.get_week_player(latest["week_start"], player["player_id"]) if latest else None
        if row:
            status, _ = get_status(row["activity"])
            activity = format_number(row["activity"])
        else:
            status = "⚪"
            activity = "нет данных"

        lifetime = format_number(int(player["total_activity"] or 0))
        if player["telegram_id"]:
            tg_label = f"@{html.escape(player['telegram_username'])}" if player['telegram_username'] else "Telegram"
            tg_link = mention_user(player["telegram_id"], tg_label)
            lines.append(f"{status} <b>{html.escape(player['nick'])}</b> — {tg_link} — {activity} | 📈 {lifetime}")
        else:
            lines.append(f"{status} <b>{html.escape(player['nick'])}</b> — {activity} | 📈 {lifetime}")

        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {player['nick'][:25]}",
                callback_data=f"player_{player['player_id']}"
            )
        ])

    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"users_page_{page - 1}"))
    if page < total_pages - 1:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"users_page_{page + 1}"))

    if navigation:
        buttons.append(navigation)

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_main")])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)

    if isinstance(message_or_callback, CallbackQuery):
        await safe_edit(message_or_callback.message, "\n".join(lines), reply_markup=markup)
        await message_or_callback.answer()
    else:
        await message_or_callback.answer("\n".join(lines), reply_markup=markup)


# =========================================================
# ACTIVITY (с сокращением /act) - ИСПРАВЛЕННАЯ ВЕРСИЯ
# =========================================================

def parse_manual_activity(text: str):
    """
    Parse manual activity commands.

    Supported formats:
      UID +100
      UID -50
      UID =3000
      /act UID +100
      /act UID -50
      /act UID =3000
      /set UID 3000

    Multiple players may be supplied on separate lines.
    Returns a list of dictionaries:
      {"uid": int, "mode": "add"|"subtract"|"set", "value": int}
    """
    if not text:
        return []

    lines = text.splitlines()
    result = []

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # Normalize command prefix.
        command = None
        m = re.match(r"^/(act|activity|set)\b\s*(.*)$", line, re.IGNORECASE)
        if m:
            command = m.group(1).lower()
            line = m.group(2).strip()

        # /set UID N means SET, not ADD.
        if command == "set":
            m = re.fullmatch(r"(\d{5,15})\s+(\d+)", line)
            if m:
                result.append({
                    "uid": int(m.group(1)),
                    "mode": "set",
                    "value": int(m.group(2)),
                })
                continue

            # Also accept /set UID =N.
            m = re.fullmatch(r"(\d{5,15})\s*=\s*(\d+)", line)
            if m:
                result.append({
                    "uid": int(m.group(1)),
                    "mode": "set",
                    "value": int(m.group(2)),
                })
                continue

            continue

        # Explicit operation: UID +N / UID -N / UID =N
        m = re.fullmatch(r"(\d{5,15})\s*([+\-=])\s*(\d+)", line)
        if m:
            uid = int(m.group(1))
            op = m.group(2)
            value = int(m.group(3))

            mode = {
                "+": "add",
                "-": "subtract",
                "=": "set",
            }[op]

            result.append({
                "uid": uid,
                "mode": mode,
                "value": value,
            })
            continue

        # If a command was supplied but the line did not match, ignore it.
        # This keeps normal chat messages from being interpreted as activity.
        continue

    return result

async def process_manual_activity(message: Message, text: str):
    if not is_admin(message.from_user.id):
        return False
    entries = parse_manual_activity(text)
    if not entries:
        return False
    start, end = get_selected_week(message.from_user.id)
    results = []
    for item in entries:
        uid = str(item["uid"]); mode = item["mode"]; value = int(item["value"])
        player = db.get_player(uid)
        if not player:
            results.append(f"❌ <code>{uid}</code> — UID не зарегистрирован")
            continue
        try:
            row = db.get_week_player(start.isoformat(), uid)
            old = int(row["activity"]) if row else 0
            if mode == "set":
                result = db.set_week_activity(start.isoformat(), end.isoformat(), uid, value)
                new_value = value
                change = new_value - old
                label = f"🎯 {format_number(value)}"
            else:
                delta = value if mode == "add" else -value
                result = db.add_activity(start.isoformat(), end.isoformat(), uid, delta)
                new_value = result["new_activity"]
                change = delta
                label = f"{'➕' if delta >= 0 else '➖'} {format_number(abs(delta))}"
            db.log("manual_activity", message.from_user.id, json.dumps({"uid":uid,"week":start.isoformat(),"old":old,"new":new_value,"delta":change}, ensure_ascii=False))
            results.append(f"✅ <b>{html.escape(player['nick'])}</b> — {format_number(old)} → <b>{format_number(new_value)}</b> | {label} | 🎖 {get_rank(new_value)}")
        except Exception as e:
            results.append(f"❌ <code>{uid}</code> — {html.escape(str(e))}")
    await message.answer("🔥 <b>АКТИВНОСТЬ ОБНОВЛЕНА</b>\n\n" + f"📅 {week_label(start.isoformat(), end.isoformat())}\n\n" + "\n".join(results))
    return True


@dp.message(Command("set"))
async def command_set_activity(message: Message):
    if not activity_admin(message.from_user.id): return
    text=(message.text or "").replace("/set", "", 1).strip()
    if not text:
        await message.answer("Формат: <code>/set UID 3000</code>"); return
    await process_manual_activity(message, f"/set {text}")

@dp.message(Command("activity", "act"))
async def command_activity(message: Message):
    if not activity_admin(message.from_user.id):
        return

    text = message.text or ""
    success = await process_manual_activity(message, text)

    if not success:
        await message.answer(
            "❌ <b>ФОРМАТ</b>\n\n"
            "Добавить: <code>UID +100</code>\n"
            "Вычесть: <code>UID -50</code>\n"
            "Установить: <code>UID =1000</code>\n"
            "Установить 0: <code>UID =0</code>\n\n"
            "Пример:\n"
            "<code>1429409544 +500</code>"
        )


# ----- WEEK (с сокращением /wk) -----

@dp.message(Command("week", "wk"))
async def command_week(message: Message):
    if not activity_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Пример:\n<code>/week 2026-08-17</code> или <code>/wk 2026-08-17</code>")
        return

    try:
        monday = date.fromisoformat(parts[1])
    except ValueError:
        await message.answer("❌ Неверная дата.")
        return

    if monday.weekday() != 0:
        await message.answer("❌ Дата должна быть понедельником.")
        return

    selected_weeks[message.from_user.id] = monday
    sunday = monday + timedelta(days=6)

    await message.answer(
        f"✅ Неделя выбрана:\n\n"
        f"<b>{monday.strftime('%d.%m.%Y')} — {sunday.strftime('%d.%m.%Y')}</b>\n\n"
        "Теперь активность можно добавлять."
    )


# ----- PUBLISH (с сокращением /pub) -----

@dp.message(Command("publish", "pub"))
async def command_publish(message: Message):
    if not is_admin(message.from_user.id): return
    weeks = db.get_unpublished_completed_weeks(datetime.now(ZoneInfo(TIMEZONE)).date().isoformat())
    if not weeks:
        await message.answer("📭 Нет завершённых недель для публикации.")
        return
    week = weeks[0]
    await show_publish_preview(message, week["week_start"], admin_id=message.from_user.id)

async def show_publish_preview(target, week_start: str, admin_id: int):
    week = db.get_week(week_start)
    if not week:
        return
    text = "📢 <b>ПРЕДПРОСМОТР ПУБЛИКАЦИИ</b>\n\n" + await build_week_report(week_start)
    pending_publish[admin_id] = week_start
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"publish_confirm_{week_start}"), InlineKeyboardButton(text="⏰ Отложить", callback_data="publish_delay")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="publish_cancel")]
    ])
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=markup); await target.answer()
    elif isinstance(target, Message):
        await target.edit_text(text, reply_markup=markup)
    else:
        await target.answer(text, reply_markup=markup)


async def build_week_report(week_start: str):
    week = db.get_week(week_start)
    if not week:
        return ""
    players = db.get_week_players(week_start)
    rewards = db.calculate_rewards(week_start)
    lines = ["🏆 <b>ИТОГИ АКТИВНОСТИ ГИЛЬДИИ</b>", "", f"📅 {week_label(week['week_start'], week['week_end'])}", f"🔥 Общая активность: <b>{format_number(week['total_activity'])}</b>", f"👥 Участников: <b>{week['players_count']}</b>", "", "🏆 <b>РЕЙТИНГ</b>", ""]
    for i, player in enumerate(players, 1):
        tg_part = ""
        if player["telegram_id"]:
            tg_label = f"@{html.escape(player['telegram_username'])}" if player['telegram_username'] else "Telegram"
            tg_part = f" — {mention_user(player['telegram_id'], tg_label)}"
        lines.append(f"{get_medal(i)} <b>{html.escape(player['nick'])}</b>{tg_part} — 🔥 {format_number(int(player['activity']))} — 🎖 {get_rank(int(db.get_player(player['player_id'])['total_activity'] or 0))}")
    monthly = [r for r in rewards if r["reward_type"] == "monthly" and r["status"] != "cancelled"]
    weekly = [r for r in rewards if r["reward_type"] == "weekly" and r["status"] != "cancelled"]
    lines.extend(["", "🎁 <b>НАГРАДЫ ЗА АКТИВНОСТЬ</b>", "", "💎 <b>Месячный ваучер — 2600 алмазов</b>"])
    lines.extend(f"• {html.escape(r['nick'] or r['player_id'])} — {format_number(int(r['actual_activity']))}" for r in monthly)
    if not monthly: lines.append("• Нет заслуживших")
    lines.extend(["", "💎 <b>Недельный ваучер — 450 алмазов</b>"])
    lines.extend(f"• {html.escape(r['nick'] or r['player_id'])} — {format_number(int(r['actual_activity']))}" for r in weekly)
    if not weekly: lines.append("• Нет заслуживших")
    return "\n".join(lines)

async def send_week_report(chat_id: int, week_start: str):
    text = await build_week_report(week_start)
    if text:
        await bot.send_message(chat_id=chat_id, text=text)



async def send_activity_report():
    if not GUILD_CHAT_ID:
        return False
    tz=ZoneInfo(TIMEZONE)
    now=datetime.now(tz)
    # Weekly data exists only after the Monday 04:10 rollover.
    week=db.get_latest_week()
    if not week:
        return False
    rows=db.get_week_activity_report(week["week_start"])
    if not rows:
        return False
    lines=["📊 <b>АКТИВНОСТЬ ГИЛЬДИИ</b>","━━━━━━━━━━━━━━━━━━",""]
    for r in rows:
        if r['telegram_id']:
            tg_label = f"@{html.escape(r['telegram_username'])}" if r['telegram_username'] else "Telegram"
            tg_link = mention_user(r['telegram_id'], tg_label)
            lines.append(f"{html.escape(r['nick'])} — {tg_link} — ⚡️ {format_number(int(r['activity'] or 0))}")
        else:
            lines.append(f"{html.escape(r['nick'])} — ⚡️ {format_number(int(r['activity'] or 0))}")
    lines += ["","━━━━━━━━━━━━━━━━━━",f"🕐 Отчёт каждые {ACTIVITY_REPORT_INTERVAL_HOURS} часа"]
    await bot.send_message(GUILD_CHAT_ID, "\n".join(lines))
    db.log("activity_report", None, {"week_start":week["week_start"],"players":len(rows)})
    return True


async def activity_report_loop():
    """Send the current week's activity report every N hours, aligned to Moscow clock."""
    interval=max(1,int(ACTIVITY_REPORT_INTERVAL_HOURS))*3600
    while True:
        try:
            tz=ZoneInfo(TIMEZONE); now=datetime.now(tz)
            next_hour=((now.hour//max(1,int(ACTIVITY_REPORT_INTERVAL_HOURS)))+1)*max(1,int(ACTIVITY_REPORT_INTERVAL_HOURS))
            next_dt=now.replace(minute=0,second=0,microsecond=0)
            if next_hour>=24: next_dt=(next_dt+timedelta(days=1)).replace(hour=0)
            else: next_dt=next_dt.replace(hour=next_hour)
            await asyncio.sleep(max(5,(next_dt-now).total_seconds()))
            await send_activity_report()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка отчёта активности")
            await asyncio.sleep(30)


async def weekly_rollover(today_iso: str):
    """Закрывает прошлую неделю, создаёт новую и лично сообщает владельцу о наградах."""
    try:
        new_start = date.fromisoformat(today_iso)
    except Exception:
        new_start = datetime.now(ZoneInfo(TIMEZONE)).date()
    previous_start = new_start - timedelta(days=7)
    previous_end = new_start - timedelta(days=1)
    previous_week = db.get_week(previous_start.isoformat())
    old_ranks = {}
    if previous_week:
        for row in db.get_week_players(previous_start.isoformat()):
            p_before = db.get_player(row["player_id"])
            if p_before:
                old_ranks[row["player_id"]] = get_rank(int(p_before["total_activity"] or 0))
    rollover_result = db.rollover_week(previous_start.isoformat(), ACTIVITY_COINS_PER_100) if previous_week else {"coins": 0, "activity": 0, "players": 0}
    if previous_week:
        for pid, old_rank in old_ranks.items():
            p_after = db.get_player(pid)
            if not p_after: continue
            new_rank = get_rank(int(p_after["total_activity"] or 0))
            if new_rank != old_rank and p_after["telegram_id"]:
                try:
                    await bot.send_message(int(p_after["telegram_id"]), f"🏆 <b>НОВЫЙ ТИТУЛ!</b>\n\n🎮 {html.escape(p_after['nick'])}\n{html.escape(old_rank)} ➜ <b>{html.escape(new_rank)}</b>\n\n🔥 За всё время: <b>{format_number(int(p_after['total_activity'] or 0))}</b>")
                except Exception:
                    logger.exception("Не удалось отправить уведомление о титуле")
    new_achievements = []
    if previous_week:
        for row in db.get_week_players(previous_start.isoformat()):
            awards = db.award_achievements(row["player_id"])
            if awards:
                p = db.get_player(row["player_id"])
                for a in awards:
                    new_achievements.append((p, a))
                    if p and p["telegram_id"]:
                        try:
                            await bot.send_message(int(p["telegram_id"]), f"🏆 <b>НОВОЕ ДОСТИЖЕНИЕ!</b>\n\n{a['name']}\n{html.escape(a['description'])}")
                        except Exception:
                            logger.exception("Не удалось отправить достижение игроку %s", p["player_id"])
    new_week = db.ensure_week(new_start.isoformat(), (new_start + timedelta(days=6)).isoformat())

    # Награды рассчитываются по закрытой неделе после фиксации её итоговой активности.
    rewards = db.calculate_rewards(previous_start.isoformat()) if db.get_week(previous_start.isoformat()) else []
    weekly = [r for r in rewards if r["reward_type"] == "weekly" and r["status"] != "cancelled"]
    monthly = [r for r in rewards if r["reward_type"] == "monthly" and r["status"] != "cancelled"]

    lines = [
        "🌅 <b>НОВАЯ НЕДЕЛЯ НАЧАЛАСЬ</b>",
        "",
        f"⏰ Закрытие недели: <b>04:10 МСК</b>",
        f"📅 Закрыта неделя: <b>{week_label(previous_start.isoformat(), previous_end.isoformat())}</b>",
        f"📅 Новая неделя: <b>{week_label(new_start.isoformat(), (new_start + timedelta(days=6)).isoformat())}</b>",
        "",
        "🎁 <b>ЗАСЛУЖИЛИ НАГРАДУ</b>",
        ""
    ]
    if monthly:
        lines.append("💎 <b>2600 алмазов — месячный ваучер</b>")
        for r in monthly:
            p = db.get_player(r['player_id'])
            tg = f" — {mention_user(p['telegram_id'], '@'+p['telegram_username'] if p and p['telegram_username'] else 'Telegram')}" if p and p['telegram_id'] else ""
            lines.append(f"• <b>{html.escape(r['nick'] or r['player_id'])}</b> — UID <code>{r['player_id']}</code>{tg} — {format_number(int(r['actual_activity']))} очков")
    if weekly:
        lines.append("💎 <b>450 алмазов — недельный ваучер</b>")
        for r in weekly:
            p = db.get_player(r['player_id'])
            tg = f" — {mention_user(p['telegram_id'], '@'+p['telegram_username'] if p and p['telegram_username'] else 'Telegram')}" if p and p['telegram_id'] else ""
            lines.append(f"• <b>{html.escape(r['nick'] or r['player_id'])}</b> — UID <code>{r['player_id']}</code>{tg} — {format_number(int(r['actual_activity']))} очков")
    if not monthly and not weekly:
        lines.append("• Никто не достиг порога награды.")

    text = "\n".join(lines)
    if OWNER_ID:
        await bot.send_message(OWNER_ID, text)
    db.log("weekly_rollover", OWNER_ID, {
        "closed_week": previous_start.isoformat(),
        "new_week": new_start.isoformat(),
        "weekly_rewards": len(weekly),
        "monthly_rewards": len(monthly),
        "activity_added_to_lifetime": rollover_result.get("activity", 0),
        "coins_awarded": rollover_result.get("coins", 0),
        "new_achievements": len(new_achievements),
    })


async def publish_week(week_start: str):
    if not GUILD_CHAT_ID:
        raise RuntimeError("GUILD_CHAT_ID не установлен.")
    week = db.get_week(week_start)
    if not week or int(week["published"]):
        return False
    if not week["publish_confirmed_at"]:
        logger.info("Неделя %s не подтверждена администраторами — публикация пропущена", week_start)
        return False
    if not db.claim_publish(week_start):
        return False
    try:
        await send_week_report(GUILD_CHAT_ID, week_start)
        db.mark_published(week_start)
        db.log("published", None, json.dumps({"week_start":week_start}, ensure_ascii=False))
        return True
    except Exception:
        db.release_publish_claim(week_start)
        raise


# =========================================================

@dp.callback_query(F.data == "menu_ranks")
async def callback_menu_ranks(callback: CallbackQuery):
    if get_admin_rank(callback.from_user.id) < 7:
        await callback.answer("Нет доступа.", show_alert=True); return
    await callback.message.answer(
        "👑 <b>РАНГИ</b>\n\n" +
        "\n".join(f"{n}. {rank_name(n)}" for n in range(8,0,-1)) +
        "\n\nИспользуй:\n<code>роль TELEGRAM_ID УРОВЕНЬ</code>\n"
        "или ответь на сообщение: <code>роль УРОВЕНЬ</code>"
    )
    await callback.answer()

@dp.callback_query(F.data == "menu_admins")
async def callback_menu_admins(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True); return
    await command_admins(callback.message)
    await callback.answer()

# CALLBACKS - ПОЛНАЯ ПАНЕЛЬ
# =========================================================


# =========================================================
# ADMIN INTERACTIVE FLOWS
# =========================================================

async def show_admin_player_picker(target, action: str, page: int = 0):
    players = db.get_all_players()
    page_size = 8
    total_pages = max(1, (len(players) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    chunk = players[page*page_size:(page+1)*page_size]
    buttons = [[InlineKeyboardButton(text=f"👤 {p['nick'][:28]}", callback_data=f"adminpick_{action}_{p['player_id']}")] for p in chunk]
    nav=[]
    if page: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"adminpickpage_{action}_{page-1}"))
    if page < total_pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"adminpickpage_{action}_{page+1}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")])
    title={"remove":"🗑 <b>УДАЛЕНИЕ ИГРОКА</b>","unbind":"🔓 <b>ОТВЯЗКА TELEGRAM</b>","coins":"🪙 <b>РЕДАКТОР КОИНОВ</b>"}.get(action,"👤 Игроки")
    text=f"{title}\n\nВыбери игрока (страница {page+1}/{total_pages}):"
    if isinstance(target, CallbackQuery): await target.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)); await target.answer()
    else: await target.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("adminpickpage_"))
async def callback_admin_pick_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    parts = callback.data.split("_", 2)
    if len(parts) != 3 or not parts[2].lstrip("-").isdigit():
        await callback.answer("Некорректная страница.", show_alert=True)
        return
    _, action, page = parts
    await show_admin_player_picker(callback, action, int(page))

@dp.callback_query(F.data.startswith("adminpick_remove_"))
async def callback_admin_remove_pick(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    uid=callback.data[len("adminpick_remove_"):]; p=db.get_player(uid)
    if not p: await callback.answer("Игрок не найден", show_alert=True); return
    wp=db.get_latest_week(); row=db.get_week_player(wp["week_start"],uid) if wp else None
    activity=int(row["activity"]) if row else 0
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 Удалить",callback_data=f"remove_confirm_{uid}"),InlineKeyboardButton(text="❌ Отмена",callback_data="menu_removeuser")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_removeuser")]])
    await safe_edit(callback.message, f"🗑 <b>Удалить игрока?</b>\n\n👤 {html.escape(p['nick'])}\n🆔 <code>{uid}</code>\n🔥 Активность текущей недели: <b>{format_number(activity)}</b>\n\nИстория недель сохранится.",reply_markup=kb); await callback.answer()

@dp.callback_query(F.data.startswith("remove_confirm_"))
async def callback_remove_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    uid=callback.data[len("remove_confirm_"):]; p=db.get_player(uid)
    if not p: await callback.answer("Игрок уже удалён",show_alert=True); return
    db.delete_player(uid); db.log("player_deleted",callback.from_user.id,json.dumps({"uid":uid,"nick":p["nick"]},ensure_ascii=False))
    await safe_edit(callback.message, f"✅ Игрок <b>{html.escape(p['nick'])}</b> удалён.\n\n📚 История активности в week_players сохранена.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Удалено")

@dp.callback_query(F.data.startswith("adminpick_unbind_"))
async def callback_admin_unbind_pick(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    uid=callback.data[len("adminpick_unbind_"):]; p=db.get_player(uid)
    if not p: await callback.answer("Игрок не найден",show_alert=True); return
    tg=p["telegram_id"] or "не привязан"
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔓 Отвязать",callback_data=f"unbind_confirm_{uid}"),InlineKeyboardButton(text="❌ Отмена",callback_data="menu_unbind")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_unbind")]])
    await safe_edit(callback.message, f"🔓 <b>Отвязать Telegram?</b>\n\n👤 {html.escape(p['nick'])}\n🆔 <code>{uid}</code>\n📱 Telegram: <code>{tg}</code>",reply_markup=kb); await callback.answer()

@dp.callback_query(F.data.startswith("unbind_confirm_"))
async def callback_unbind_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    uid=callback.data[len("unbind_confirm_"):]; p=db.get_player(uid)
    if not p: await callback.answer("Игрок не найден",show_alert=True); return
    db.unbind_player(uid); db.log("telegram_unbound",callback.from_user.id,json.dumps({"uid":uid},ensure_ascii=False))
    await safe_edit(callback.message, f"✅ Telegram отвязан от <b>{html.escape(p['nick'])}</b>.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Готово")

@dp.callback_query(F.data == "refresh_all")
async def callback_refresh_all(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    players=db.get_all_players(); ok=0; errors=0
    await safe_edit(callback.message, f"⏳ Обновление профилей: 0/{len(players)}")
    for i,p in enumerate(players,1):
        try:
            profile=await ff_client.get_player_profile(p["player_id"],FF_REGION)
            if not profile: raise ValueError("API не вернул профиль")
            if FF_GUILD_ID and str(profile.guild_id or "") != str(FF_GUILD_ID):
                raise ValueError("Игрок больше не состоит в указанной гильдии")
            db.update_player_profile(p["player_id"],profile.raw_data)
            ok+=1
        except Exception as e:
            errors+=1; db.log("api_error",callback.from_user.id,json.dumps({"uid":p["player_id"],"error":str(e)},ensure_ascii=False))
        if i==len(players) or i%5==0:
            try: await safe_edit(callback.message, f"⏳ Обновление профилей: {i}/{len(players)}\n\n✅ {ok} | ❌ {errors}")
            except Exception: pass
    db.log("profiles_refreshed",callback.from_user.id,json.dumps({"total":len(players),"ok":ok,"errors":errors},ensure_ascii=False))
    await safe_edit(callback.message, f"✅ <b>ОБНОВЛЕНИЕ ЗАВЕРШЕНО</b>\n\n✅ Обновлено: <b>{ok}</b>\n❌ Ошибок: <b>{errors}</b>",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer()

@dp.callback_query(F.data.startswith("refresh_player_"))
async def callback_refresh_player(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    uid=callback.data[len("refresh_player_"):]; p=db.get_player(uid)
    if not p: await callback.answer("Игрок не найден",show_alert=True); return
    try:
        profile=await ff_client.get_player_profile(uid,FF_REGION)
        if not profile: raise ValueError("API не вернул профиль")
        if FF_GUILD_ID and str(profile.guild_id or "") != str(FF_GUILD_ID): raise ValueError("Игрок больше не состоит в указанной гильдии")
        db.update_player_profile(uid,profile.raw_data); db.log("profile_refreshed",callback.from_user.id,json.dumps({"uid":uid},ensure_ascii=False))
        await safe_edit(callback.message, f"✅ Профиль <b>{html.escape(profile.nickname)}</b> обновлён.\n\n🌍 {profile.region}\n🏅 Уровень: {profile.level}\n🏰 {html.escape(profile.guild_name or '—')}",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Обновлено")
    except Exception as e:
        db.log("api_error",callback.from_user.id,json.dumps({"uid":uid,"error":str(e)},ensure_ascii=False)); await callback.answer(str(e),show_alert=True)

@dp.message(AddPlayerStates.waiting_uid)
async def add_player_uid(message: Message,state:FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    uid=(message.text or "").strip()
    if not uid.isdigit() or not 8<=len(uid)<=12: await message.answer("❌ UID должен содержать 8–12 цифр."); return
    try:
        profile=await ff_client.get_player_profile(uid,FF_REGION)
        if not profile: raise ValueError("Игрок не найден через Free Fire API.")
        if FF_GUILD_ID and str(profile.guild_id or "") != str(FF_GUILD_ID): raise ValueError("Игрок не состоит в нужной гильдии.")
    except Exception as e: await message.answer(f"❌ {html.escape(str(e))}"); return
    await state.update_data(uid=uid,profile=profile.raw_data,nick=profile.nickname); await state.set_state(AddPlayerStates.waiting_telegram)
    await message.answer(f"👤 {html.escape(profile.nickname)}\n🆔 <code>{uid}</code>\n🏰 {html.escape(profile.guild_name or '—')}\n🌍 {profile.region}\n\nВведите Telegram ID игрока:",reply_markup=back_keyboard())

@dp.message(AddPlayerStates.waiting_telegram)
async def add_player_telegram(message: Message,state:FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    raw=(message.text or "").strip()
    if not raw.lstrip("-").isdigit(): await message.answer("❌ Telegram ID должен быть числом."); return
    tg=int(raw); data=await state.get_data(); existing=db.get_player_by_telegram(tg)
    if existing and existing["player_id"] != data["uid"]: await message.answer("❌ Этот Telegram уже привязан к другому игроку."); return
    await state.update_data(telegram_id=tg); await state.set_state(AddPlayerStates.waiting_nick)
    await message.answer(f"👤 Полученный ник: <b>{html.escape(data['nick'])}</b>\n\nВведите ник или отправьте <code>-</code>, чтобы использовать ник из API:")

@dp.message(AddPlayerStates.waiting_nick)
async def add_player_nick(message: Message,state:FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    data=await state.get_data(); nick=(message.text or "").strip(); nick=data["nick"] if nick=="-" else nick
    if not nick: await message.answer("❌ Ник не может быть пустым."); return
    await state.update_data(nick=nick); await state.clear()
    profile=data["profile"]
    preview=f"📋 <b>ПРЕДПРОСМОТР</b>\n\n👤 {html.escape(nick)}\n🆔 <code>{data['uid']}</code>\n📱 <code>{data['telegram_id']}</code>\n🏰 {html.escape(profile.get('clanBasicInfo',{}).get('clanName') or '—')}\n🌍 {html.escape(str(profile.get('basicInfo',{}).get('region') or '—'))}\n\nДобавить игрока?"
    pending_publish[message.from_user.id]=None
    # Reuse pending_lifetime dictionary only for compact confirmation storage.
    pending_lifetime_activity[message.from_user.id]={"kind":"add_player","data":data}
    await message.answer(preview,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Добавить",callback_data="addplayer_confirm"),InlineKeyboardButton(text="❌ Отмена",callback_data="addplayer_cancel")]]))

@dp.callback_query(F.data == "addplayer_confirm")
async def addplayer_confirm(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    pending=pending_lifetime_activity.pop(callback.from_user.id,None)
    if not pending or pending.get("kind")!="add_player": await callback.answer("Данные устарели",show_alert=True); return
    d=pending["data"]
    try:
        db.add_or_update_player(d["uid"],d["nick"],int(d["telegram_id"]),None,d["profile"]); db.log("player_added",callback.from_user.id,json.dumps({"uid":d["uid"]},ensure_ascii=False))
        await safe_edit(callback.message, f"✅ Игрок <b>{html.escape(d['nick'])}</b> добавлен.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Добавлен")
    except Exception as e: await callback.answer(str(e),show_alert=True)

@dp.callback_query(F.data == "addplayer_cancel")
async def addplayer_cancel(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): return
    pending_lifetime_activity.pop(callback.from_user.id,None); await safe_edit(callback.message, "❌ Добавление отменено.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Отменено")

@dp.callback_query(F.data.startswith("reward_award_"))
async def callback_reward_award(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    try: rid=int(callback.data[len("reward_award_"):])
    except: await callback.answer("Некорректная награда",show_alert=True); return
    db.mark_reward_awarded(rid,callback.from_user.id); db.log("reward_awarded",callback.from_user.id,json.dumps({"reward_id":rid},ensure_ascii=False))
    await callback.answer("Награда отмечена как выданная")
    await callback_menu_rewards(callback)

@dp.callback_query(F.data == "publish_delay")
async def callback_publish_delay(callback:CallbackQuery):
    pending_publish.pop(callback.from_user.id,None); await safe_edit(callback.message, "⏰ Публикация отложена. Её можно открыть снова через 📢 Опубликовать.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Отложено")

@dp.callback_query(F.data == "publish_cancel")
async def callback_publish_cancel(callback:CallbackQuery):
    pending_publish.pop(callback.from_user.id,None); await safe_edit(callback.message, "❌ Публикация отменена.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Отменено")

@dp.callback_query(F.data.startswith("publish_confirm_"))
async def callback_publish_confirm(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    week_start=callback.data[len("publish_confirm_"):]; week=db.get_week(week_start)
    if not week: await callback.answer("Неделя не найдена",show_alert=True); return
    if int(week["published"]): await callback.answer("Уже опубликовано",show_alert=True); return
    db.confirm_publish(week_start); pending_publish.pop(callback.from_user.id,None); db.log("publish_confirmed",callback.from_user.id,json.dumps({"week_start":week_start},ensure_ascii=False))
    try:
        now=datetime.now(ZoneInfo(TIMEZONE)); publish_at=now.replace(hour=PUBLISH_HOUR,minute=PUBLISH_MINUTE,second=0,microsecond=0)
        if now >= publish_at and week["week_end"] < now.date().isoformat():
            await publish_week(week_start)
            await safe_edit(callback.message, f"✅ <b>ПУБЛИКАЦИЯ ПОДТВЕРЖДЕНА И ВЫПОЛНЕНА</b>\n\n📅 {week_label(week_start,week['week_end'])}",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Опубликовано"); return
    except Exception as e:
        logger.exception("Отложенная публикация не выполнена")
        await callback.answer(f"Подтверждено, но публикация не выполнена: {e}",show_alert=True); return
    await safe_edit(callback.message, f"✅ <b>ПУБЛИКАЦИЯ ПОДТВЕРЖДЕНА</b>\n\n📅 {week_label(week_start,week['week_end'])}\n\nВ понедельник в заданное время бот опубликует результаты.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Подтверждено")

async def send_publish_warning(week_start: str):
    db.mark_publish_requested(week_start)
    for role_row in db.get_admin_roles():
        admin_id = int(role_row["telegram_id"])
        try:
            await show_publish_preview(await bot.send_message(admin_id,"📢 Готовлю предпросмотр…"),week_start,admin_id)
        except Exception as e:
            logger.exception("Не удалось отправить предупреждение админу %s: %s",admin_id,e)
            db.log("publish_warning_error",None,json.dumps({"admin_id":admin_id,"error":str(e)},ensure_ascii=False))

@dp.callback_query(F.data == "menu_help")
async def callback_menu_help(callback: CallbackQuery):
    await command_help(callback.message)
    await callback.answer()

@dp.callback_query(F.data == "menu_rules")
async def callback_menu_rules(callback: CallbackQuery):
    await safe_edit(callback.message, RULES_TEXT, reply_markup=back_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "menu_guild_rules")
async def callback_menu_guild_rules(callback: CallbackQuery):
    await send_rules_bundle(callback.message, "guild", reply_markup=panel_keyboard(callback.from_user.id))
    await callback.answer()

@dp.callback_query(F.data == "menu_kv_rules")
async def callback_menu_kv_rules(callback: CallbackQuery):
    await send_rules_bundle(callback.message, "kv", reply_markup=panel_keyboard(callback.from_user.id))
    await callback.answer()

@dp.callback_query(F.data == "owner_coins")
async def callback_owner_coins(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("Только владелец.", show_alert=True); return
    await show_admin_player_picker(callback, "coins", 0)
    await callback.answer()

@dp.callback_query(F.data.startswith("adminpick_coins_"))
async def callback_owner_coins_pick(callback: CallbackQuery, state: FSMContext):
    if not owner_only(callback.from_user.id):
        await callback.answer("Только владелец.", show_alert=True); return
    uid=callback.data[len("adminpick_coins_"):]
    p=db.get_player(uid)
    if not p or not p["telegram_id"]:
        await callback.answer("У игрока нет Telegram-привязки.", show_alert=True); return
    await state.set_state(CoinEditStates.waiting_value)
    await state.update_data(target_telegram_id=int(p["telegram_id"]), target_uid=uid, target_nick=p["nick"])
    await safe_edit(callback.message, f"🪙 <b>РЕДАКТОР КОИНОВ</b>\n\n👤 {html.escape(p['nick'])}\n📱 Telegram: <code>{p['telegram_id']}</code>\n💰 Сейчас: <b>{db.get_coin_balance(p['telegram_id'])}</b>\n\nВведи новое количество коинов:", reply_markup=back_keyboard())
    await callback.answer()

@dp.message(CoinEditStates.waiting_value)
async def owner_coins_value(message: Message, state: FSMContext):
    if not owner_only(message.from_user.id):
        await state.clear(); return
    raw=(message.text or "").strip()
    if not raw.isdigit():
        await message.answer("❌ Введи целое число, например <code>5000</code>."); return
    data=await state.get_data()
    amount=int(raw)
    try:
        db.set_coin_balance(int(data["target_telegram_id"]), amount, message.from_user.id)
        db.log("coins_set", message.from_user.id, {"telegram_id": data["target_telegram_id"], "uid": data["target_uid"], "amount": amount})
        await state.clear()
        await message.answer(f"✅ Баланс игрока <b>{html.escape(data['target_nick'])}</b> установлен: <b>{amount} 🪙</b>", reply_markup=admin_keyboard(message.from_user.id))
    except Exception as e:
        await message.answer(f"❌ {html.escape(str(e))}")

@dp.callback_query(F.data == "menu_coins")
async def callback_menu_coins(callback: CallbackQuery):
    balance=db.get_coin_balance(callback.from_user.id); refs=db.get_referral_stats(callback.from_user.id)
    await safe_edit(callback.message, f"🪙 <b>КОИНЫ</b>\n\nБаланс: <b>{balance}</b>\n👥 Приглашено: <b>{refs}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔗 Моя ссылка",callback_data="menu_ref")],[InlineKeyboardButton(text="🛒 Магазин",callback_data="menu_shop")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_main")]]))
    await callback.answer()

@dp.callback_query(F.data == "menu_ref")
async def callback_menu_ref(callback: CallbackQuery):
    me=await bot.get_me(); link=f"https://t.me/{me.username}?start=ref_{callback.from_user.id}"
    await safe_edit(callback.message, f"🔗 <b>ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА</b>\n\n<code>{link}</code>\n\n🎁 +{REFERRAL_COINS_PER_INVITE} 🪙 за каждого нового участника после его успешной регистрации.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Магазин",callback_data="menu_shop")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_main")]]))
    await callback.answer()

@dp.callback_query(F.data == "menu_shop")
async def callback_menu_shop(callback: CallbackQuery):
    await safe_edit(callback.message, "🛒 <b>МАГАЗИН</b>\n\nВыбери раздел:", reply_markup=shop_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "menu_summon")
async def callback_menu_summon(callback: CallbackQuery):
    """Run the same summon action as /summon instead of only showing a hint."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Только администрация.", show_alert=True)
        return
    if callback.message.chat.type not in ("group", "supergroup"):
        await callback.answer("Открой админ-панель в группе.", show_alert=True)
        return

    now = time.monotonic()
    last = summon_last.get(callback.message.chat.id, 0)
    if now - last < SUMMON_COOLDOWN_SECONDS:
        await callback.answer(
            f"⏳ Созыв можно повторить через {int(SUMMON_COOLDOWN_SECONDS - (now - last))} сек.",
            show_alert=True,
        )
        return

    players = [p for p in db.get_all_players() if p["telegram_id"]]
    if not players:
        await callback.answer("📭 Нет привязанных участников.", show_alert=True)
        return

    summon_last[callback.message.chat.id] = now
    chunks = []
    current = "📣 <b>СОЗЫВ УЧАСТНИКОВ</b>\n\n"
    for p in players:
        item = mention_user(p["telegram_id"], p["nick"]) + " "
        if len(current) + len(item) > 3800:
            chunks.append(current)
            current = ""
        current += item
    if current.strip():
        chunks.append(current)

    for chunk in chunks:
        await callback.message.answer(chunk)
    await callback.answer("📣 Созыв отправлен")

@dp.callback_query(F.data == "menu_moderation")
async def callback_menu_moderation(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("Только администрация.",show_alert=True); return
    await safe_edit(callback.message, "🛡 <b>МОДЕРАЦИЯ</b>\n\nОтветь на сообщение участника:\n/warn причина — предупреждение\n/warnings — история предупреждений\n/mute — ограничение\n/ban причина — бан\n/unban — снять бан\n/kick — исключить", reply_markup=admin_keyboard(callback.from_user.id))
    await callback.answer()

@dp.callback_query(F.data == "menu_progress")
async def callback_menu_progress(callback: CallbackQuery):
    p=db.get_player_by_telegram(callback.from_user.id)
    text=player_progress_text(p['player_id']) if p else "❌ Ты не зарегистрирован."
    await safe_edit(callback.message,text,reply_markup=back_keyboard()); await callback.answer()

@dp.callback_query(F.data.startswith("player_ach_"))
async def callback_player_achievements(callback: CallbackQuery):
    player_id=callback.data[len("player_ach_"):]
    await safe_edit(callback.message, achievements_text(player_id), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📈 Прогресс", callback_data=f"player_prog_{player_id}")],[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"player_{player_id}")]])); await callback.answer()

@dp.callback_query(F.data.startswith("player_prog_"))
async def callback_player_progress(callback: CallbackQuery):
    player_id=callback.data[len("player_prog_"):]
    await safe_edit(callback.message, player_progress_text(player_id), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👑 Достижения", callback_data=f"player_ach_{player_id}")],[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"player_{player_id}")]])); await callback.answer()

@dp.callback_query(F.data == "menu_achievements")
async def callback_menu_achievements(callback: CallbackQuery):
    p=db.get_player_by_telegram(callback.from_user.id)
    text=achievements_text(p['player_id']) if p else "❌ Ты не зарегистрирован."
    await safe_edit(callback.message,text,reply_markup=back_keyboard()); await callback.answer()

@dp.callback_query(F.data == "menu_streak")
async def callback_menu_streak(callback: CallbackQuery):
    await safe_edit(callback.message,streak_text(callback.from_user.id),reply_markup=back_keyboard()); await callback.answer()

@dp.callback_query(F.data == "menu_coin_top")
async def callback_menu_coin_top(callback: CallbackQuery):
    await safe_edit(callback.message,monthly_coin_top_text(),reply_markup=back_keyboard()); await callback.answer()

@dp.callback_query(F.data == "menu_tournaments")
async def callback_menu_tournaments(callback: CallbackQuery):
    await safe_edit(callback.message,tournaments_text(),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Обновить",callback_data="menu_tournaments")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_main")]])); await callback.answer()

@dp.callback_query(F.data == "admin_tournaments")
async def callback_admin_tournaments(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только администрация.", show_alert=True); return
    text=("🏆 <b>УПРАВЛЕНИЕ ТУРНИРАМИ</b>\n\n"
          "/tournament create НАЗВАНИЕ — создать\n"
          "/tournament set ID UID ОЧКИ — установить результат\n"
          "/tournament end ID — завершить\n"
          "/tournaments — посмотреть турниры")
    await safe_edit(callback.message,text,reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer()

@dp.callback_query(F.data == "admin_anticheat")
async def callback_admin_anticheat(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только администрация.", show_alert=True); return
    rows=db.get_anticheat_events(15)
    lines=["🧠 <b>АНТИНАКРУТКА</b>", "", "Подозрительные скачки активности:", ""]
    if not rows: lines.append("✅ Подозрительных скачков пока нет.")
    for r in rows:
        lines.append(f"⚠️ <b>{html.escape(r['nick'] or r['player_id'])}</b> — {format_number(int(r['previous_activity']))} ➜ {format_number(int(r['current_activity']))}\n   {html.escape(r['reason'])}")
    await safe_edit(callback.message,"\n".join(lines),reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer()

@dp.callback_query(F.data == "menu_main")
async def callback_menu_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    pending_lifetime_activity.pop(callback.from_user.id, None)
    pending_weekly_activity.pop(callback.from_user.id, None)
    await safe_edit(callback.message, 
        build_main_panel(callback.from_user.id),
        reply_markup=panel_keyboard(callback.from_user.id)
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_profile")
async def callback_menu_profile(callback: CallbackQuery):
    player = db.get_player_by_telegram(callback.from_user.id)

    if not player:
        await safe_edit(callback.message, 
            "❌ Ты не зарегистрирован.\n\n"
            "Используй:\n<code>/register UID</code>",
            reply_markup=back_keyboard()
        )
        await callback.answer()
        return

    await safe_edit(callback.message, enhanced_player_card(player["player_id"]), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Достижения", callback_data=f"player_ach_{player['player_id']}"), InlineKeyboardButton(text="📈 Прогресс", callback_data=f"player_prog_{player['player_id']}")],
        [InlineKeyboardButton(text="🔥 Серия", callback_data="menu_streak")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_main")]
    ]))
    await callback.answer()


@dp.callback_query(F.data == "menu_stats")
async def callback_menu_stats(callback: CallbackQuery):
    await safe_edit(callback.message, build_stats_text(), reply_markup=back_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "menu_top")
async def callback_menu_top(callback: CallbackQuery):
    latest = get_current_week_record()

    if not latest:
        await safe_edit(callback.message, "📭 Статистика пустая.", reply_markup=back_keyboard())
        await callback.answer()
        return

    players = db.get_week_players(latest["week_start"])
    if not players:
        await safe_edit(callback.message, "📭 Нет данных за эту неделю.", reply_markup=back_keyboard())
        await callback.answer()
        return

    lines = [
        "🏆 <b>ТОП АКТИВНОСТИ</b>",
        "",
        f"📅 {week_label(latest['week_start'], latest['week_end'])}",
        ""
    ]

    for index, player in enumerate(players[:20], start=1):
        rank = get_rank(int(player["total_activity"] or 0))
        tg_part = ""
        if player["telegram_id"]:
            tg_label = f"@{html.escape(player['telegram_username'])}" if player['telegram_username'] else "Telegram"
            tg_part = f" — {mention_user(player['telegram_id'], tg_label)}"
        lines.append(
            f"{get_medal(index)} <b>{html.escape(player['nick'])}</b>{tg_part}\n"
            f"   🔥 {format_number(player['activity'])} | 📚 {format_number(int(player['total_activity'] or 0))} | 🎖 {rank}"
        )

    if len(players) > 20:
        lines.append(f"\n... и ещё {len(players) - 20} участников")

    await safe_edit(callback.message, "\n".join(lines), reply_markup=back_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "menu_low")
async def callback_menu_low(callback: CallbackQuery):
    latest = get_current_week_record()

    if not latest:
        await safe_edit(callback.message, "📭 Статистика ещё не загружена.", reply_markup=back_keyboard())
        await callback.answer()
        return

    players = db.get_low_activity_players(latest["week_start"], 100, LOW_ACTIVITY_LIMIT)

    lines = [
        "🔴 <b>НИЗКАЯ АКТИВНОСТЬ</b>",
        "",
        week_label(latest["week_start"], latest["week_end"]),
        "",
        f"Порог: <b>{LOW_ACTIVITY_LIMIT}</b>",
        ""
    ]

    if not players:
        lines.append("🎉 Игроков с низкой активностью нет!")
    else:
        for index, player in enumerate(players, start=1):
            lines.append(f"{index}. <b>{html.escape(player['nick'])}</b> — {format_number(player['activity'])}")

    await safe_edit(callback.message, "\n".join(lines), reply_markup=back_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "menu_history")
async def callback_menu_history(callback: CallbackQuery):
    await send_history(callback)


@dp.callback_query(F.data == "menu_ff")
async def callback_menu_ff(callback: CallbackQuery):
    await safe_edit(callback.message, 
        "🎮 <b>ПРОВЕРКА FREE FIRE</b>\n\n"
        "Отправь UID командой:\n"
        "<code>/ff UID</code>\n\n"
        "Например:\n"
        "<code>/ff 1429409544</code>",
        reply_markup=back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_register")
async def callback_menu_register(callback: CallbackQuery):
    await safe_edit(callback.message, 
        "📝 <b>РЕГИСТРАЦИЯ</b>\n\n"
        "Отправь свой UID:\n"
        "<code>/register UID</code> или <code>/reg UID</code>\n\n"
        "Например:\n"
        "<code>/register 1429409544</code>\n\n"
        "⚠️ Регистрация только для членов гильдии.",
        reply_markup=back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_week")
async def callback_menu_week(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    current,_=get_default_week(); previous=current-timedelta(days=7)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📅 Текущая: {current.strftime('%d.%m.%Y')}",callback_data=f"week_select_{current.isoformat()}")],
        [InlineKeyboardButton(text=f"📅 Предыдущая: {previous.strftime('%d.%m.%Y')}",callback_data=f"week_select_{previous.isoformat()}")],
        [InlineKeyboardButton(text="✏️ Ввести дату",callback_data="week_enter_date")],
        [InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_main")]
    ])
    await state.clear(); await safe_edit(callback.message, "📆 <b>ВЫБОР НЕДЕЛИ</b>\n\nВыбери неделю или введи понедельник вручную.",reply_markup=kb); await callback.answer()

@dp.callback_query(F.data.startswith("week_select_"))
async def callback_week_select(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    try: monday=date.fromisoformat(callback.data[len("week_select_"):])
    except: await callback.answer("Неверная дата",show_alert=True); return
    if monday.weekday()!=0: await callback.answer("Дата должна быть понедельником",show_alert=True); return
    selected_weeks[callback.from_user.id]=monday; sunday=monday+timedelta(days=6)
    await safe_edit(callback.message, f"✅ Неделя выбрана:\n\n<b>{week_label(monday.isoformat(),sunday.isoformat())}</b>\n\nТеперь импорт и ручная активность будут работать для этой недели.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer("Неделя выбрана")

@dp.callback_query(F.data == "week_enter_date")
async def callback_week_enter_date(callback:CallbackQuery,state:FSMContext):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    await state.set_state(WeekSelectStates.waiting_date); await safe_edit(callback.message, "✏️ Введи дату понедельника в формате <code>YYYY-MM-DD</code>:",reply_markup=back_keyboard()); await callback.answer()

@dp.message(WeekSelectStates.waiting_date)
async def receive_week_date(message:Message,state:FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try: monday=date.fromisoformat((message.text or '').strip())
    except: await message.answer("❌ Неверный формат. Пример: <code>2026-08-17</code>"); return
    if monday.weekday()!=0: await message.answer("❌ Дата должна быть понедельником."); return
    selected_weeks[message.from_user.id]=monday; await state.clear(); sunday=monday+timedelta(days=6); await message.answer(f"✅ Неделя выбрана: <b>{week_label(monday.isoformat(),sunday.isoformat())}</b>",reply_markup=admin_keyboard(message.from_user.id))


@dp.callback_query(F.data == "menu_import")
async def callback_menu_import(callback: CallbackQuery):
    if not activity_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return

    await safe_edit(callback.message, 
        "📥 <b>ИМПОРТ АКТИВНОСТИ</b>\n\n"
        "Можно выбрать неделю:\n"
        "<code>/week YYYY-MM-DD</code>\n\n"
        "Затем отправить таблицу активности.\n\n"
        "Для ручного изменения используй:\n"
        "<code>/activity UID +12000</code>",
        reply_markup=back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_activity_help")
async def callback_activity_help(callback: CallbackQuery, state: FSMContext):
    if not activity_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    start_date,_=get_selected_week(callback.from_user.id); rows=db.get_all_players()
    buttons=[]
    for p in rows:
        wp=db.get_week_player(start_date.isoformat(),p["player_id"]); value=int(wp["activity"]) if wp else 0
        buttons.append([InlineKeyboardButton(text=f"👤 {p['nick'][:24]} — {format_number(value)}",callback_data=f"weekly_player_{p['player_id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_admin")]); await state.clear(); await safe_edit(callback.message, f"🔥 <b>УСТАНОВИТЬ АКТИВНОСТЬ</b>\n\n📅 {week_label(start_date.isoformat(),(start_date+timedelta(days=6)).isoformat())}\n\nВыбери игрока:",reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)); await callback.answer()

@dp.callback_query(F.data.startswith("weekly_player_"))
async def callback_weekly_player(callback:CallbackQuery):
    if not activity_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    uid=callback.data[len("weekly_player_"):]; p=db.get_player(uid)
    if not p: await callback.answer("Игрок не найден",show_alert=True); return
    start_date,_=get_selected_week(callback.from_user.id); row=db.get_week_player(start_date.isoformat(),uid); current=int(row["activity"]) if row else 0
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить",callback_data=f"weekly_add_{uid}"),InlineKeyboardButton(text="➖ Вычесть",callback_data=f"weekly_sub_{uid}")],[InlineKeyboardButton(text="🎯 Установить",callback_data=f"weekly_set_{uid}")],[InlineKeyboardButton(text="⬅️ К игрокам",callback_data="menu_activity_help")]])
    await safe_edit(callback.message, f"👤 <b>{html.escape(p['nick'])}</b>\n🆔 <code>{uid}</code>\n🔥 Текущая активность: <b>{format_number(current)}</b>\n\nВыбери действие:",reply_markup=kb); await callback.answer()

async def start_weekly_edit(callback:CallbackQuery,state:FSMContext,uid:str,mode:str):
    p=db.get_player(uid)
    if not p: await callback.answer("Игрок не найден",show_alert=True); return
    start_date,_=get_selected_week(callback.from_user.id); row=db.get_week_player(start_date.isoformat(),uid); current=int(row["activity"]) if row else 0
    await state.set_state(WeeklyActivityStates.waiting_value); await state.update_data(player_id=uid,mode=mode,week_start=start_date.isoformat())
    prompt={"add":"➕ Введи количество для добавления","sub":"➖ Введи количество для вычитания","set":"🎯 Введи конечную активность"}[mode]
    await safe_edit(callback.message, f"👤 <b>{html.escape(p['nick'])}</b>\n🔥 Сейчас: <b>{format_number(current)}</b>\n\n{prompt}:\n\nОтправь целое число.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена",callback_data=f"weekly_player_{uid}")]])); await callback.answer()

@dp.callback_query(F.data.startswith("weekly_add_"))
async def callback_weekly_add(callback:CallbackQuery,state:FSMContext):
    if not activity_admin(callback.from_user.id): return
    await start_weekly_edit(callback,state,callback.data[len("weekly_add_"):],"add")

@dp.callback_query(F.data.startswith("weekly_sub_"))
async def callback_weekly_sub(callback:CallbackQuery,state:FSMContext):
    if not activity_admin(callback.from_user.id): return
    await start_weekly_edit(callback,state,callback.data[len("weekly_sub_"):],"sub")

@dp.callback_query(F.data.startswith("weekly_set_"))
async def callback_weekly_set(callback:CallbackQuery,state:FSMContext):
    if not activity_admin(callback.from_user.id): return
    await start_weekly_edit(callback,state,callback.data[len("weekly_set_"):],"set")

@dp.message(WeeklyActivityStates.waiting_value)
async def receive_weekly_value(message:Message,state:FSMContext):
    if not activity_admin(message.from_user.id): await state.clear(); return
    raw=(message.text or '').strip().replace(' ','')
    if not raw.isdigit(): await message.answer("❌ Введи только неотрицательное целое число."); return
    value=int(raw); data=await state.get_data(); uid=data.get('player_id'); mode=data.get('mode'); week_start=data.get('week_start'); p=db.get_player(uid)
    if not p or mode not in {'add','sub','set'}: await state.clear(); await message.answer("❌ Сценарий устарел.",reply_markup=admin_keyboard(message.from_user.id)); return
    row=db.get_week_player(week_start,uid); old=int(row['activity']) if row else 0; new=value if mode=='set' else old+(value if mode=='add' else -value)
    if new<0: await message.answer("❌ Активность не может быть отрицательной."); return
    pending_weekly_activity[message.from_user.id]={"uid":uid,"week_start":week_start,"old":old,"new":new,"delta":new-old}
    await state.clear(); await message.answer(f"📋 <b>ПОДТВЕРЖДЕНИЕ</b>\n\n👤 {html.escape(p['nick'])}\n🔥 Было: <b>{format_number(old)}</b>\n🔥 Новое: <b>{format_number(new)}</b>\n📊 Изменение: <b>{'+' if new-old>=0 else ''}{format_number(new-old)}</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Подтвердить",callback_data="weekly_confirm"),InlineKeyboardButton(text="❌ Отмена",callback_data="weekly_cancel")]]))

@dp.callback_query(F.data == "weekly_confirm")
async def callback_weekly_confirm(callback:CallbackQuery):
    if not activity_admin(callback.from_user.id): return
    pending=pending_weekly_activity.pop(callback.from_user.id,None)
    if not pending: await callback.answer("Данные устарели",show_alert=True); return
    start=pending['week_start']; end=(date.fromisoformat(start)+timedelta(days=6)).isoformat()
    try: db.set_week_activity(start,end,pending['uid'],pending['new']); db.log('manual_activity_panel',callback.from_user.id,json.dumps(pending,ensure_ascii=False))
    except Exception as e: await callback.answer(str(e),show_alert=True); return
    p=db.get_player(pending['uid']); await safe_edit(callback.message, f"✅ <b>АКТИВНОСТЬ ИЗМЕНЕНА</b>\n\n👤 {html.escape(p['nick'])}\n🔥 {format_number(pending['old'])} → <b>{format_number(pending['new'])}</b>\n📈 За всё время: <b>{format_number(int(p['total_activity']))}</b>",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer('Сохранено')

@dp.callback_query(F.data == "weekly_cancel")
async def callback_weekly_cancel(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): return
    pending_weekly_activity.pop(callback.from_user.id,None); await safe_edit(callback.message, "❌ Изменение активности отменено.",reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer('Отменено')


@dp.callback_query(F.data == "menu_publish")
async def callback_menu_publish(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return
    if not GUILD_CHAT_ID:
        await safe_edit(callback.message, "❌ GUILD_CHAT_ID не установлен.", reply_markup=back_keyboard())
        await callback.answer(); return
    today=datetime.now(ZoneInfo(TIMEZONE)).date().isoformat()
    weeks=db.get_unpublished_completed_weeks(today)
    if not weeks:
        await safe_edit(callback.message, "📭 <b>НЕТ ЗАВЕРШЁННЫХ НЕДЕЛЬ</b>",reply_markup=back_keyboard()); await callback.answer(); return
    await show_publish_preview(callback,weeks[0]["week_start"],callback.from_user.id)


# ----- АДМИН-ПАНЕЛЬ -----

@dp.callback_query(F.data == "menu_admin")
async def callback_menu_admin(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return

    await safe_edit(callback.message, 
        "👑 <b>АДМИН-ПАНЕЛЬ</b>\n\n"
        "Управление гильдией:",
        reply_markup=admin_keyboard(callback.from_user.id)
    )
    await callback.answer()


@dp.callback_query(F.data == "menu_adduser")
async def callback_menu_adduser(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    await state.clear(); await state.set_state(AddPlayerStates.waiting_uid)
    await safe_edit(callback.message, "➕ <b>ДОБАВИТЬ ИГРОКА</b>\n\nВведите Free Fire UID:", reply_markup=back_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "menu_removeuser")
async def callback_menu_removeuser(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    await show_admin_player_picker(callback, "remove", 0)


@dp.callback_query(F.data == "menu_unbind")
async def callback_menu_unbind(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    await show_admin_player_picker(callback, "unbind", 0)


@dp.callback_query(F.data == "menu_refresh")
async def callback_menu_refresh(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.", show_alert=True); return
    buttons=[[InlineKeyboardButton(text="👥 Обновить всех", callback_data="refresh_all")]]
    for p in db.get_all_players(): buttons.append([InlineKeyboardButton(text=f"🔄 {p['nick'][:30]}", callback_data=f"refresh_player_{p['player_id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")])
    await safe_edit(callback.message, "🔄 <b>ОБНОВИТЬ ПРОФИЛЬ</b>\n\nВыбери игрока или обнови всех:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@dp.callback_query(F.data == "menu_full_stats")
async def callback_menu_full_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return

    text = build_stats_text("current")
    markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📅 Текущая",callback_data="fullstats_current"),InlineKeyboardButton(text="📚 История",callback_data="fullstats_history"),InlineKeyboardButton(text="📊 Всё время",callback_data="fullstats_all")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_admin")]])
    await safe_edit(callback.message, text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data.startswith("fullstats_"))
async def callback_fullstats_mode(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    mode=callback.data[len("fullstats_"):]; text=build_stats_text("current" if mode=="current" else ("history" if mode=="history" else "all"))
    markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📅 Текущая",callback_data="fullstats_current"),InlineKeyboardButton(text="📚 История",callback_data="fullstats_history"),InlineKeyboardButton(text="📊 Всё время",callback_data="fullstats_all")],[InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_admin")]])
    await safe_edit(callback.message, text,reply_markup=markup); await callback.answer()

@dp.callback_query(F.data == "menu_rewards")
async def callback_menu_rewards(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return
    latest = get_current_week_record()
    if not latest:
        await safe_edit(callback.message, "📭 Недель пока нет.", reply_markup=admin_keyboard(callback.from_user.id))
        await callback.answer()
        return
    rewards = db.calculate_rewards(latest["week_start"])
    lines = [
        "🎁 <b>НАГРАДЫ</b>", "",
        f"📅 {week_label(latest['week_start'], latest['week_end'])}", ""
    ]
    if not rewards:
        lines.append("🏆 Заслуживших награды пока нет.")
    buttons=[]
    if not rewards:
        lines.append("🏆 Заслуживших награды пока нет.")
    else:
        for reward in rewards:
            label = "💎 2600 — месячный ваучер" if reward["reward_type"] == "monthly" else "💎 450 — недельный ваучер"
            status = "✅ Выдано" if reward["status"] == "awarded" else ("❌ Отменено" if reward["status"] == "cancelled" else "⏳ Не выдано")
            lines.append(f"{label}\n• <b>{html.escape(reward['nick'] or reward['player_id'])}</b> — {format_number(reward['actual_activity'])}\n  {status}")
            if reward["status"] not in ("awarded","cancelled"):
                buttons.append([InlineKeyboardButton(text=f"✅ Выдано: {reward['nick'] or reward['player_id']}",callback_data=f"reward_award_{reward['reward_id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_admin")])
    await safe_edit(callback.message, "\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


async def render_logs(target, page=0):
    limit=10; rows=db.get_logs(limit, page*limit); total=len(db.get_logs(100000,0)); total_pages=max(1,(total+limit-1)//limit); page=max(0,min(page,total_pages-1)); rows=db.get_logs(limit,page*limit)
    lines=["📋 <b>ПОСЛЕДНИЕ СОБЫТИЯ</b>","",f"Страница <b>{page+1}/{total_pages}</b>",""]
    if not rows: lines.append("📭 Лог пуст.")
    for r in rows:
        details=f" — {html.escape(r['details'] or '')}" if r['details'] else ''
        lines.append(f"<code>{r['created_at']}</code> · <b>{html.escape(r['action'])}</b>{details}")
    nav=[]
    if page: nav.append(InlineKeyboardButton(text="⬅️",callback_data=f"logs_page_{page-1}"))
    if page<total_pages-1: nav.append(InlineKeyboardButton(text="➡️",callback_data=f"logs_page_{page+1}"))
    kb=[]
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="🔄 Обновить",callback_data=f"logs_page_{page}"),InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_admin")])
    markup=InlineKeyboardMarkup(inline_keyboard=kb)
    if isinstance(target,CallbackQuery): await target.message.edit_text("\n".join(lines),reply_markup=markup); await target.answer()
    else: await target.answer("\n".join(lines),reply_markup=markup)

@dp.callback_query(F.data == "menu_logs")
async def callback_menu_logs(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    await render_logs(callback,0)

@dp.callback_query(F.data.startswith("logs_page_"))
async def callback_logs_page(callback:CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("🔒 Только для администратора.",show_alert=True); return
    await render_logs(callback,int(callback.data[len("logs_page_"):]))


@dp.callback_query(F.data == "menu_lifetime_activity")
async def callback_menu_lifetime_activity(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return

    await state.clear()
    pending_lifetime_activity.pop(callback.from_user.id, None)
    players = db.get_all_players()
    if not players:
        await safe_edit(callback.message, "📭 Игроков пока нет.", reply_markup=admin_keyboard(callback.from_user.id))
        await callback.answer()
        return

    lines = [
        "📈 <b>ОБЩЕЕ КОЛИЧЕСТВО ОЧКОВ</b>",
        "",
        "Выбери игрока. Это значение хранится отдельно от активности конкретной недели.",
        ""
    ]
    buttons = []
    for player in players:
        buttons.append([
            InlineKeyboardButton(
                text=f"👤 {player['nick']} — {format_number(int(player['total_activity'] or 0))}",
                callback_data=f"lifetime_player_{player['player_id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")])
    await safe_edit(callback.message, "\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@dp.callback_query(F.data.startswith("lifetime_player_"))
async def callback_lifetime_player(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True)
        return
    player_id = callback.data[len("lifetime_player_"):]
    player = db.get_player(player_id)
    if not player:
        await callback.answer("Игрок не найден.", show_alert=True)
        return

    await state.clear()
    pending_lifetime_activity.pop(callback.from_user.id, None)
    current = int(player["total_activity"] or 0)
    text = (
        "📈 <b>ОБЩИЕ ОЧКИ ЗА ВСЁ ВРЕМЯ</b>\n\n"
        f"👤 <b>{html.escape(player['nick'])}</b>\n"
        f"🆔 UID: <code>{player_id}</code>\n"
        f"🔥 Сейчас: <b>{format_number(current)}</b>\n\n"
        "Выбери действие. Изменение этого числа <b>не меняет недельную активность</b>."
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить", callback_data=f"lifetime_add_{player_id}"),
            InlineKeyboardButton(text="➖ Вычесть", callback_data=f"lifetime_sub_{player_id}")
        ],
        [InlineKeyboardButton(text="🎯 Установить", callback_data=f"lifetime_set_{player_id}")],
        [InlineKeyboardButton(text="⬅️ К игрокам", callback_data="menu_lifetime_activity")]
    ])
    await safe_edit(callback.message, text, reply_markup=keyboard)
    await callback.answer()


async def start_lifetime_edit(callback: CallbackQuery, state: FSMContext, player_id: str, mode: str):
    player = db.get_player(player_id)
    if not player:
        await callback.answer("Игрок не найден.", show_alert=True)
        return
    await state.set_state(LifetimeActivityStates.waiting_value)
    await state.update_data(player_id=player_id, mode=mode)
    current = int(player["total_activity"] or 0)
    prompts = {
        "add": "➕ Введи количество очков для добавления:",
        "sub": "➖ Введи количество очков для вычитания:",
        "set": "🎯 Введи новое общее количество очков:"
    }
    await safe_edit(callback.message, 
        f"👤 <b>{html.escape(player['nick'])}</b>\n"
        f"🔥 Сейчас: <b>{format_number(current)}</b>\n\n"
        f"{prompts[mode]}\n\n"
        "Отправь только целое неотрицательное число.\n"
        "❌ Отмена — кнопкой ниже.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"lifetime_player_{player_id}")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("lifetime_add_"))
async def callback_lifetime_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True); return
    await start_lifetime_edit(callback, state, callback.data[len("lifetime_add_"):], "add")


@dp.callback_query(F.data.startswith("lifetime_sub_"))
async def callback_lifetime_sub(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True); return
    await start_lifetime_edit(callback, state, callback.data[len("lifetime_sub_"):], "sub")


@dp.callback_query(F.data.startswith("lifetime_set_"))
async def callback_lifetime_set(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True); return
    await start_lifetime_edit(callback, state, callback.data[len("lifetime_set_"):], "set")


@dp.message(LifetimeActivityStates.waiting_value)
async def receive_lifetime_activity_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip().replace(" ", "")
    if not raw.isdigit():
        await message.answer("❌ Введи только целое неотрицательное число.")
        return
    value = int(raw)
    data = await state.get_data()
    player_id = data.get("player_id")
    mode = data.get("mode")
    player = db.get_player(player_id)
    if not player or mode not in {"add", "sub", "set"}:
        await state.clear()
        await message.answer("❌ Сценарий редактирования устарел. Открой панель заново.", reply_markup=admin_keyboard(message.from_user.id))
        return
    old_value = int(player["total_activity"] or 0)
    new_value = value if mode == "set" else old_value + (value if mode == "add" else -value)
    if new_value < 0:
        await message.answer("❌ Общее количество очков не может быть отрицательным.")
        return
    pending_lifetime_activity[message.from_user.id] = {
        "player_id": player_id,
        "mode": mode,
        "old": old_value,
        "value": value,
        "new": new_value,
    }
    await message.answer(
        "📋 <b>ПОДТВЕРЖДЕНИЕ</b>\n\n"
        f"👤 {html.escape(player['nick'])}\n"
        f"🔥 Было: <b>{format_number(old_value)}</b>\n"
        f"🔥 Новое: <b>{format_number(new_value)}</b>\n"
        f"📊 Изменение: <b>{'+' if new_value-old_value >= 0 else ''}{format_number(new_value-old_value)}</b>\n\n"
        "Изменится только показатель «за всё время». Недельная статистика останется без изменений.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data="lifetime_confirm"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="lifetime_cancel")
            ]
        ])
    )


@dp.callback_query(F.data == "lifetime_confirm")
async def callback_lifetime_confirm(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True); return
    pending = pending_lifetime_activity.pop(callback.from_user.id, None)
    if not pending:
        await callback.answer("Данные устарели.", show_alert=True)
        await state.clear()
        return
    try:
        result = db.set_total_activity(pending["player_id"], pending["new"])
        db.log("lifetime_activity_changed", callback.from_user.id, json.dumps({"player_id": pending["player_id"], "old": result["old_total"], "new": result["new_total"]}, ensure_ascii=False))
    except Exception as e:
        await callback.answer(str(e), show_alert=True)
        return
    await state.clear()
    player = db.get_player(pending["player_id"])
    await safe_edit(callback.message, 
        "✅ <b>ОБЩИЕ ОЧКИ ИЗМЕНЕНЫ</b>\n\n"
        f"👤 {html.escape(player['nick'])}\n"
        f"🔥 Было: <b>{format_number(result['old_total'])}</b>\n"
        f"🔥 Стало: <b>{format_number(result['new_total'])}</b>\n"
        f"📊 Изменение: <b>{'+' if result['delta'] >= 0 else ''}{format_number(result['delta'])}</b>\n\n"
        "📅 Недельная активность не изменена.",
        reply_markup=admin_keyboard(callback.from_user.id)
    )
    await callback.answer("Сохранено")


@dp.callback_query(F.data == "lifetime_cancel")
async def callback_lifetime_cancel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🔒 Только для администратора.", show_alert=True); return
    pending_lifetime_activity.pop(callback.from_user.id, None)
    await state.clear()
    await safe_edit(callback.message, "❌ Изменение отменено.", reply_markup=admin_keyboard(callback.from_user.id))
    await callback.answer("Отменено")


@dp.callback_query(F.data.startswith("users_page_"))
async def callback_users_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("Ошибка страницы.", show_alert=True)
        return

    await show_users_page(callback, page)


@dp.callback_query(F.data.startswith("player_"))
async def callback_player(callback: CallbackQuery):
    player_id = callback.data[len("player_"):]
    await safe_edit(callback.message, enhanced_player_card(player_id), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Достижения", callback_data=f"player_ach_{player_id}"), InlineKeyboardButton(text="📈 Прогресс", callback_data=f"player_prog_{player_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="users_page_0")]
    ]))
    await callback.answer()


# =========================================================
# ACTIVITY IMPORT (текстовые сообщения)
# =========================================================

def build_preview(entries: list[ActivityEntry], start: date, end: date):
    sorted_entries = sorted(entries, key=lambda x: x.activity, reverse=True)
    total = total_activity(entries)
    low_count = sum(1 for entry in entries if entry.activity < LOW_ACTIVITY_LIMIT)

    lines = [
        "📋 <b>ПРЕДПРОСМОТР</b>",
        "",
        f"📅 <b>{start.strftime('%d.%m.%Y')} — {end.strftime('%d.%m.%Y')}</b>",
        "",
        f"👥 Игроков: <b>{len(entries)}</b>",
        f"🔥 Всего: <b>{format_number(total)}</b>",
        f"🔴 Низкая активность: <b>{low_count}</b>",
        "",
        "🏆 <b>TOP 10</b>",
        ""
    ]

    for index, entry in enumerate(sorted_entries[:10], start=1):
        status, _ = get_status(entry.activity)
        lines.append(
            f"{get_medal(index)} <b>{html.escape(entry.nick)}</b> — {format_number(entry.activity)} {status}"
        )

    lines.extend([
        "",
        "⚠️ Проверь данные.",
        "",
        "После подтверждения они будут сохранены."
    ])

    return "\n".join(lines)


def looks_like_activity_data(text: str) -> bool:
    text = text.strip()
    if not text:
        return False
    if text.startswith("/"):
        return False

    lower = text.lower()
    activity_words = ("активность", "activity", "free fire", "freefire", "ник", "id")

    if any(word in lower for word in activity_words):
        return True

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) >= 2:
        numeric_lines = sum(1 for line in lines if sum(1 for c in line if c.isdigit()) >= 2)
        if numeric_lines >= 2:
            return True

    return False



# =========================================================
# COMMUNITY / IRIS-LIKE FEATURES
# =========================================================

RULES_TEXT = """📜 <b>ПРАВИЛА ГИЛЬДИИ</b>

🚨 <b>ПРАВИЛА «ШЛЮХ НАДЗОР»</b>

1. 🤝 <b>Уважение</b>
Уважительно относимся к участникам.
Шутки разрешены, но если человек просит остановиться — останавливаемся.

2. 🚫 <b>Никакой политики и национальной/религиозной вражды</b>
Мы здесь играть, а не выяснять, кто лучше.
Оскорбления по национальности, религии или политические срачи → предупреждение / кик в зависимости от ситуации.

3. 🎤 <b>Будь частью команды</b>
По возможности играем с микрофоном и общаемся.
Постоянно сидеть молча и никогда не играть с другими участниками — не наша философия.

4. 🎮 <b>Участие в жизни гильдии</b>
КВ, тренировки, совместные игры, мероприятия и розыгрыши — желательно участвовать.

5. 💤 <b>AFK</b>
Уходишь надолго — предупреди руководство.
Если человек пропал без предупреждения, руководство может убрать его из состава.

6. 💎 <b>Награды</b>
Награды получают участники, которые выполнили условия активности.
Попрошайничество запрещено.

7. 👑 <b>Руководство</b>
Решения по составу и организации гильдии принимает руководство.
Если не согласен — спокойно объясни свою позицию, а не устраивай конфликт.

8. 🧠 <b>Адекватность</b>
Неважно, насколько хорошо ты играешь.
Если от тебя постоянно проблемы — место в составе не гарантируется.

9. 🕵️ <b>Никакого намеренного саботажа</b>
Не мешаем КВ, тренировкам и другим участникам специально.

10. 🔥 <b>Главное правило</b>
Не будь просто цифрой в составе. Будь частью «Шлюх надзор».

⚠️ <b>За нарушение:</b>
1 предупреждение → ограничение → исключение
Тяжёлые нарушения могут привести к мгновенному исключению."""

summon_last = {}

def mention_user(user_id: int, name: str = ""):
    """Clickable Telegram mention using registered in-game nick everywhere."""
    uid = int(user_id)
    display = name or "Участник"
    try:
        player = db.get_player_by_telegram(uid)
        if player:
            display = player["nick"] or display
    except Exception:
        pass
    return f'<a href="tg://user?id={uid}">{html.escape(str(display))}</a>'

def registered_display_name(user_id: int, fallback: str = "Участник") -> str:
    """Return registered in-game nick; never expose raw Telegram ID to users."""
    try:
        player = db.get_player_by_telegram(int(user_id))
        if player and player["nick"]:
            return str(player["nick"])
    except Exception:
        pass
    return fallback or "Участник"

@dp.message(Command("achievements", "ach"))
async def command_achievements(message: Message):
    p=db.get_player_by_telegram(message.from_user.id)
    await message.answer(achievements_text(p['player_id']) if p else "❌ Ты не зарегистрирован.")

@dp.message(Command("progress", "prog"))
async def command_progress(message: Message):
    p=db.get_player_by_telegram(message.from_user.id)
    await message.answer(player_progress_text(p['player_id']) if p else "❌ Ты не зарегистрирован.")

@dp.message(Command("streak"))
async def command_streak(message: Message):
    await message.answer(streak_text(message.from_user.id))

@dp.message(Command("coinstop", "coin_top"))
async def command_coin_top(message: Message):
    await message.answer(monthly_coin_top_text())

@dp.message(Command("tournaments", "tournament"))
async def command_tournaments(message: Message):
    parts=(message.text or '').split(maxsplit=2)
    if len(parts)==1:
        await message.answer(tournaments_text()); return
    if not is_admin(message.from_user.id):
        await message.answer("🔒 Только администрация."); return
    action=parts[1].lower()
    try:
        if action=='create' and len(parts)==3:
            tid=db.create_tournament(parts[2],created_by=message.from_user.id)
            await message.answer(f"🏆 Турнир создан: <b>#{tid} {html.escape(parts[2])}</b>")
        elif action=='set' and len(parts)==3:
            vals=parts[2].split()
            if len(vals)!=3: raise ValueError('Формат: /tournament set ID_TOURNAMENT UID ОЧКИ')
            db.set_tournament_points(int(vals[0]),vals[1],int(vals[2])); await message.answer('✅ Очки турнира обновлены.')
        elif action=='end' and len(parts)==3:
            db.close_tournament(int(parts[2])); await message.answer('🏁 Турнир завершён.')
        else:
            await message.answer("🏆 <b>Управление турнирами</b>\n/tournaments\n/tournament create НАЗВАНИЕ\n/tournament set ID UID ОЧКИ\n/tournament end ID")
    except Exception as e:
        await message.answer(f"❌ {html.escape(str(e))}")

@dp.message(Command("rules"))
async def command_rules(message: Message):
    await message.answer(RULES_TEXT)

@dp.message(Command("greet"))
async def command_greet(message: Message):
    await message.answer("👋 <b>Привет!</b>\n\nПривет, ник!\nРады видеть тебя в гильдии. Не забудь вписать\n<code>/reg</code> и свой ID из игры.")

@dp.message(Command("ref", "referral"))
async def command_ref(message: Message):
    if not bot:
        return
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{message.from_user.id}"
    coins = db.get_coin_balance(message.from_user.id)
    refs = db.get_referral_stats(message.from_user.id)
    await message.answer(
        "🔗 <b>ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА</b>\n\n"
        f"{link}\n\n"
        f"👥 Приглашено: <b>{refs}</b>\n"
        f"🪙 Баланс: <b>{coins}</b>\n\n"
        f"🎁 За каждого нового зарегистрированного участника: <b>+{REFERRAL_COINS_PER_INVITE} 🪙</b>\n"
        "Награда начисляется один раз после успешной регистрации приглашённого игрока."
    )

@dp.message(Command("coins", "coin"))
async def command_coins(message: Message):
    balance = db.get_coin_balance(message.from_user.id)
    refs = db.get_referral_stats(message.from_user.id)
    await message.answer(f"🪙 <b>КОИНЫ</b>\n\nБаланс: <b>{balance}</b>\n👥 Успешных приглашений: <b>{refs}</b>\n\n/shop — обменять коины")

SHOP_CORE_PRODUCTS = [
    ("all_locks", "🔓 Снять все блокировки", SHOP_ALL_LOCKS_COINS, "Снимает бан, мут и очищает активные предупреждения."),
    ("unban", "🚫 Снять бан", UNBAN_EXCHANGE_COINS, "Снимает бан в гильдейском Telegram-чате."),
    ("unmute", "🔇 Снять Мут", SHOP_UNMUTE_COINS, "Снимает ограничение на отправку сообщений."),
    ("unwarn", "⚠️ Снять варн", SHOP_UNWARN_COINS, "Очищает активные предупреждения."),
    ("lite", "🥶 Ваучер LITE", SHOP_LITE_COINS, "Заявка администрации на ваучер LITE."),
    ("classic", "⭐️ Ваучер Classic", SHOP_CLASSIC_COINS, "Заявка администрации на ваучер Classic."),
    ("pro", "👑 Ваучер PRO", SHOP_PRO_COINS, "Заявка администрации на ваучер PRO."),
    ("kb", "👑 КБ — 10 каток", SHOP_KB_COINS, "Заявка на буст: 10 каток КБ."),
    ("bo", "⭐️ БО — 10 звёзд", SHOP_BO_COINS, "Заявка на буст: 10 звёзд БО."),
    ("lvlbot", "👤 БОТ ДЛЯ ЛВЛ", SHOP_LVL_BOT_COINS, "Заявка на бота для ЛВЛ."),
]


def shop_keyboard():
    rows=[]
    rows.append([InlineKeyboardButton(text="💋 Чат 💬", callback_data="shop_cat_chat")])
    rows.append([InlineKeyboardButton(text="⚡️ Донат 💎", callback_data="shop_cat_donate")])
    rows.append([InlineKeyboardButton(text="⚡️ БУСТ ⚡️", callback_data="shop_cat_boost")])
    rows.append([InlineKeyboardButton(text="Прочее", callback_data="shop_cat_extra")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_coins")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def shop_category_keyboard(prefix):
    if prefix == "chat": keys = SHOP_CORE_PRODUCTS[:4]
    elif prefix == "donate": keys = SHOP_CORE_PRODUCTS[4:7]
    elif prefix == "boost": keys = SHOP_CORE_PRODUCTS[7:]
    else: keys=[]
    rows=[[InlineKeyboardButton(text=f"{name} — {price} 🪙", callback_data=f"shop_buy_{key}")] for key,name,price,_ in keys]
    if prefix == "extra":
        rows += [[InlineKeyboardButton(text=f"🛍 {r['name']} — {r['price']} 🪙", callback_data=f"shop_product_{r['product_id']}")] for r in db.get_shop_products()]
        if not db.get_shop_products(): rows.append([InlineKeyboardButton(text="Пока нет дополнительных товаров", callback_data="shop_noop")])
    rows.append([InlineKeyboardButton(text="⬅️ Магазин", callback_data="menu_shop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def shop_text(prefix):
    titles={"chat":"💋 <b>ЧАТ 💬</b>","donate":"⚡️ <b>ДОНАТ 💎</b>","boost":"⚡️ <b>БУСТ ⚡️</b>","extra":"<b>Прочее</b>"}
    if prefix == "chat": keys=SHOP_CORE_PRODUCTS[:4]
    elif prefix == "donate": keys=SHOP_CORE_PRODUCTS[4:7]
    elif prefix == "boost": keys=SHOP_CORE_PRODUCTS[7:]
    else: keys=[]
    lines=[titles[prefix],""]
    for key,name,price,desc in keys: lines += [f"{name}",f"Цена: <b>{price} Коинов</b>",""]
    if prefix == "extra":
        products=db.get_shop_products()
        if products:
            for r in products: lines += [f"🛍 {html.escape(r['name'])}",f"Цена: <b>{r['price']} Коинов</b>",""]
        else: lines.append("Дополнительных товаров пока нет.")
    return "\n".join(lines).strip()


@dp.callback_query(F.data == "shop_noop")
async def callback_shop_noop(callback: CallbackQuery):
    await callback.answer("Пока нет дополнительных товаров.")


@dp.callback_query(F.data.startswith("shop_cat_"))
async def callback_shop_category(callback: CallbackQuery):
    category=callback.data[len("shop_cat_"):]
    if category not in {"chat","donate","boost","extra"}:
        await callback.answer("Категория не найдена",show_alert=True); return
    await safe_edit(callback.message, shop_text(category), reply_markup=shop_category_keyboard(category))
    await callback.answer()


async def create_shop_request_for_user(user_id, request_type, price, reward_label):
    balance=db.get_coin_balance(user_id)
    if balance < price:
        raise ValueError(f"Нужно {price} 🪙, у тебя {balance}.")
    pending=[r for r in db.get_shop_requests('pending',100) if int(r['telegram_id'])==int(user_id) and r['request_type']==request_type]
    if pending:
        raise ValueError("У тебя уже есть такая ожидающая заявка.")
    rid=db.create_shop_request(user_id, request_type, price, 0)
    for role_row in db.get_admin_roles():
        try:
            await bot.send_message(int(role_row['telegram_id']), f"🛒 <b>ЗАЯВКА #{rid}</b>\n\n👤 Telegram ID: <code>{user_id}</code>\n📦 {html.escape(reward_label)}\n🪙 Цена: <b>{price}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='✅ Одобрить',callback_data=f'shop_approve_{rid}'),InlineKeyboardButton(text='❌ Отклонить',callback_data=f'shop_decline_{rid}')]]))
        except Exception: pass
    if OWNER_ID:
        try:
            await bot.send_message(OWNER_ID, f"🛒 <b>НОВАЯ ПОКУПКА</b>\n\n👤 Telegram ID: <code>{user_id}</code>\n📦 {html.escape(reward_label)}\n🪙 Цена: <b>{price}</b>\n🆔 Заявка: <code>#{rid}</code>")
        except Exception:
            logger.exception("Не удалось уведомить владельца о покупке")
    return rid


@dp.callback_query(F.data.startswith("shop_buy_"))
async def callback_shop_buy(callback: CallbackQuery):
    key=callback.data[len("shop_buy_"):]
    item=next((x for x in SHOP_CORE_PRODUCTS if x[0]==key),None)
    if not item:
        await callback.answer("Товар не найден",show_alert=True); return
    _,name,price,desc=item
    # Chat moderation products execute immediately against the guild chat.
    if key == "lvlbot":
        await callback.answer("🤖 Бот находится в разработке", show_alert=True)
        return
    if key in {"all_locks","unban","unmute","unwarn"}:
        if not GUILD_CHAT_ID: await callback.answer("GUILD_CHAT_ID не настроен.",show_alert=True); return
        balance=db.get_coin_balance(callback.from_user.id)
        if balance < price:
            await callback.answer(f"Нужно {price} 🪙, у тебя {balance}.", show_alert=True); return
        try:
            if key in {"all_locks","unban"}:
                await bot.unban_chat_member(GUILD_CHAT_ID, callback.from_user.id, only_if_banned=True)
                db.clear_ban(GUILD_CHAT_ID, callback.from_user.id)
            if key in {"all_locks","unmute"}:
                await bot.restrict_chat_member(GUILD_CHAT_ID, callback.from_user.id, permissions=ChatPermissions(can_send_messages=True), until_date=None)
            if key in {"all_locks","unwarn"}:
                db.clear_warnings(GUILD_CHAT_ID, callback.from_user.id)
            db.spend_coins(callback.from_user.id, price, "shop_moderation", json.dumps({"product":key},ensure_ascii=False))
        except Exception as e:
            await callback.answer(f"Не удалось применить товар: {e}",show_alert=True); return
        await callback.message.answer(f"✅ <b>{html.escape(name)}</b> выполнено. Списано {price} 🪙.")
        await callback.answer("Готово")
        return
    try:
        rid=await create_shop_request_for_user(callback.from_user.id,key,price,name)
    except ValueError as e:
        await callback.answer(str(e),show_alert=True); return
    await callback.message.answer(f"📨 Заявка <b>#{rid}</b> создана: {html.escape(name)}. Цена {price} 🪙. После одобрения администрации коины будут списаны.")
    await callback.answer("Заявка создана")


@dp.callback_query(F.data.startswith("shop_product_"))
async def callback_shop_extra_product(callback: CallbackQuery):
    try: pid=int(callback.data[len("shop_product_"):])
    except ValueError: await callback.answer("Товар не найден",show_alert=True); return
    row=next((r for r in db.get_shop_products() if int(r['product_id'])==pid),None)
    if not row: await callback.answer("Товар не найден",show_alert=True); return
    try: rid=await create_shop_request_for_user(callback.from_user.id,f"product_{pid}",int(row['price']),row['name'])
    except ValueError as e: await callback.answer(str(e),show_alert=True); return
    await callback.message.answer(f"📨 Заявка <b>#{rid}</b> создана на «{html.escape(row['name'])}». Цена {row['price']} 🪙.")
    await callback.answer("Заявка создана")


@dp.message(Command("shop"))
async def command_shop(message: Message):
    await message.answer(
        "🛒 <b>МАГАЗИН</b>\n\nВыбери раздел:",
        reply_markup=shop_keyboard(),
    )

@dp.callback_query(F.data == "shop_diamonds")
async def callback_shop_diamonds(callback: CallbackQuery):
    await callback.answer("Этот товар теперь находится в разделе ⚡️ Донат 💎", show_alert=True)
    await safe_edit(callback.message, shop_text("donate"), reply_markup=shop_category_keyboard("donate"))

@dp.callback_query(F.data == "shop_unban")
async def callback_shop_unban(callback: CallbackQuery):
    await callback.answer("Этот товар теперь находится в разделе 💋 Чат 💬", show_alert=True)
    await safe_edit(callback.message, shop_text("chat"), reply_markup=shop_category_keyboard("chat"))

@dp.callback_query(F.data.startswith("shop_approve_"))
async def callback_shop_approve(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("Только администрация.",show_alert=True); return
    rid=int(callback.data.rsplit('_',1)[1])
    try: status=db.handle_shop_request(rid,callback.from_user.id,True)
    except ValueError as e: await callback.answer(str(e),show_alert=True); return
    row=db.get_shop_request(rid)
    await safe_edit(callback.message, f"✅ <b>Заявка #{rid} одобрена</b>\n\n👤 <code>{row['telegram_id']}</code>\n📦 {html.escape(row['request_type'])}\n🪙 Списано: {row['coins_cost']}")
    try: await bot.send_message(row['telegram_id'],f"✅ Заявка #{rid} одобрена. {row['coins_cost']} 🪙 списано.\n📦 Товар: {html.escape(row['request_type'])}\nОжидай выдачу администрацией.")
    except Exception: pass
    await callback.answer("Одобрено")

@dp.callback_query(F.data.startswith("shop_decline_"))
async def callback_shop_decline(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): await callback.answer("Только администрация.",show_alert=True); return
    rid=int(callback.data.rsplit('_',1)[1])
    try: status=db.handle_shop_request(rid,callback.from_user.id,False)
    except ValueError as e: await callback.answer(str(e),show_alert=True); return
    row=db.get_shop_request(rid)
    await safe_edit(callback.message, f"❌ <b>Заявка #{rid} отклонена</b>\n\n👤 <code>{row['telegram_id']}</code>")
    try: await bot.send_message(row['telegram_id'],f"❌ Заявка #{rid} на обмен алмазов отклонена. Коины не списаны.")
    except Exception: pass
    await callback.answer("Отклонено")

@dp.message(Command("shopadd"))
async def command_shopadd(message: Message):
    if not owner_only(message.from_user.id): return
    parts=(message.text or "").split(maxsplit=2)
    if len(parts)<3 or not parts[1].isdigit():
        await message.answer("Формат: <code>/shopadd ЦЕНА НАЗВАНИЕ</code>")
        return
    price=int(parts[1]); name=parts[2].strip()
    if price<=0 or not name: await message.answer("❌ Некорректная цена/название."); return
    pid=db.add_shop_product(name,price)
    await message.answer(f"✅ Дополнительный товар добавлен. ID: <code>{pid}</code>\n{name} — {price} 🪙")

@dp.message(Command("shopremove"))
async def command_shopremove(message: Message):
    if not owner_only(message.from_user.id): return
    parts=(message.text or "").split()
    if len(parts)!=2 or not parts[1].isdigit(): await message.answer("Формат: <code>/shopremove ID</code>"); return
    if not db.deactivate_shop_product(int(parts[1])): await message.answer("❌ Товар не найден."); return
    await message.answer("✅ Дополнительный товар отключён.")

@dp.message(Command("setcoins"))
async def command_setcoins(message: Message):
    if not owner_only(message.from_user.id):
        await message.answer("🔒 Только владелец."); return
    parts=(message.text or "").split()
    if len(parts)!=3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Формат: <code>/setcoins TELEGRAM_ID КОЛИЧЕСТВО</code>"); return
    tg=int(parts[1]); amount=int(parts[2])
    if not db.get_player_by_telegram(tg):
        await message.answer("❌ Игрок с таким Telegram ID не зарегистрирован."); return
    db.set_coin_balance(tg, amount, message.from_user.id)
    db.log("coins_set", message.from_user.id, {"telegram_id":tg,"amount":amount})
    await message.answer(f"✅ Баланс установлен: <code>{tg}</code> → <b>{amount} 🪙</b>")

@dp.message(Command("shopproducts"))
async def command_shopproducts(message: Message):
    if not owner_only(message.from_user.id): return
    rows=db.get_shop_products()
    if not rows: await message.answer("📭 Дополнительных товаров нет."); return
    await message.answer("\n".join(f"#{r['product_id']} — {html.escape(r['name'])} — {r['price']} 🪙" for r in rows))

@dp.message(Command("shoprequests"))
async def command_shoprequests(message: Message):
    if not is_admin(message.from_user.id): return
    rows=db.get_shop_requests('pending',30)
    if not rows: await message.answer("📭 Ожидающих заявок нет."); return
    lines=["🛒 <b>ЗАЯВКИ</b>",""]
    for r in rows: lines.append(f"#{r['request_id']} — <code>{r['telegram_id']}</code> — {r['reward_amount']} 💎 — {r['coins_cost']} 🪙")
    await message.answer("\n".join(lines))

@dp.message(Command("sayall", "сказатьвсем"))
async def command_say_all(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Только руководство (6–8).")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Эту команду используй в групповом чате.")
        return
    parts=(message.text or "").split(maxsplit=1)
    text=parts[1].strip() if len(parts)>1 else ""
    if not text and message.reply_to_message:
        text=(message.reply_to_message.text or message.reply_to_message.caption or "").strip()
    if not text:
        await message.answer("📣 Формат: <code>сказать всем текст</code> или ответь на сообщение.")
        return
    await message.answer("📣 <b>ОБЪЯВЛЕНИЕ</b>\n\n"+html.escape(text))

@dp.message(Command("admincommands", "командыадминов"))
async def command_admin_commands(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Команды админов доступны только рангам 6–8.")
        return
    text = (
        "🛡 <b>КОМАНДЫ АДМИНОВ</b>\n\n"
        "👥 <b>Игроки</b>\n"
        "<code>добавить UID</code> · <code>удалить UID</code> · <code>отвязать UID</code> · <code>обновить</code>\n\n"
        "🔥 <b>Активность</b>\n"
        "<code>установить активность</code> · <code>импорт активности</code> · <code>за всё время</code> · <code>публикация</code>\n\n"
        "⚔️ <b>КВ</b>\n"
        "<code>создать кв</code> · <code>состав кв</code> · <code>заявки кв</code> · <code>назначенные кв</code> · <code>история кв</code>\n"
        "<code>кв результат ID НАШ_СЧЁТ СЧЁТ_ПРОТИВНИКА</code>\n\n"
        "📣 <b>Созыв</b>\n"
        "<code>созыв текст</code> · <code>калл текст</code> · <code>@all текст</code> · <code>призвать @user</code>\n\n"
        "🛡 <b>Модерация</b>\n"
        "<code>варн</code> · <code>мут</code> · <code>бан</code> · <code>кик</code> · <code>снять варн</code> · <code>размут</code> · <code>разбан</code>\n"
        "<code>снять все ограничения</code>\n\n"
        "🧹 <b>Чат</b>\n"
        "<code>очистить чат</code> · <code>стоп чат</code> · <code>запуск чат</code> · <code>сказать всем текст</code>\n\n"
        "👑 <b>Администрация</b>\n"
        "<code>кто админ</code> · <code>роль</code> · <code>повысить</code> · <code>понизить</code>\n\n"
        "🪙 Редактор коинов — только Лидер."
    )
    await message.answer(text)

@dp.message(Command("stopchat", "стопчат"))
async def command_stop_chat(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Только руководство (6–8).")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Эту команду используй в групповом чате.")
        return
    try:
        await bot.set_chat_permissions(message.chat.id, ChatPermissions(can_send_messages=False))
        await message.answer("🔒 <b>ЧАТ ОСТАНОВЛЕН</b>\nОбычным участникам временно запрещено писать.")
    except Exception as exc:
        await message.answer(f"❌ Не удалось остановить чат: {html.escape(str(exc))}")

@dp.message(Command("startchat", "запускчат"))
async def command_start_chat(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Только руководство (6–8).")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Эту команду используй в групповом чате.")
        return
    try:
        await bot.set_chat_permissions(message.chat.id, ChatPermissions(
            can_send_messages=True, can_send_audios=True, can_send_documents=True,
            can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
            can_send_voice_notes=True, can_send_polls=True, can_send_other_messages=True,
            can_add_web_page_previews=True, can_invite_users=True
        ))
        await message.answer("🔓 <b>ЧАТ ЗАПУЩЕН</b>\nОбычным участникам снова разрешено писать.")
    except Exception as exc:
        await message.answer(f"❌ Не удалось запустить чат: {html.escape(str(exc))}")

@dp.message(Command("clearall", "снятьвсеограничения", "убратьвсеограничения"))
async def command_clear_all_restrictions(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Только руководство (6–8).")
        return
    parts=(message.text or "").split()
    token=parts[1] if len(parts)>1 and not message.reply_to_message else None
    target=await resolve_moderation_target(message,token)
    if not target:
        await message.answer("❌ Ответь на сообщение пользователя или укажи @username/Telegram ID.")
        return
    if get_admin_rank(target.id)>=6:
        await message.answer("❌ Нельзя снимать ограничения с администратора через эту команду.")
        return
    problems=[]
    try:
        await bot.unban_chat_member(message.chat.id,target.id,only_if_banned=True)
    except Exception as exc:
        msg=str(exc).lower()
        if "forbidden" in msg or "not enough rights" in msg: problems.append("бан")
    try:
        await bot.restrict_chat_member(message.chat.id,target.id,permissions=ChatPermissions(
            can_send_messages=True, can_send_audios=True, can_send_documents=True,
            can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
            can_send_voice_notes=True, can_send_polls=True, can_add_web_page_previews=True,
            can_invite_users=True
        ),until_date=None)
    except Exception as exc:
        msg=str(exc).lower()
        if "forbidden" in msg or "not enough rights" in msg: problems.append("мут")
    try:
        db.clear_warnings(message.chat.id,target.id)
    except Exception:
        problems.append("варны")
    db.log("clear_all_restrictions",message.from_user.id,{"chat_id":message.chat.id,"user_id":target.id})
    if problems:
        await message.answer(f"⚠️ Ограничения сняты частично: {mention_user(target.id,target.full_name)}\nПроблемы: {', '.join(problems)}")
    else:
        await message.answer(f"🔓 <b>Все ограничения сняты</b>: {mention_user(target.id,target.full_name)}")

@dp.message(Command("mystats", "моястатистика"))
async def command_my_stats(message: Message):
    player=db.get_player_by_telegram(message.from_user.id)
    if not player:
        await message.answer("❌ Ты не зарегистрирован. Используй <code>рег</code>.")
        return
    latest=get_current_week_record()
    weekly=0; position="—"
    if latest:
        row=db.get_week_player(latest["week_start"],player["player_id"])
        if row: weekly=int(row["activity"] or 0)
        try: position=db.get_week_player_position(latest["week_start"],player["player_id"]) or "—"
        except Exception: position="—"
    await message.answer("📊 <b>МОЯ СТАТИСТИКА</b>\n\n"+
        f"🎮 Ник: <b>{html.escape(player['nick'] or '—')}</b>\n"+
        f"🆔 UID: <code>{player['player_id']}</code>\n"+
        f"🔥 За неделю: <b>{weekly}</b>\n"+
        f"🏆 Позиция: <b>{position}</b>\n"+
        f"📈 За всё время: <b>{int(player['total_activity'] or 0)}</b>\n"+
        f"📅 Недель: <b>{int(player['weeks_count'] or 0)}</b>")

@dp.message(Command("myactivity", "мояактивность"))
async def command_my_activity(message: Message):
    player=db.get_player_by_telegram(message.from_user.id)
    if not player:
        await message.answer("❌ Ты не зарегистрирован. Используй <code>рег</code>.")
        return
    latest=get_current_week_record()
    weekly=0
    if latest:
        row=db.get_week_player(latest["week_start"],player["player_id"])
        if row: weekly=int(row["activity"] or 0)
    await message.answer("🔥 <b>МОЯ АКТИВНОСТЬ</b>\n\n"+
        f"🎮 <b>{html.escape(player['nick'] or '—')}</b>\n"+
        f"🔥 Текущая неделя: <b>{weekly}</b>\n"+
        f"📈 За всё время: <b>{int(player['total_activity'] or 0)}</b>")

@dp.message(Command("summon", "call", "созыв", "калл"))
async def command_summon(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Созыв доступен только руководству (6–8).")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Эту команду используй в групповом чате."); return

    raw=(message.text or "").strip()
    body=raw
    if raw.startswith("/"):
        body=raw[1:].strip()
    # The trigger is accepted only when it is the first token/phrase.
    low=body.lower()
    target_username=None
    custom_text=""
    if low.startswith("созыв "):
        custom_text=body[6:].strip()
    elif low.startswith("калл "):
        custom_text=body[5:].strip()
    elif low.startswith("@all "):
        custom_text=body[5:].strip()
    elif low.startswith("призывать всех "):
        custom_text=body[len("призывать всех "):].strip()
    elif low.startswith("призвать @"):
        m=re.match(r"призвать\s+(@[A-Za-z0-9_]{3,})(?:\s+(.*))?$", body, re.I|re.S)
        if m:
            target_username=m.group(1)
            custom_text=(m.group(2) or "").strip()
    elif low in ("созыв","калл","@all","призывать всех"):
        custom_text=""
    elif low in ("призвать",):
        await message.answer("📣 Формат: <code>призвать @username [текст]</code>"); return
    else:
        # Preserve the old no-argument summon behavior only for explicit /summon or /call.
        token=body.split(maxsplit=1)[0].lower() if body else ""
        if token not in ("summon","call"):
            return
        custom_text=""

    now=time.monotonic(); last=summon_last.get(message.chat.id,0)
    if now-last < SUMMON_COOLDOWN_SECONDS:
        await message.answer(f"⏳ Созыв можно повторить через {int(SUMMON_COOLDOWN_SECONDS-(now-last))} сек."); return
    summon_last[message.chat.id]=now

    players=[p for p in db.get_all_players() if p["telegram_id"]]
    if target_username:
        name=target_username.lstrip("@").lower()
        players=[p for p in players if (p["telegram_username"] or "").lower()==name]
        if not players:
            try:
                member=await bot.get_chat_member(message.chat.id,target_username)
                players=[{"telegram_id":member.user.id,"nick":member.user.full_name,"telegram_username":member.user.username}]
            except Exception:
                await message.answer("❌ Пользователь не найден."); return
    if not players:
        await message.answer("📭 Нет привязанных участников."); return

    emojis=["💰","👳🏿","⛹🏼‍♀","🚦","🙋🏾","🧏🏻‍♀","🙋🏻‍♂","🕵🏽‍♂","🧍🏿","✌️","🙎‍♂️","🫦","🍄","👍🏻","🚴🏼‍♀","🫷🏻","🧏‍♀️","🍓","🫁","🧞‍♀️","🤵🏻"]
    # Stable assignment: same player gets the same emoji until the participant list changes.
    players=sorted(players,key=lambda x:(str(x["nick"]).lower(),int(x["telegram_id"])))
    lines=["📣 <b>СОЗЫВ УЧАСТНИКОВ</b>"]
    if custom_text:
        lines.append(html.escape(custom_text))
    lines.append("")
    for idx,p in enumerate(players):
        lines.append(f"{emojis[idx % len(emojis)]} {mention_user(int(p['telegram_id']),p['nick'] or str(p['telegram_id']))}")
    lines.append("\n📣 <b>Призыв окончен.</b>")
    await message.answer("\n".join(lines))

@dp.message(Command("legacy_warn"))
async def command_warn(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию.")
        return
    parts = (message.text or "").split(maxsplit=2)
    token = parts[1] if len(parts) > 1 and not message.reply_to_message else None
    reason = parts[2].strip() if token and len(parts) > 2 else (parts[1].strip() if len(parts) > 1 and message.reply_to_message else "Нарушение правил")
    target = await resolve_moderation_target(message, token)
    if not target:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение.\nПример: <code>/warn @user причина</code>")
        return
    if get_admin_rank(target.id) >= 4:
        await message.answer("❌ Нельзя предупреждать Администратора чата и выше.")
        return
    count = db.add_warning(message.chat.id, target.id, message.from_user.id, reason, 1)
    text = f"⚠️ <b>ПРЕДУПРЕЖДЕНИЕ</b>\n\n{mention_user(target.id,target.full_name)}\nПричина: {html.escape(reason)}\n\nПредупреждений: <b>{count}</b>"
    if count >= 5:
        try:
            await bot.restrict_chat_member(message.chat.id, target.id, permissions=ChatPermissions(can_send_messages=False))
            text += "\n🔇 <b>5 активных предупреждений</b> — запрет писать в чате."
        except Exception as e:
            text += f"\n⚠️ Не удалось выдать ограничение: {html.escape(str(e))}"
    db.log("warn", message.from_user.id, {"chat_id":message.chat.id,"user_id":target.id,"reason":reason,"count":count})
    await message.answer(text)

@dp.message(Command("mute"))
async def command_mute(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию."); return
    parts = (message.text or "").split(maxsplit=2)
    token = parts[1] if len(parts) > 1 and not message.reply_to_message else None
    reason = parts[2] if token and len(parts) > 2 else (parts[1] if message.reply_to_message and len(parts)>1 else "Нарушение правил")
    target = await resolve_moderation_target(message, token)
    if not target:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    if get_admin_rank(target.id) >= 4:
        await message.answer("❌ Нельзя ограничить Администратора чата и выше."); return
    try:
        await bot.restrict_chat_member(message.chat.id,target.id,permissions=ChatPermissions(can_send_messages=False),until_date=datetime.now()+timedelta(hours=1))
        db.log("mute", message.from_user.id, {"chat_id":message.chat.id,"user_id":target.id,"reason":reason,"duration_seconds":3600})
        await message.answer(f"🔇 {mention_user(target.id,target.full_name)} ограничен на 1 час.")
    except Exception as e:
        await message.answer(f"❌ Не удалось ограничить: {html.escape(str(e))}")

@dp.message(Command("unmute"))
async def command_unmute(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию."); return
    parts=(message.text or "").split()
    token=parts[1] if len(parts)>1 and not message.reply_to_message else None
    target=await resolve_moderation_target(message,token)
    if not target:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    try:
        await bot.restrict_chat_member(message.chat.id, target.id, permissions=ChatPermissions(
            can_send_messages=True, can_send_audios=True, can_send_documents=True, can_send_photos=True,
            can_send_videos=True, can_send_video_notes=True, can_send_voice_notes=True,
            can_send_polls=True, can_add_web_page_previews=True, can_invite_users=True
        ), until_date=None)
        db.log("unmute",message.from_user.id,{"chat_id":message.chat.id,"user_id":target.id})
        await message.answer(f"🔊 Ограничение снято: {mention_user(target.id,target.full_name)}")
    except Exception as e:
        await message.answer(f"❌ Не удалось снять мут: {html.escape(str(e))}")

@dp.message(Command("unwarn", "clearwarnings"))
async def command_unwarn(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию."); return
    parts=(message.text or "").split()
    token=parts[1] if len(parts)>1 and not message.reply_to_message else None
    target=await resolve_moderation_target(message,token)
    if not target:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    db.clear_warnings(message.chat.id,target.id)
    db.log("unwarn",message.from_user.id,{"chat_id":message.chat.id,"user_id":target.id})
    await message.answer(f"🧹 Все предупреждения сняты: {mention_user(target.id,target.full_name)}")

@dp.message(Command("warnings", "warns"))
async def command_warnings(message: Message):
    parts = (message.text or "").split()
    token = parts[1] if len(parts) > 1 and not message.reply_to_message else None
    target = await resolve_moderation_target(message, token)
    if not target: target = message.from_user
    rows=db.get_warnings(message.chat.id,target.id,10)
    count=db.get_warning_count(message.chat.id,target.id)
    if not rows: await message.answer(f"⚠️ {mention_user(target.id,target.full_name)} — активных предупреждений нет."); return
    lines=[f"⚠️ <b>ПРЕДУПРЕЖДЕНИЯ</b> — {mention_user(target.id,target.full_name)}",f"Всего: <b>{count}</b>",""]
    lines += [f"• {html.escape(r['reason'])}" for r in rows]
    await message.answer("\n".join(lines))

@dp.message(Command("ban"))
async def command_ban(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию."); return
    parts=(message.text or "").split(maxsplit=2)
    token = parts[1] if len(parts)>1 and not message.reply_to_message else None
    reason = parts[2] if token and len(parts)>2 else (parts[1] if message.reply_to_message and len(parts)>1 else "Нарушение правил")
    target=await resolve_moderation_target(message,token)
    if not target:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    if get_admin_rank(target.id) >= 4:
        await message.answer("❌ Нельзя забанить Администратора чата и выше."); return
    try:
        await bot.ban_chat_member(message.chat.id,target.id)
        db.set_ban(message.chat.id,target.id,message.from_user.id,reason,None)
        db.log("ban",message.from_user.id,{"chat_id":message.chat.id,"user_id":target.id,"reason":reason})
        await message.answer(f"🚫 {mention_user(target.id,target.full_name)} исключён из чата.\nПричина: {html.escape(reason)}")
    except Exception as e: await message.answer(f"❌ Не удалось забанить: {html.escape(str(e))}")

@dp.message(Command("unban"))
async def command_unban(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию."); return
    parts=(message.text or "").split()
    token=parts[1] if len(parts)>1 else None
    target=await resolve_moderation_target(message,token)
    uid=target.id if target else 0
    if not uid:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    try: await bot.unban_chat_member(message.chat.id,uid,only_if_banned=True)
    except Exception as e: await message.answer(f"❌ Не удалось снять бан: {html.escape(str(e))}"); return
    db.clear_ban(message.chat.id,uid)
    db.log("unban",message.from_user.id,{"chat_id":message.chat.id,"user_id":uid})
    await message.answer(f"🔓 Бан снят: <code>{uid}</code>")

@dp.message(Command("kick"))
async def command_kick(message: Message):
    if not moderation_admin(message.from_user.id):
        await message.answer("❌ Нет прав на модерацию."); return
    parts=(message.text or "").split()
    token=parts[1] if len(parts)>1 and not message.reply_to_message else None
    target=await resolve_moderation_target(message,token)
    if not target:
        await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    if get_admin_rank(target.id) >= 4:
        await message.answer("❌ Нельзя исключить Администратора чата и выше."); return
    try:
        await bot.ban_chat_member(message.chat.id,target.id)
        await bot.unban_chat_member(message.chat.id,target.id,only_if_banned=True)
        db.log("kick",message.from_user.id,{"chat_id":message.chat.id,"user_id":target.id})
        await message.answer(f"👢 {mention_user(target.id,target.full_name)} исключён из чата.")
    except Exception as e: await message.answer(f"❌ Не удалось исключить: {html.escape(str(e))}")

@dp.chat_member()
async def on_chat_member_update(event: ChatMemberUpdated):
    if not WELCOME_ENABLED: return
    if event.chat.type not in ("group","supergroup"): return
    old=event.old_chat_member.status; new=event.new_chat_member.status
    if new not in ("member","restricted") or old in ("member","restricted","administrator","creator"): return
    user=event.new_chat_member.user
    if is_admin(user.id): return
    await bot.send_message(event.chat.id, f"👋 <b>Привет, {html.escape(user.full_name)}!</b>\n\nРады видеть тебя в гильдии.\nНе забудь вписать <code>/reg</code> и свой ID из игры.\n\n📜 /rules")


# =========================================================
# IRIS-STYLE COMMUNITY UTILITIES
# Без дуэлей/рулетки/кубов/РП — только полезные функции гильдии.
# =========================================================

def resolve_target_user(message: Message):
    """Resolve Telegram target from reply, @username or numeric ID."""
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user

    text = (message.text or "").strip()
    parts = text.split()
    if len(parts) < 2:
        return None

    token = parts[1].lstrip("@")
    if token.isdigit():
        class SimpleUser:
            def __init__(self, uid):
                self.id = int(uid)
                self.full_name = str(uid)
                self.username = None
        return SimpleUser(token)

    # Try registered Telegram username from the guild DB.
    try:
        row = db.get_player_by_telegram_username(token)
        if row and row["telegram_id"]:
            class SimpleUser:
                def __init__(self, uid, username=None, name=None):
                    self.id = int(uid)
                    self.username = username
                    self.full_name = name or username or str(uid)
            return SimpleUser(row["telegram_id"], token, row["nick"])
    except Exception:
        pass
    return None


async def render_who_user(message: Message, target):
    uid = int(target.id)
    player = db.get_player_by_telegram(uid)
    admin_level = get_admin_rank(uid)

    lines = [
        "👤 <b>ИНФОРМАЦИЯ О ПОЛЬЗОВАТЕЛЕ</b>",
        "",
        f"🆔 Telegram ID: <code>{uid}</code>",
    ]

    username = getattr(target, "username", None)
    if username:
        lines.append(f"🔗 Username: @{html.escape(username)}")
    if getattr(target, "full_name", None):
        lines.append(f"👤 Имя: <b>{html.escape(target.full_name)}</b>")

    if admin_level >= 2:
        lines.append(f"🛡 Ранг: <b>{rank_name(admin_level)}</b>")
    else:
        lines.append("🛡 Ранг: <b>👤 Участник</b>")

    if player:
        lines.extend([
            "",
            "🎮 <b>FREE FIRE</b>",
            f"🏷 Ник: <b>{html.escape(player['nick'])}</b>",
            f"🆔 UID: <code>{player['player_id']}</code>",
            f"🔥 За всё время: <b>{format_number(int(player['total_activity'] or 0))}</b>",
            f"📅 Недель: <b>{int(player['weeks_count'] or 0)}</b>",
        ])
    else:
        lines.extend(["", "🎮 Free Fire: <b>не зарегистрирован</b>"])

    await message.answer("\n".join(lines))


@dp.message(Command("whoami"))
async def command_whoami(message: Message):
    await render_who_user(message, message.from_user)


@dp.message(Command("who", "whois"))
async def command_who(message: Message):
    target = resolve_target_user(message)
    if not target:
        await message.answer(
            "👤 Укажи пользователя или ответь на его сообщение.\n"
            "<code>кто это @username</code>\n"
            "или просто ответь: <code>кто это</code>"
        )
        return
    await render_who_user(message, target)


@dp.message(Command("id", "userid"))
async def command_user_id(message: Message):
    target = resolve_target_user(message) or message.from_user
    await message.answer(f"🆔 Telegram ID: <code>{target.id}</code>")


@dp.message(Command("ping"))
async def command_ping(message: Message):
    await message.answer("🏓 <b>Понг!</b>\n🤖 Vaka на связи.")


@dp.message(Command("chatinfo"))
async def command_chatinfo(message: Message):
    try:
        chat = await bot.get_chat(message.chat.id)
        count = await bot.get_chat_member_count(message.chat.id)
        lines = [
            "🏰 <b>ИНФОРМАЦИЯ О ЧАТЕ</b>",
            "",
            f"💬 Название: <b>{html.escape(chat.title or 'Без названия')}</b>",
            f"🆔 ID: <code>{message.chat.id}</code>",
            f"👥 Участников Telegram: <b>{count}</b>",
            f"🎮 Участников гильдии в БД: <b>{db.get_players_count()}</b>",
        ]
        await message.answer("\n".join(lines))
    except Exception as e:
        await message.answer(f"❌ Не удалось получить информацию о чате: {html.escape(str(e))}")


@dp.message(Command("random"))
async def command_random(message: Message):
    parts = (message.text or "").split()
    if len(parts) not in (2, 3) or not all(p.lstrip("-").isdigit() for p in parts[1:]):
        await message.answer("🎲 Формат: <code>рандом 100</code> или <code>рандом 10 100</code>")
        return
    a = int(parts[1])
    b = int(parts[2]) if len(parts) == 3 else a
    lo, hi = (a, b) if a <= b else (b, a)
    await message.answer(f"🎲 Выпало: <b>{random.randint(lo, hi)}</b>")


@dp.message(Command("choose"))
async def command_choose(message: Message):
    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer("🎯 Формат: <code>выбери первое или второе</code>")
        return
    options = [x.strip() for x in re.split(r"\s+или\s+", raw[1], flags=re.I) if x.strip()]
    if len(options) < 2:
        await message.answer("🎯 Раздели варианты словом <code>или</code>.")
        return
    await message.answer(f"🎯 Я выбираю: <b>{html.escape(random.choice(options))}</b>")


@dp.message(Command("yesno"))
async def command_yesno(message: Message):
    answers = ["Да ✅", "Нет ❌", "Неопределённо 🤔"]
    await message.answer(random.choice(answers))


@dp.message(F.text.regexp(r"^(вака|vaka|вака!|vaka!)$", flags=re.IGNORECASE))
async def vaka_trigger(message: Message):
    await message.answer(
        "🤖 <b>Вака на связи.</b>\n"
        "Я здесь 👋\n\n"
        "Напиши <code>команды</code>, чтобы посмотреть доступные функции."
    )




# =========================================================
# VAKA GUILD KNOWLEDGE / SYSTEM PROMPT
# =========================================================
VAKA_GUILD_SYSTEM_PROMPT = """Ты — Вака, официальный бот гильдии «ШЛЮХ НАДЗОР» в Free Fire.

ТЫ ДОЛЖЕН ЗНАТЬ И ПОНИМАТЬ:
- название гильдии;
- уровень гильдии;
- количество зарегистрированных пользователей;
- количество участников гильдии в игре;
- описание гильдии;
- администрацию и иерархию рангов;
- правила гильдии и правила КВ;
- устройство КВ;
- функции бота, команды и панели;
- RP;
- активность и коины;
- турниры;
- браки;
- заявки;
- социальные сети и общую информацию о гильдии.

РАНГИ:
8 — 👑 Лидер
7 — ⭐️ Заместитель
6 — 🛡 Админ чата
5 — 🔨 Главный проверяющий
4 — 🔍 Проверяющий
3 — Помощник
2 — ⚡️ ССМШИК
1 — 👤 Участник
0 — 🚫 Не зарегистрирован

ПРАВИЛА:
Используй переданные ниже актуальные тексты правил гильдии и КВ как источник истины. Не придумывай отсутствующие пункты.

КВ:
КВ — отдельная система бота: предложения, заявки, составы, назначенные КВ, история и уведомления. Для полного КВ используется состав 4 на 4 и три сквада по 4 игрока. Конкретные настройки, оружие, персонажи, наказания и реванши бери только из актуальных правил КВ.

ФУНКЦИИ:
Бот поддерживает регистрацию и проверку UID Free Fire, профили, активность, коины, рейтинг, достижения, турниры, КВ, заявки, назначенные КВ, историю КВ, браки, RP, модерацию, созыв, правила, помощь и админ-панель.

ПАНЕЛИ:
Гостевая панель — для незарегистрированных. Основная панель — для зарегистрированных. Админ-панель — для руководства 6–8. Редактор коинов доступен только лидеру.

RP:
RP — отдельные действия, выполняемые в ответ на сообщение пользователя или через предусмотренные команды. RP не является разделом панелей. Существующие RP-действия нельзя выдумывать, удалять или переопределять.

АКТИВНОСТЬ И КОИНЫ:
При закрытии недели недельная активность конвертируется в коины по действующей настройке бота, а недельная активность добавляется к показателю «за всё время». Не выдумывай курс или статистику, если они не переданы в контексте.

ЗАЯВКИ:
Заявка в гильдию может содержать UID, причину вступления, как пользователь нашёл гильдию и дополнительную информацию. Решение принимает администрация.

БРАКИ:
Есть предложения, принятие, отказ, развод и просмотр браков.

СОЦСЕТИ:
Бот: @Nadzo69rBot
Чат: @nadzor67
Администрация: @Vavix, @overside1, @swswswqqqq
Новости: @ndzorsh
TikTok: @nadzor_sh

ПОВЕДЕНИЕ:
Отвечай на русском языке, если пользователь не просит другой язык. Будь живым и полезным. Не выдумывай команды, функции, игроков, статистику, права или результаты КВ. Не раскрывай токены, API-ключи, пароли и другие секреты. Если актуальные данные не переданы, честно скажи об этом. Не утверждай, что выполнил действие, если действие реально не выполнялось.
"""

def build_vaka_system_prompt():
    try:
        registered = db.get_players_count()
    except Exception:
        registered = "не удалось получить"
    guild_level = os.getenv("FF_GUILD_LEVEL", "").strip() or "не указан в настройках"
    in_game = os.getenv("FF_GUILD_MEMBERS", "").strip() or "не указан в настройках/API"
    rules_guild = globals().get("RULES_TEXT", "")
    rules_kv = globals().get("RULES_KV", "")
    return (VAKA_GUILD_SYSTEM_PROMPT
            + f"\n\nАКТУАЛЬНЫЙ КОНТЕКСТ:\nНазвание: ШЛЮХ НАДЗОР\nУровень гильдии: {guild_level}\nЗарегистрированных пользователей: {registered}\nУчастников гильдии в игре: {in_game}\nОписание: Free Fire гильдия для совместной игры, КВ, активности и мероприятий.\n\nАКТУАЛЬНЫЕ ПРАВИЛА ГИЛЬДИИ:\n{rules_guild}\n\nАКТУАЛЬНЫЕ ПРАВИЛА КВ:\n{rules_kv}")

async def _ai_openai_compatible(session, base_url, api_key, model, question, provider_name):
    """Call an OpenAI-compatible provider and return non-empty text or raise."""
    if not api_key:
        raise RuntimeError(f"{provider_name}: API key is not configured")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if provider_name == "OpenRouter":
        headers["HTTP-Referer"] = "https://t.me/Nadzo69rBot"
        headers["X-Title"] = "Vaka"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": build_vaka_system_prompt()},
            {"role": "user", "content": question},
        ],
        "temperature": 0.7,
    }
    async with session.post(base_url, json=payload, headers=headers) as resp:
        body = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"{provider_name}: HTTP {resp.status}: {body[:300]}")
        try:
            data = json.loads(body)
        except Exception as exc:
            raise RuntimeError(f"{provider_name}: invalid JSON") from exc
    answer = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    if not answer:
        raise RuntimeError(f"{provider_name}: empty response")
    return answer


async def _ai_gemini(session, model, api_key, question):
    if not api_key:
        raise RuntimeError("Gemini: API key is not configured")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {"contents": [{"parts": [{"text": build_vaka_system_prompt() + "\n\n" + question}]}], "generationConfig": {"temperature": 0.7}}
    async with session.post(url, params={"key": api_key}, json=payload) as resp:
        body = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Gemini: HTTP {resp.status}: {body[:300]}")
        data = json.loads(body)
    parts = []
    for candidate in data.get("candidates", []):
        for part in (candidate.get("content") or {}).get("parts", []):
            if part.get("text"):
                parts.append(str(part["text"]))
    answer = "\n".join(parts).strip()
    if not answer:
        raise RuntimeError("Gemini: empty response")
    return answer


async def _ai_cloudflare(session, model, token, account_id, question):
    if not token or not account_id:
        raise RuntimeError("Cloudflare: token/account id is not configured")
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messages": [
        {"role": "system", "content": build_vaka_system_prompt()},
        {"role": "user", "content": question},
    ]}
    async with session.post(url, json=payload, headers=headers) as resp:
        body = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Cloudflare: HTTP {resp.status}: {body[:300]}")
        data = json.loads(body)
    result = data.get("result") or {}
    answer = str(result.get("response") or result.get("text") or "").strip()
    if not answer:
        raise RuntimeError("Cloudflare: empty response")
    return answer


async def answer_vaka_question(message: Message, question: str):
    question = question.strip()
    if not question:
        return False

    # Lightweight Iris-style deterministic/random replies. These are handled
    # locally so they work even when every AI provider is unavailable.
    low=question.lower()
    actor=registered_display_name(message.from_user.id) if "registered_display_name" in globals() else message.from_user.full_name
    actor=actor or message.from_user.full_name
    if "кто молодец" in low:
        candidates=[p["nick"] for p in db.get_all_players() if p["nick"]]
        name=random.choice(candidates) if candidates else actor
        await message.answer(f"😎 {html.escape(name)} молодец!")
        return True
    if "кто меня любит" in low:
        await message.answer(f"❤️ {html.escape(actor)} любят!")
        return True
    if low.startswith("инфа"):
        percent=random.randint(0,100)
        await message.answer(f"🔮 {percent}%")
        return True

    if not AI_ENABLED:
        await message.answer("😴 Вака устал и не хочет отвечать на тупые вопросы")
        return True

    # The requested provider order is authoritative. A provider is skipped when
    # its key is absent, errors, times out, or returns an empty answer.
    providers = [
        ("gemini-1.5-flash", "Gemini"),
        ("llama-3.3-70b-versatile", "Groq"),
        ("mistral-small-latest", "Mistral"),
        ("@cf/meta/llama-3.1-8b-instruct", "Cloudflare"),
        ("meta-llama/llama-3.1-8b-instruct:free", "OpenRouter"),
    ]
    configured = {
        "Gemini": bool(AI_GEMINI_API_KEY),
        "Groq": bool(AI_GROQ_API_KEY),
        "Mistral": bool(AI_MISTRAL_API_KEY),
        "Cloudflare": bool(AI_CLOUDFLARE_API_TOKEN and AI_CLOUDFLARE_ACCOUNT_ID),
        "OpenRouter": bool(AI_OPENROUTER_API_KEY),
    }

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for model, provider in providers:
            try:
                if not configured[provider]:
                    logger.warning("AI provider skipped: %s (%s)", provider, model)
                    continue
                if provider == "Gemini":
                    answer = await _ai_gemini(session, model, AI_GEMINI_API_KEY, question)
                elif provider == "Groq":
                    answer = await _ai_openai_compatible(session, "https://api.groq.com/openai/v1/chat/completions", AI_GROQ_API_KEY, model, question, provider)
                elif provider == "Mistral":
                    answer = await _ai_openai_compatible(session, "https://api.mistral.ai/v1/chat/completions", AI_MISTRAL_API_KEY, model, question, provider)
                elif provider == "Cloudflare":
                    answer = await _ai_cloudflare(session, model, AI_CLOUDFLARE_API_TOKEN, AI_CLOUDFLARE_ACCOUNT_ID, question)
                else:
                    answer = await _ai_openai_compatible(session, "https://openrouter.ai/api/v1/chat/completions", AI_OPENROUTER_API_KEY, model, question, provider)
                await message.answer(html.escape(answer))
                logger.info("Vaka AI answered with %s/%s", provider, model)
                return True
            except Exception as exc:
                logger.warning("Vaka AI provider failed %s/%s: %s", provider, model, exc)

    await message.answer("😴 Вака устал и не хочет отвечать на тупые вопросы")
    return True

async def handle_monitoring_payload(message: Message, text: str):
    if message.chat.id != MONITORING_CHAT_ID:
        return False
    week_start, entries, tournaments, errors = parse_monitoring_text(text)
    if errors:
        await message.answer("❌ <b>Ошибка мониторинга</b>\n\n" + "\n".join(f"• {html.escape(e)}" for e in errors[:10]))
        return True
    current_start, current_end = get_default_week()
    if week_start != current_start.isoformat():
        await message.answer(
            f"⚠️ <b>Неверная неделя мониторинга.</b>\n\n"
            f"Сейчас принимается: <code>{current_start.isoformat()}</code>\n"
            f"Получено: <code>{html.escape(week_start)}</code>"
        )
        return True
    try:
        result = db.save_monitoring_snapshot(week_start, current_end.isoformat(), entries)
        for tid, uid, points in tournaments:
            try:
                db.set_tournament_points(tid, uid, points)
            except Exception as exc:
                errors.append(f"Турнир #{tid}, UID {uid}: {exc}")
        db.log("monitoring_snapshot", message.from_user.id, {
            "chat_id": message.chat.id,
            "week_start": week_start,
            "players": len(entries),
            "tournaments": len(tournaments),
        })
        for pid, previous, current, reason in result["anomalies"]:
            db.add_anticheat_event(week_start, pid, previous, current, reason)
            p = db.get_player(pid)
            if OWNER_ID and p:
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"🧠 <b>АНТИНАКРУТКА</b>\n\n"
                        f"👤 {html.escape(p['nick'])}\n"
                        f"📅 {format_date(week_start)}\n"
                        f"📉 Было: {format_number(previous)}\n"
                        f"📈 Стало: {format_number(current)}\n"
                        f"⚠️ {html.escape(reason)}"
                    )
                except Exception:
                    logger.exception("Не удалось отправить anti-cheat уведомление")
        total = sum(e.activity for e in entries)
        reply = (
            "✅ <b>МОНИТОРИНГ ПРИНЯТ</b>\n\n"
            f"📅 Неделя: <code>{week_start}</code>\n"
            f"👥 Игроков: <b>{len(entries)}</b>\n"
            f"🔥 Общая активность: <b>{format_number(total)}</b>\n"
            f"🏆 Турнирных записей: <b>{len(tournaments)}</b>\n"
            "\nℹ️ В расчёт берётся только PLAYER → WEEK_ACTIVITY.\n"
            "GAME_TOTAL не влияет на lifetime или коины."
        )
        if result["anomalies"]:
            reply += f"\n\n🧠 Аномалий: <b>{len(result['anomalies'])}</b> — уведомление отправлено владельцу."
        if errors:
            reply += "\n\n⚠️ " + "\n".join(html.escape(e) for e in errors[:5])
        await message.answer(reply)
    except Exception as exc:
        logger.exception("Ошибка сохранения мониторинга")
        await message.answer(f"❌ Мониторинг не сохранён: {html.escape(str(exc))}")
    return True


RP_ACTIONS={
"ударить":"ударил","обнять":"обнял","поцеловать":"поцеловал","погладить":"погладил","укусить":"укусил","прижать":"прижал","подмигнуть":"подмигнул","пнуть":"пнул","дать пять":"дал пять","пожать руку":"пожал руку","подарить цветок":"подарил цветок","потанцевать":"потанцевал с","облить водой":"облил водой","украсть тапок":"украл тапок у","похлопать":"похлопал","поиграть":"поиграл с","поддержать":"поддержал","рассмешить":"рассмешил","поднять настроение":"поднял настроение","дать конфету":"дал конфету","пригласить на чай":"пригласил на чай","покормить":"покормил","укрыть пледом":"укрыл пледом","потрепать":"потрепал по голове","сделать комплимент":"сделал комплимент","позвать гулять":"позвал гулять","пригласить танцевать":"пригласил танцевать","помахать":"помахал","поклониться":"поклонился","дать кулак":"дал кулачок","пожать плечами":"пожал плечами","спрятаться":"спрятался от","догнать":"догнал","поймать":"поймал","защитить":"защитил","извиниться":"извинился перед","испугать":"испугал","поздравить":"поздравил","потрогать":"потрогал","похвалить":"похвалил","понюхать":"понюхал","ущипнуть":"ущипнул","шлепнуть":"шлёпнул","пригласить на чаёк":"пригласил на чаёк","облизать":"облизал","куснуть":"куснул","лизнуть":"лизнул","поиграть в игру":"поиграл в игру с","помочь":"помог","успокоить":"успокоил","пожелать удачи":"пожелал удачи","подарить подарок":"подарил подарок","дать шоколадку":"дал шоколадку","кинуть мем":"кинул мем","поделиться едой":"поделился едой с","рассказать анекдот":"рассказал анекдот","пригласить погулять":"пригласил погулять","сделать завтрак":"приготовил завтрак для","сходить в кино":"сходил в кино с","поговорить по душам":"поговорил по душам с","поблагодарить":"поблагодарил","толкнуть":"толкнул","отпихнуть":"отпихнул","потянуть за волосы":"потянул за волосы","таскать за волосы":"потащил за волосы","признаться в любви":"признался в любви к","пригласить на свидание":"пригласил на свидание","устроить сюрприз":"устроил сюрприз для","пожелать спокойной ночи":"пожелал спокойной ночи","пожелать доброго утра":"пожелал доброго утра","встретить":"встретил","проводить":"проводил","прикрыть":"прикрыл","поддержать морально":"поддержал морально","утешить":"утешил","развеселить":"развеселил","похлопать по плечу":"похлопал по плечу","потанцевать вместе":"потанцевал вместе с","послать воздушный поцелуй":"послал воздушный поцелуй","помахать рукой":"помахал рукой","подарить розу":"подарил розу","подарить конфету":"подарил конфету","подарить плюшку":"подарил плюшку","дать оберег":"дал оберег","пригласить в кино":"пригласил в кино","сделать чай":"сделал чай для","принести кофе":"принёс кофе для","дать совет":"дал совет","выслушать":"выслушал","поболтать":"поболтал с","поиграть вместе":"поиграл вместе с","пожать руку крепко":"крепко пожал руку"
}
RP_EMOJI={"ударить":"🤜","обнять":"🤗","поцеловать":"💋","погладить":"👋","укусить":"🧛","прижать":"🫂","подмигнуть":"😉","пнуть":"🦶","дать пять":"🙏","пожать руку":"🤝","подарить цветок":"🌹","потанцевать":"💃","облить водой":"💦","украсть тапок":"🥿","похлопать":"👏","поиграть":"🎮","поддержать":"💙","рассмешить":"😂","поднять настроение":"😊","дать конфету":"🍬","пригласить на чай":"☕","покормить":"🍕","укрыть пледом":"🧣","потрепать":"🫳","сделать комплимент":"✨","позвать гулять":"🚶","пригласить танцевать":"🕺","помахать":"👋","поклониться":"🙇","дать кулак":"👊","пожать плечами":"🤷","спрятаться":"🙈","догнать":"🏃","поймать":"🫴","защитить":"🛡️","извиниться":"🙏","испугать":"😱","поздравить":"🥳","потрогать":"🙌","похвалить":"👏","понюхать":"👃","ущипнуть":"🤏","шлепнуть":"🍑","пригласить на чаёк":"☕","облизать":"👅","куснуть":"😬","лизнуть":"👅","помочь":"🫶","успокоить":"🫂","пожелать удачи":"🍀","подарить подарок":"🎁","дать шоколадку":"🍫","кинуть мем":"😂","поделиться едой":"🍽️","рассказать анекдот":"😄","пригласить погулять":"🌳","сделать завтрак":"🥞","сходить в кино":"🎬","поговорить по душам":"💬","поблагодарить":"🙏","толкнуть":"👉","отпихнуть":"🫷","потянуть за волосы":"💇","таскать за волосы":"💇","признаться в любви":"❤️","пригласить на свидание":"💐","устроить сюрприз":"🎁","пожелать спокойной ночи":"🌙","пожелать доброго утра":"🌅","встретить":"👋","проводить":"🚶","прикрыть":"🛡️","поддержать морально":"💙","утешить":"🫂","развеселить":"😂","похлопать по плечу":"👏","потанцевать вместе":"💃","послать воздушный поцелуй":"😘","помахать рукой":"👋","подарить розу":"🌹","подарить конфету":"🍬","подарить плюшку":"🧸","дать оберег":"🪬","пригласить в кино":"🎬","сделать чай":"☕","принести кофе":"☕","дать совет":"💡","выслушать":"👂","поболтать":"💬","поиграть вместе":"🎮","пожать руку крепко":"🤝"}
RP_ALIASES={"обними":"обнять","обнимашки":"обнять","целовать":"поцеловать","поцелуй":"поцеловать","лапать":"погладить","куснуть":"укусить","дай пять":"дать пять","пятюню":"дать пять","цветочек":"подарить цветок","танцы":"потанцевать","потанцуем":"потанцевать","комплимент":"сделать комплимент","чмок":"поцеловать","помоги":"помочь","кусь":"укусить","лизь":"лизнуть","чмокнуть":"поцеловать","пятюню":"дать пять","шлеп":"шлепнуть","шлёп":"шлепнуть","толкни":"толкнуть","оттолкнуть":"отпихнуть","поблагодари":"поблагодарить","комплимент":"сделать комплимент","признайся в любви":"признаться в любви","свидание":"пригласить на свидание"}
RP_ACTION_PATTERN = r"^(?:" + "|".join(map(re.escape, sorted(list(RP_ACTIONS)+list(RP_ALIASES),key=len,reverse=True))) + r")(?:\s+|$)"

async def _rp_target(message, text):
    """Resolve RP target exactly from reply first, then @username or numeric Telegram ID."""
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    raw=(text or "").strip()
    m=re.search(r"@([A-Za-z0-9_]{3,})", raw)
    if m:
        try:
            row=db.get_player_by_telegram_username(m.group(1))
            if row and row["telegram_id"]:
                return type("RPUser", (), {"id": int(row["telegram_id"]), "full_name": row["nick"] or row["telegram_username"] or m.group(1)})()
        except Exception:
            pass
        try:
            chat=await bot.get_chat("@"+m.group(1))
            if getattr(chat, "id", None):
                return chat
        except Exception:
            pass
    m=re.search(r"(?<!\d)(-?\d{5,15})(?!\d)", raw)
    if m:
        try:
            chat=await bot.get_chat(int(m.group(1)))
            if getattr(chat, "id", None):
                return chat
        except Exception:
            pass
    return None

async def _run_rp(message, action, target):
    if not target:
        await message.answer("👉 Ответь на сообщение пользователя или укажи @username/ID."); return
    if target.id==message.from_user.id:
        await message.answer("❌ Нельзя выполнить это действие на себе."); return
    if message.chat.type != "private" and db.iris_chat_settings(message.chat.id)["rp_enabled"] is False:
        return
    # Use the unified RP registry. Legacy actions and all IRIS_EXTRA_RP entries
    # (including entries already present in the user's file) remain available.
    registry = globals().get("ALL_RP_ACTIONS", RP_ACTIONS)
    if action not in registry:
        await message.answer("❌ Неизвестное RP-действие.")
        return
    entry = registry[action]
    if isinstance(entry, tuple):
        emoji, verb = entry
    else:
        emoji, verb = RP_EMOJI.get(action, "✨"), entry
    await message.answer(f"{emoji} | {mention_user(message.from_user.id,message.from_user.full_name)} {verb} {mention_user(target.id,target.full_name)}")
    db.log_rp_action(message.from_user.id,target.id,action)


@dp.message(F.text.regexp(r"^VAKA_MONITORING_V1(?:\s|$)", flags=re.I | re.M))
async def monitoring_message_handler(message: Message):
    if message.chat.id != MONITORING_CHAT_ID:
        await message.answer("🔒 Этот формат принимается только в чате мониторинга.")
        return
    await handle_monitoring_payload(message, message.text or "")



# --- IRIS RP DISPATCHER (ordered before generic text fallback) ---
# The RP vocabulary is defined above; keeping the pattern here prevents
# NameError during decorator registration and ensures RP is not swallowed
# by the generic text handler.
RP_ACTIONS_V71 = RP_ACTIONS.copy()
RP_ALIASES_V71 = RP_ALIASES.copy()
# The authoritative RP registry is assembled later in this file. The handler
# is registered before that block, so its filter must not freeze an older
# vocabulary at import time. The parser below consults the final registry
# when messages are actually processed.
RP_INVOCATION_PATTERN = r"^(?:[/!.]|Ирис\s+)?.+$"

def _parse_rp_invocation(text):
    raw=(text or "").strip()
    normalized=re.sub(r"^(?:[/!.]|Ирис\s+)","",raw,flags=re.I).strip()
    low=normalized.lower()

    registry = globals().get("ALL_RP_ACTIONS")
    aliases = globals().get("ALL_RP_ALIASES")
    if registry is None:
        registry = RP_ACTIONS_V71
    if aliases is None:
        aliases = RP_ALIASES_V71

    phrases = sorted(
        set(str(x).lower() for x in registry) | set(str(x).lower() for x in aliases),
        key=len,
        reverse=True,
    )
    for phrase in phrases:
        if low == phrase or low.startswith(phrase+" "):
            action=str(aliases.get(phrase, phrase)).lower()
            rest=normalized[len(phrase):].strip()
            return action, rest
    return None, ""

async def _dispatch_unified_text_command(message: Message, state: FSMContext | None = None) -> bool:
    """Route recognized slash/no-slash commands through existing handlers.

    The project contains several legacy routers. This bridge prevents the broad
    RP compatibility handler from swallowing ordinary commands while preserving
    all existing handlers and aliases.
    """
    text=(message.text or "").strip()
    if not text:
        return False
    if state is not None and await state.get_state():
        return False

    is_slash=text.startswith("/")
    normalized=text[1:].strip() if is_slash else text
    normalized=re.sub(r"^(?:!|\.|Ирис\s+)","",normalized,flags=re.I).strip()
    if not normalized:
        return False
    low=normalized.lower()

    alias_map=globals().get("V71_NO_SLASH", {})
    fn_map=globals().get("V71_FN", {})

    # Marriage has its own priority dispatcher and must keep accepting the
    # reply/@username workflow and "брак да/нет" responses.
    marriage_routes={
        "брак": "iris_marriage_priority", "брак да": "iris_marriage_priority", "брак нет": "iris_marriage_priority",
        "мой брак": "iris_my_marriage_priority", "моя брак": "iris_my_marriage_priority",
        "браки": "iris_marriages_priority", "список браков": "iris_marriages_priority",
        "развод": "iris_divorce_priority", "статус отн": "iris_relationship_priority",
        "статус отношений": "iris_relationship_priority",
    }
    for phrase in sorted(marriage_routes,key=len,reverse=True):
        if low==phrase or low.startswith(phrase+" "):
            fn=globals().get(marriage_routes[phrase])
            if fn:
                await fn(message)
                return True

    for phrase in sorted(alias_map,key=len,reverse=True):
        phrase_low=str(phrase).lower()
        if low==phrase_low or low.startswith(phrase_low+" "):
            cmd=alias_map[phrase]
            original=normalized[len(str(phrase)):].strip()
            if cmd=="kvproposal":
                await state.set_state(KVProposalStates.guild)
                await message.answer("⚔️ <b>Предложение КВ</b>\n\n1/5. Название вашей гильдии:")
                return True
            fn=fn_map.get(cmd)
            if fn:
                await fn(message.model_copy(update={"text":"/"+cmd+(" "+original if original else "")}))
                return True
            return False

    # Explicit slash-only aliases, including new compound commands.
    token=normalized.split(maxsplit=1)[0].lower()
    slash_map=globals().get("UNIFIED_SLASH_ALIASES", {})
    cmd=slash_map.get(token)
    if cmd:
        fn=fn_map.get(cmd)
        if fn:
            original=normalized[len(token):].strip()
            await fn(message.model_copy(update={"text":"/"+cmd+(" "+original if original else "")}))
            return True
    return False

@dp.message(StateFilter(None), F.text.regexp(RP_INVOCATION_PATTERN, flags=re.I))
async def iris_rp_authoritative_handler(message: Message, state: FSMContext):
    text=(message.text or "").strip()
    if not text:
        return
    action, rest=_parse_rp_invocation(text)
    if action and action in ALL_RP_ACTIONS:
        target=await _rp_target(message, rest)
        await _run_rp(message, action, target)
        return

    # Do not swallow non-RP commands.
    if await _dispatch_unified_text_command(message, state):
        return

    # Preserve Vaka's conversational AI entry point.
    m_vaka=re.match(r"^(?:вака|vaka)[,:]?\s+(.+)$",text,re.I|re.S)
    if m_vaka:
        await answer_vaka_question(message,m_vaka.group(1))
        return

    # Preserve the existing generic conversational/AI pipeline for ordinary
    # text. The early RP handler must not make normal chat messages disappear.
    fallback = globals().get("receive_activity_or_alias")
    if fallback:
        await fallback(message, state)
        return

@dp.message(StateFilter(None), F.text.regexp(r"^(?:[/!.]|Ирис\s+)?[A-Za-zА-Яа-яЁё0-9_\-]+(?:\s+.*)?$", flags=re.I))
async def iris_custom_rp_handler(message: Message):
    action, rest=_parse_rp_invocation(message.text or "")
    if action:
        return
    raw=(message.text or "").strip()
    normalized=re.sub(r"^(?:[/!.]|Ирис\s+)","",raw,flags=re.I).strip()
    name=normalized.split(maxsplit=1)[0].lower() if normalized else ""
    row=db.iris_custom_rp(name)
    if not row:
        return
    target=await _rp_target(message, normalized)
    if not target:
        await message.answer("👉 Ответь на сообщение пользователя или укажи @username.")
        return
    template=str(row["template"])
    template=template.replace("{цель}",mention_user(target.id,target.full_name)).replace("{target}",mention_user(target.id,target.full_name))
    actor=mention_user(message.from_user.id,message.from_user.full_name)
    if "{я}" in template or "{actor}" in template:
        template=template.replace("{я}",actor).replace("{actor}",actor)
    else:
        template=f"{actor} {template}"
    await message.answer(f"{row['emoji']} | {template}")
    db.log_rp_action(message.from_user.id,target.id,name)
# --- END IRIS RP DISPATCHER ---


# =========================================================
# PRIORITY IRIS MARRIAGE DISPATCHER
# Must be registered before the generic no-slash dispatcher.
# =========================================================

async def _resolve_marriage_target(message):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    m=re.search(r"@([A-Za-z0-9_]{3,})", message.text or "")
    if m:
        try:
            row=db.get_player_by_telegram_username(m.group(1))
            if row and row["telegram_id"]:
                return type("MarriageUser",(),{
                    "id":int(row["telegram_id"]),
                    "full_name":row["nick"] or row["telegram_username"] or m.group(1),
                    "username":row["telegram_username"] or m.group(1)
                })()
        except Exception:
            pass
        try:
            return (await bot.get_chat_member(message.chat.id,"@"+m.group(1))).user
        except Exception:
            return None
    return None

def _marriage_mention(user_id, full_name):
    return mention_user(int(user_id), full_name or str(user_id))

@dp.message(StateFilter(None), F.text.regexp(
    r"^(?:брак(?:\s+(?:да|нет))?|!брак(?:\s+@?[A-Za-z0-9_]{3,})?)$",
    flags=re.I
))
async def iris_marriage_priority(message: Message):
    text=(message.text or "").strip()
    low=text.lower()

    # Accept / decline pending proposal.
    if low in ("брак да","брак нет"):
        pending=db.pending_chat_marriage_for_target(message.chat.id,message.from_user.id)
        if not pending:
            await message.answer("💍 У тебя нет ожидающего предложения брака.")
            return
        proposer_id=int(pending["proposer_id"])
        if db.active_chat_marriage(message.chat.id,message.from_user.id) or db.active_chat_marriage(message.chat.id,proposer_id):
            await message.answer("💔 Нельзя заключить второй брак. Пользователь изменяет.")
            return
        if low=="брак да":
            db.accept_chat_marriage(message.chat.id,pending["id"])
            await message.answer(
                f"💍 <b>БРАК ЗАРЕГИСТРИРОВАН!</b>\n"
                f"{_marriage_mention(pending['user1_id'],str(pending['user1_id']))} ❤️ "
                f"{_marriage_mention(pending['user2_id'],str(pending['user2_id']))}"
            )
        else:
            db.decline_chat_marriage(message.chat.id,pending["id"])
            await message.answer("❌ Предложение брака отклонено.")
        return

    target=await _resolve_marriage_target(message)
    if not target:
        await message.answer("💍 Ответь на сообщение пользователя или укажи @username.")
        return
    if int(target.id)==int(message.from_user.id):
        await message.answer("❌ На себе жениться нельзя.")
        return
    if db.active_chat_marriage(message.chat.id,message.from_user.id):
        await message.answer("💔 У тебя уже есть брак. Пользователь изменяет.")
        return
    if db.active_chat_marriage(message.chat.id,target.id):
        await message.answer("💔 Этот пользователь уже состоит в браке. Пользователь изменяет.")
        return
    if db.pending_chat_marriage_for_target(message.chat.id,target.id):
        await message.answer("💍 У этого пользователя уже есть ожидающее предложение.")
        return

    mid=db.create_chat_marriage(message.chat.id,message.from_user.id,target.id,message.from_user.id)
    kb=InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💍 Принять",callback_data=f"marry_yes_{mid}"),
        InlineKeyboardButton(text="❌ Отклонить",callback_data=f"marry_no_{mid}")
    ]])
    await message.answer(
        f"💍 <b>ПРЕДЛОЖЕНИЕ БРАКА</b>\n\n"
        f"{_marriage_mention(message.from_user.id,message.from_user.full_name)} "
        f"предлагает брак { _marriage_mention(target.id,target.full_name) } ❤️\n\n"
        f"Чтобы принять: <code>брак да</code>\n"
        f"Чтобы отклонить: <code>брак нет</code>",
        reply_markup=kb
    )

@dp.callback_query(F.data.startswith("marry_yes_") | F.data.startswith("marry_no_"))
async def iris_marriage_priority_callback(callback: CallbackQuery):
    try:
        mid=int(callback.data.rsplit("_",1)[1])
    except Exception:
        await callback.answer("Некорректное предложение.",show_alert=True); return
    row=db.marriage_by_id(mid)
    if not row or row["status"]!="pending":
        await callback.answer("Предложение уже недоступно.",show_alert=True); return
    if "chat_id" in row.keys() and row["chat_id"] is not None and int(row["chat_id"])!=int(callback.message.chat.id):
        await callback.answer("Предложение из другого чата.",show_alert=True); return
    if int(callback.from_user.id) not in (int(row["user1_id"]),int(row["user2_id"])) or int(callback.from_user.id)==int(row["proposer_id"]):
        await callback.answer("Принять предложение может только получатель.",show_alert=True); return
    if callback.data.startswith("marry_yes_"):
        if db.active_chat_marriage(callback.message.chat.id,row["user1_id"]) or db.active_chat_marriage(callback.message.chat.id,row["user2_id"]):
            await callback.answer("Нельзя заключить второй брак.",show_alert=True); return
        db.accept_chat_marriage(callback.message.chat.id,mid)
        await callback.message.edit_text(
            f"💍 <b>БРАК ЗАРЕГИСТРИРОВАН!</b> ❤️\n"
            f"{_marriage_mention(row['user1_id'],registered_display_name(row['user1_id']))} ❤️ "
            f"{_marriage_mention(row['user2_id'],registered_display_name(row['user2_id']))}"
        )
        await callback.answer("Брак заключён")
    else:
        db.decline_chat_marriage(callback.message.chat.id,mid)
        await callback.message.edit_text("❌ Предложение брака отклонено.")
        await callback.answer("Отклонено")

@dp.message(StateFilter(None), F.text.regexp(r"^(?:мой брак)$", flags=re.I))
async def iris_my_marriage_priority(message: Message):
    row=db.active_chat_marriage(message.chat.id,message.from_user.id)
    if not row:
        await message.answer("💍 У тебя нет активного брака.")
        return
    partner=row["user2_id"] if int(row["user1_id"])==int(message.from_user.id) else row["user1_id"]
    days=0
    try:
        days=max(0,(datetime.now()-datetime.fromisoformat(str(row["created_at"]))).days)
    except Exception:
        pass
    level=min(10,1+days//7)
    progress=min(100,(days%7)*100//7)
    await message.answer(
        f"💍 <b>МОЙ БРАК</b>\n\n"
        f"❤️ Партнёр: {_marriage_mention(partner,str(partner))}\n"
        f"📅 Вместе: {days} дн.\n"
        f"💗 Уровень отношений: <b>{level}</b>\n"
        f"📈 Прогресс: <b>{progress}%</b>"
    )

@dp.message(StateFilter(None), F.text.regexp(r"^(?:браки|список браков)$", flags=re.I))
async def iris_marriages_priority(message: Message):
    rows=db.list_chat_active_marriages(message.chat.id,limit=100,offset=0)
    if not rows:
        await message.answer("💍 В этом чате пока нет активных браков.")
        return
    out=["💍 <b>БРАКИ В ЧАТЕ</b>",""]
    for n,row in enumerate(rows,1):
        out.append(f"{n}. {_marriage_mention(row['user1_id'],registered_display_name(row['user1_id']))} ❤️ {_marriage_mention(row['user2_id'],registered_display_name(row['user2_id']))}")
    await message.answer("\n".join(out))

@dp.message(StateFilter(None), F.text.regexp(r"^(?:развод|!развод)$", flags=re.I))
async def iris_divorce_priority(message: Message):
    row=db.active_chat_marriage(message.chat.id,message.from_user.id)
    if not row:
        await message.answer("💔 Активного брака нет.")
        return
    db.divorce_chat_marriage(message.chat.id,message.from_user.id)
    await message.answer("💔 Брак расторгнут.")

@dp.message(StateFilter(None), F.text.regexp(r"^(?:статус отн|статус отношений)(?:\s+.*)?$", flags=re.I))
async def iris_relationship_priority(message: Message):
    target=await _resolve_marriage_target(message)
    if not target:
        await message.answer("❤️ Укажи @username или ответь на сообщение пользователя.")
        return
    row=db.active_chat_marriage_between(message.chat.id,message.from_user.id,target.id)
    if not row:
        await message.answer("🤍 Брак между вами не зарегистрирован.")
        return
    days=0
    try:
        days=max(0,(datetime.now()-datetime.fromisoformat(str(row["created_at"]))).days)
    except Exception:
        pass
    level=min(10,1+days//7)
    progress=min(100,(days%7)*100//7)
    await message.answer(f"❤️ Уровень отношений: <b>{level}</b>\n📈 Прогресс: <b>{progress}%</b>\n📅 Вместе: <b>{days} дн.</b>")


@dp.message(StateFilter(None), F.text.regexp(r"^(?!/).+$", flags=re.S))
async def receive_activity_or_alias(message: Message, state: FSMContext):
    text = message.text or ""
    text_lower = text.lower().strip()
    text_lower = text.lower().strip()
    # V7.1 longest-match no-slash commands. This runs before legacy activity parsing
    # so phrases such as "снять мут" and "предложить кв" are deterministic.
    try:
        current_state = await state.get_state()
    except Exception:
        current_state = None
    if not current_state and "V71_NO_SLASH" in globals():
        for phrase in sorted(V71_NO_SLASH, key=len, reverse=True):
            if text_lower == phrase or text_lower.startswith(phrase + " "):
                cmd = V71_NO_SLASH[phrase]
                original = text[len(phrase):].strip()
                if cmd == "kvproposal":
                    await state.set_state(KVProposalStates.guild)
                    await message.answer("⚔️ <b>Предложение КВ</b>\n\n1/5. Название вашей гильдии:")
                    return
                fn = V71_FN.get(cmd) if "V71_FN" in globals() else None
                if fn:
                    aliased = message.model_copy(update={"text": "/" + cmd + (" " + original if original else "")})
                    await fn(aliased)
                    return
    m_vaka = re.match(r"^(?:вака|vaka)[,:]?\s+(.+)$", text, re.IGNORECASE | re.DOTALL)
    if m_vaka:
        await answer_vaka_question(message, m_vaka.group(1))
        return

    # Команды без / с аргументами. Обрабатываются только при точном первом слове.
    first = text_lower.split(maxsplit=1)[0] if text_lower else ""
    text_command_map = {
        # Moderation / admin
        "пред": "warn", "варн": "warn", "предупреждение": "warn", "мут": "mute", "бан": "ban",
        "разбан": "unban", "размут": "unmute", "снять мут": "unmute", "снять варны": "unwarn", "кик": "kick", "преды": "warnings", "предупреждения": "warnings", "наказания": "warnings",
        "созыв": "summon", "созвать": "summon", "очистить чат": "clear", "очистить": "clear", "чистка чата": "clear", "итого": "total", "добавить": "adduser", "отвязать": "unbind",
        "удалить": "removeplayer", "обновить": "refresh", "логи": "logs", "администраторы": "admins", "админы": "admins",
        "админка": "panel", "панель": "panel", "права": "adminpanel", "роль": "setrole", "назначить": "setrole",
        "повысить": "promote", "понизить": "demote",
        # Guild / user commands
        "профиль": "profile", "стата": "stats", "статистика": "stats", "топ": "top", "рейтинг": "top",
        "активность": "activity", "история": "history", "неделя": "week", "участники": "users", "ники": "users",
        "правила": "rules", "команды": "help", "помощь": "help", "реферал": "ref", "рефералы": "ref",
        "коины": "coins", "монеты": "coins", "магазин": "shop", "регистрация": "register",
        # Utility commands
        "кто": "who", "ктоя": "whoami", "ид": "userid", "айди": "userid", "пинг": "ping", "чат": "chatinfo",
        "рандом": "random", "выбери": "choose", "данет": "yesno",
        "гость": "guest", "гостевая": "guest", "заявка": "apply", "вступить": "apply", "кв": "kv", "квшки": "kvs", "браки": "marry", "развестись": "divorce", "развод": "divorce",
        # English command names also work without /
        "promote": "promote", "demote": "demote", "kick": "kick", "ban": "ban", "mute": "mute", "warn": "warn",
        "unban": "unban", "unmute": "unmute", "unwarn": "unwarn", "admins": "admins", "addlist": "addlist", "panel": "panel", "profile": "profile",
        "stats": "stats", "top": "top", "users": "users", "rules": "rules", "help": "help", "ref": "ref",
        "coins": "coins", "shop": "shop", "ff": "ff", "register": "register", "summon": "summon",
        "activity": "activity", "history": "history", "week": "week", "publish": "publish", "refresh": "refresh",
        "logs": "logs", "role": "setrole", "setrole": "setrole", "adduser": "adduser", "unbind": "unbind",
        "removeplayer": "removeplayer", "whoami": "whoami", "who": "who", "userid": "userid", "ping": "ping",
        "chatinfo": "chatinfo", "random": "random", "choose": "choose", "yesno": "yesno"
    }
    if first in text_command_map:
        original = message.text or ""
        # aiogram 3 / Pydantic Message is frozen: never mutate message.text.
        # Make a lightweight copy with the slash command and dispatch the existing handler.
        fn = {
            "warn": command_warn, "mute": command_mute, "unmute": command_unmute, "unwarn": command_unwarn, "ban": command_ban, "unban": command_unban,
            "kick": command_kick, "warnings": command_warnings, "summon": command_summon,
            "total": command_total_activity, "clear": command_clear_chat, "adduser": command_adduser, "unbind": command_unbind,
            "removeplayer": command_removeplayer, "refresh": command_refresh, "logs": command_logs,
            "admins": command_admins, "setrole": command_setrole, "promote": command_promote,
            "demote": command_demote, "adminpanel": command_rank_adminpanel, "addlist": command_addlist,
            "help": command_help, "panel": command_panel, "profile": command_profile, "ff": command_ff,
            "register": command_register, "top": command_top,
            "stats": command_stats, "history": command_history, "users": command_users, "set": command_set_activity,
            "activity": command_activity, "week": command_week, "publish": command_publish, "rules": command_rules,
            "ref": command_ref, "coins": command_coins, "shop": command_shop, "shoprequests": command_shoprequests,
            "whoami": command_whoami, "who": command_who, "userid": command_user_id, "ping": command_ping,
            "chatinfo": command_chatinfo, "random": command_random, "choose": command_choose, "yesno": command_yesno,
            "greet": command_greet, "guest": command_guest, "apply": command_apply, "kv": command_kv, "kvs": command_kv, "marry": command_marry, "divorce": command_divorce
        }
        fn = fn.get(text_command_map[first])
        if fn:
            aliased = message.model_copy(update={"text": "/" + original})
            await fn(aliased)
            return

    if text_lower in TEXT_ALIASES:
        await handle_alias(message, TEXT_ALIASES[text_lower])
        return

    # Authoritative RP fallback: the generic text handler is registered early, so
    # it must delegate every legacy/new RP action (including 18+ entries) instead
    # of swallowing the message before the later unified RP handler can see it.
    try:
        registry = globals().get("ALL_RP_ACTIONS", {})
        aliases = globals().get("ALL_RP_ALIASES", {})
        for phrase in sorted(set(registry) | set(aliases), key=len, reverse=True):
            if text_lower == phrase or text_lower.startswith(phrase + " "):
                action = aliases.get(phrase, phrase)
                rest = text[len(phrase):].strip()
                target = await _rp_target(message, rest) if "_rp_target" in globals() else (message.reply_to_message.from_user if message.reply_to_message else None)
                await _run_rp(message, action, target)
                return
    except Exception:
        logger.exception("Unified RP fallback failed")

    uid = extract_uid(text)
    if uid and len(text.strip()) < 20 and text.strip() == uid:
        player = db.get_player(uid)
        if player:
            await send_player_profile(message.chat.id, uid)
        else:
            await message.answer(
                f"❌ Игрок с UID <code>{uid}</code> не зарегистрирован.\n\n"
                f"Для регистрации используй:\n<code>/register {uid}</code>"
            )
        return

    if activity_admin(message.from_user.id):
        if re.search(r"\b\d{8,12}\s+[+-=]?\d", text):
            processed = await process_manual_activity(message, text)
            if processed:
                return

        if looks_like_activity_data(text) and message.chat.id == GUILD_CHAT_ID:
            await handle_activity_import(message, text)


async def handle_activity_import(message: Message, text: str):
    if message.chat.id != GUILD_CHAT_ID:
        return
    if not activity_admin(message.from_user.id):
        return
    entries, errors = parse_activity_text(text)

    if not entries:
        await message.answer(
            "❌ Похоже, это статистика активности, но я не смог распознать игроков."
        )
        return

    start, end = get_selected_week(message.from_user.id)
    start_text = start.isoformat()

    if db.week_exists(start_text):
        await message.answer(
            "⚠️ Эта неделя уже сохранена.\n\n"
            "Для ручного изменения используй:\n"
            "<code>UID +ОЧКИ</code>"
        )
        return

    pending_uploads[message.from_user.id] = {
        "entries": entries,
        "week_start": start_text,
        "week_end": end.isoformat()
    }

    preview = build_preview(entries, start, end)

    if errors:
        preview += "\n\n⚠️ <b>ПРЕДУПРЕЖДЕНИЯ</b>\n\n"
        for error in errors[:10]:
            preview += f"• {html.escape(error)}\n"

    await message.answer(preview, reply_markup=confirm_keyboard())


# =========================================================
# CONFIRM / CANCEL
# =========================================================

@dp.callback_query(F.data == "activity_confirm")
async def activity_confirm(callback: CallbackQuery):
    if not activity_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return

    upload = pending_uploads.get(callback.from_user.id)
    if not upload:
        await callback.answer("Данные устарели.", show_alert=True)
        return

    try:
        db.save_week(
            upload["week_start"],
            upload["week_end"],
            upload["entries"]
        )
        previous_week = db.get_previous_week(upload["week_start"])
        if previous_week:
            for entry in upload["entries"]:
                prev = db.get_week_player(previous_week["week_start"], entry.player_id)
                prev_value = int(prev["activity"] or 0) if prev else 0
                current_value = int(entry.activity or 0)
                if prev_value >= 1000 and (current_value >= max(5000, prev_value * 4) or current_value - prev_value >= 10000):
                    reason = f"Резкий рост: {prev_value} → {current_value}"
                    db.add_anticheat_event(upload["week_start"], entry.player_id, prev_value, current_value, reason)
                    p = db.get_player(entry.player_id)
                    if OWNER_ID and p:
                        try:
                            await bot.send_message(OWNER_ID, f"🧠 <b>АНТИНАКРУТКА</b>\n\n👤 {html.escape(p['nick'])}\n📅 {format_date(upload['week_start'])}\n📉 Было: {format_number(prev_value)}\n📈 Стало: {format_number(current_value)}\n⚠️ {html.escape(reason)}")
                        except Exception:
                            logger.exception("Не удалось отправить anti-cheat уведомление")
        db.log("activity_import", callback.from_user.id, {
            "week_start": upload["week_start"],
            "week_end": upload["week_end"],
            "players": len(upload["entries"]),
            "total_activity": total_activity(upload["entries"]),
        })
    except Exception as e:
        logger.exception("Ошибка сохранения.")
        await callback.answer(str(e), show_alert=True)
        return

    total = total_activity(upload["entries"])
    low_count = sum(1 for entry in upload["entries"] if entry.activity < LOW_ACTIVITY_LIMIT)

    del pending_uploads[callback.from_user.id]

    await safe_edit(callback.message, 
        "✅ <b>СТАТИСТИКА СОХРАНЕНА</b>\n\n"
        f"📅 {week_label(upload['week_start'], upload['week_end'])}\n\n"
        f"👥 Игроков: <b>{len(upload['entries'])}</b>\n"
        f"🔥 Активность: <b>{format_number(total)}</b>\n"
        f"🔴 Низкая активность: <b>{low_count}</b>"
    )

    await callback.answer("Сохранено!")


@dp.callback_query(F.data == "activity_cancel")
async def activity_cancel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return

    pending_uploads.pop(callback.from_user.id, None)

    await safe_edit(callback.message, 
        "❌ <b>ЗАГРУЗКА ОТМЕНЕНА</b>\n\nДанные не сохранены."
    )

    await callback.answer("Отменено.")


# =========================================================
# LIFETIME ACTIVITY COMMAND
# =========================================================

@dp.message(Command("total"))
async def command_total_activity(message: Message, state: FSMContext):
    if not activity_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer(
            "📈 <b>ОБЩИЕ ОЧКИ ЗА ВСЁ ВРЕМЯ</b>\n\n"
            "Формат:\n"
            "<code>/total UID +100</code> — добавить\n"
            "<code>/total UID -50</code> — вычесть\n"
            "<code>/total UID =3000</code> — установить\n\n"
            "Это меняет только общий накопленный показатель и не меняет недельную активность."
        )
        return

    uid, operation = parts[1], parts[2]
    if not uid.isdigit() or not re.fullmatch(r"[+\-=]?\d+", operation):
        await message.answer("❌ Формат: <code>/total UID +100</code>, <code>/total UID -50</code> или <code>/total UID =3000</code>")
        return
    player = db.get_player(uid)
    if not player:
        await message.answer("❌ Игрок с таким UID не найден.")
        return

    current = int(player["total_activity"] or 0)
    if operation.startswith("="):
        mode, value = "set", int(operation[1:])
        new_value = value
    elif operation.startswith("-"):
        mode, value = "sub", int(operation[1:])
        new_value = current - value
    else:
        mode, value = "add", int(operation.lstrip("+"))
        new_value = current + value
    if new_value < 0:
        await message.answer("❌ Общее количество очков не может быть отрицательным.")
        return

    pending_lifetime_activity[message.from_user.id] = {
        "player_id": uid, "mode": mode, "old": current, "value": value, "new": new_value
    }
    await state.clear()
    await message.answer(
        "📋 <b>ПОДТВЕРЖДЕНИЕ</b>\n\n"
        f"👤 {html.escape(player['nick'])}\n"
        f"🔥 Было: <b>{format_number(current)}</b>\n"
        f"🔥 Новое: <b>{format_number(new_value)}</b>\n"
        f"📊 Изменение: <b>{'+' if new_value-current >= 0 else ''}{format_number(new_value-current)}</b>\n\n"
        "Недельная статистика не изменится.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="lifetime_confirm"), InlineKeyboardButton(text="❌ Отмена", callback_data="lifetime_cancel")]
        ])
    )



# =========================================================
# RANK / ADMIN MANAGEMENT
# =========================================================

def _role_target_from_message(message: Message, parts: list[str]):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id
    if len(parts) >= 2:
        raw = parts[1].lstrip("@")
        if raw.isdigit():
            return int(raw)
        row = db.get_player_by_telegram_username(raw)
        if row and row["telegram_id"]:
            return int(row["telegram_id"])
    return None


async def _set_target_role(message: Message, target_id: int | None, new_level: int, action: str):
    actor = int(message.from_user.id)
    actor_rank = get_admin_rank(actor)
    if actor_rank < 7:
        await message.answer("❌ Управлять ролями могут только 👑 Лидер и ⭐ Заместитель.")
        return
    if target_id is None or not 1 <= int(new_level) <= 8:
        await message.answer(
            "📋 Формат: <code>/promote @user 3</code> или <code>/demote 123456789 2</code>\n\n"
            "8 👑 Лидер\n7 ⭐ Заместитель\n6 🛡 Админ чата\n5 🔨 Глав Проверяюший\n"
            "4 🔍 Проверяющий\n3 Помощник\n2 ⚡ ССМШИК\n1 👤 Участник\n0 🚫 Не зарегистрированный"
        )
        return
    new_level = int(new_level)
    if target_id == actor:
        await message.answer("❌ Нельзя менять собственную роль.")
        return
    old_level = get_admin_rank(target_id)
    if old_level >= actor_rank:
        await message.answer("❌ Нельзя управлять пользователем с равной или более высокой ролью.")
        return
    if new_level >= actor_rank:
        await message.answer("❌ Нельзя назначить роль равную или выше своей.")
        return
    # Only the leader can create/change another leader.
    if new_level == 8:
        await message.answer("❌ Передать роль 👑 Лидера может только действующий лидер.")
        return
    target_player = db.get_player_by_telegram(target_id)
    target_name = target_player["nick"] if target_player else str(target_id)
    db.set_admin_role(target_id, new_level)
    db.log(action, actor, {
        "target_id": target_id,
        "target_nick": target_name,
        "old_level": old_level,
        "new_level": new_level,
        "old_role": rank_name(old_level),
        "new_role": rank_name(new_level),
    })
    await message.answer(
        f"✅ Роль изменена.\n\n👤 <b>{html.escape(target_name)}</b>\n"
        f"Было: {rank_name(old_level)}\nСтало: {rank_name(new_level)}"
    )


@dp.message(Command("promote"))
async def command_promote(message: Message):
    parts=(message.text or "").split()
    target=_role_target_from_message(message,parts)
    level=int(parts[-1]) if parts and parts[-1].isdigit() else None
    await _set_target_role(message,target,level,"promote") if level is not None else await _set_target_role(message,target,0,"promote")


@dp.message(Command("demote"))
async def command_demote(message: Message):
    parts=(message.text or "").split()
    target=_role_target_from_message(message,parts)
    level=int(parts[-1]) if parts and parts[-1].isdigit() else None
    await _set_target_role(message,target,level,"demote") if level is not None else await _set_target_role(message,target,0,"demote")


@dp.message(Command("role", "setrole"))
async def command_setrole(message: Message):
    parts=(message.text or "").split()
    target=_role_target_from_message(message,parts)
    level=int(parts[-1]) if parts and parts[-1].isdigit() else None
    await _set_target_role(message,target,level,"set_role") if level is not None else await _set_target_role(message,target,0,"set_role")


@dp.message(Command("admins", "administrators", "whoadmins"))
async def command_admins(message: Message):
    rows = db.get_admin_roles()
    lines = ["👑 <b>АДМИНИСТРАЦИЯ ГИЛЬДИИ</b>", ""]
    found = False
    for row in rows:
        level = int(row["role_level"])
        if level < 2:
            continue
        found = True
        uid = int(row["telegram_id"])
        player = db.get_player_by_telegram(uid)
        nick = player["nick"] if player else "Не зарегистрирован"
        username = f" @{player['telegram_username']}" if player and player["telegram_username"] else ""
        lines.append(f"{rank_name(level)}: <b>{html.escape(nick)}</b>{html.escape(username)}")
    if not found:
        lines.append("Администрация пока не назначена.")
    await message.answer("\n".join(lines))


@dp.message(Command("adminpanel"))
async def command_rank_adminpanel(message: Message):
    r = get_admin_rank(message.from_user.id)
    if not management_admin(message.from_user.id):
        await message.answer("👤 У тебя нет административных прав.")
        return
    lines = [
        "🛡 <b>АДМИН-ПАНЕЛЬ</b>",
        f"Твой ранг: <b>{rank_name(r)}</b>",
        "",
        "📊 Профиль / статистика / топ",
    ]
    if r >= 3:
        lines.append("🔎 Проверка / участники / созыв")
    if r >= 5:
        lines.append("🔥 Активность гильдии / модерация")
    if r >= 6:
        lines.append("👑 Управление рангами")
    await message.answer("\n".join(lines))


# =========================================================
# ADMIN COMMANDS
# =========================================================



@dp.message(Command("addlist"))
async def command_addlist(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет административных прав."); return
    raw_lines=(message.text or "").splitlines()[1:]
    if not raw_lines:
        await message.answer("❌ Формат:\n<code>/addlist</code>\n<code>TELEGRAM_ID UID NICK</code>"); return
    added=updated=0
    errors=[]
    seen_tg=set()
    seen_uid=set()
    for line_no,line in enumerate(raw_lines,1):
        if not line.strip(): continue
        try:
            parts=line.split(maxsplit=2)
            if len(parts)!=3: raise ValueError("нужно 3 поля: Telegram_ID UID Nick")
            tg_id=int(parts[0]); uid=parts[1].strip(); nick=parts[2].strip()
            if tg_id <= 0: raise ValueError("неверный Telegram ID")
            if not uid.isdigit(): raise ValueError("UID должен быть числом")
            if not nick: raise ValueError("пустой Nick")
            if tg_id in seen_tg: raise ValueError("дублирующийся Telegram ID в списке")
            if uid in seen_uid: raise ValueError("дублирующийся UID в списке")
            seen_tg.add(tg_id); seen_uid.add(uid)
            existing_uid=db.get_player(uid)
            existing_tg=db.get_player_by_telegram(tg_id)
            if existing_tg and existing_tg["player_id"] != uid:
                raise ValueError(f"Telegram ID уже привязан к UID {existing_tg['player_id']}")
            db.add_or_update_player(uid,nick,tg_id,None)
            if existing_uid:
                updated += 1
                db.log("player_update",message.from_user.id,{"player_id":uid,"nick":nick,"telegram_id":tg_id})
            else:
                added += 1
                db.log("player_add",message.from_user.id,{"player_id":uid,"nick":nick,"telegram_id":tg_id})
        except Exception as e:
            errors.append(f"строка {line_no}: {e}")
    text=f"✅ <b>Массовое добавление завершено</b>\n\n➕ Новых: {added}\n🔄 Обновлено: {updated}\n❌ Ошибок: {len(errors)}"
    if errors: text+="\n\n"+"\\n".join(html.escape(x) for x in errors[:10])
    await message.answer(text)

@dp.message(Command("adduser"))
async def command_adduser(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) != 4:
        await message.answer(
            "❌ Формат:\n\n"
            "<code>/adduser TELEGRAM_ID UID NICK</code>"
        )
        return

    try:
        telegram_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Telegram ID должен быть числом.")
        return

    uid = parts[2]
    nick = parts[3].strip()

    if not uid.isdigit():
        await message.answer("❌ UID должен состоять из цифр.")
        return

    db.add_or_update_player(uid, nick, telegram_id, None)

    await message.answer(
        f"✅ <b>УЧАСТНИК СОХРАНЁН</b>\n\n"
        f"👤 Ник: <b>{html.escape(nick)}</b>\n"
        f"🎮 UID: <code>{uid}</code>\n"
        f"📱 Telegram: <code>{telegram_id}</code>"
    )


@dp.message(Command("unbind"))
async def command_unbind(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование:\n<code>/unbind UID</code>")
        return

    player = db.get_player(parts[1])
    if not player:
        await message.answer("❌ Участник не найден.")
        return

    db.unbind_player(parts[1])

    await message.answer(
        f"🔓 Telegram-привязка удалена.\n\n"
        f"🎮 UID: <code>{parts[1]}</code>\n"
        f"👤 Ник: <b>{html.escape(player['nick'])}</b>"
    )



@dp.message(Command("removeplayer"))
async def command_removeplayer(message:Message):
    if not is_admin(message.from_user.id): return
    parts=(message.text or "").split()
    if len(parts)!=2: await message.answer("Формат: <code>/removeplayer UID</code>"); return
    p=db.get_player(parts[1])
    if not p: await message.answer("❌ Игрок не найден."); return
    db.delete_player(parts[1]); db.log("player_deleted",message.from_user.id,json.dumps({"uid":parts[1]},ensure_ascii=False)); await message.answer(f"✅ Игрок <b>{html.escape(p['nick'])}</b> удалён. История сохранена.")

@dp.message(Command("refresh"))
async def command_refresh(message:Message):
    if not is_admin(message.from_user.id): return
    players=db.get_all_players(); ok=0; errors=0
    status=await message.answer(f"⏳ Обновление: 0/{len(players)}")
    for i,p in enumerate(players,1):
        try:
            profile=await ff_client.get_player_profile(p["player_id"],FF_REGION)
            if not profile: raise ValueError("API не вернул профиль")
            if FF_GUILD_ID and str(profile.guild_id or "")!=str(FF_GUILD_ID): raise ValueError("Игрок не в нужной гильдии")
            db.update_player_profile(p["player_id"],profile.raw_data); ok+=1
        except Exception as e: errors+=1; db.log("api_error",message.from_user.id,json.dumps({"uid":p["player_id"],"error":str(e)},ensure_ascii=False))
        if i%5==0 or i==len(players):
            try: await status.edit_text(f"⏳ Обновление: {i}/{len(players)}\\n✅ {ok} | ❌ {errors}")
            except Exception: pass
    db.log("profiles_refreshed",message.from_user.id,json.dumps({"ok":ok,"errors":errors},ensure_ascii=False)); await status.edit_text(f"✅ Обновление завершено.\\n\\n✅ {ok} | ❌ {errors}")

@dp.message(Command("logs"))
async def command_logs(message:Message):
    if not is_admin(message.from_user.id): return
    await render_logs(message,0)

# =========================================================
# OWNER BACKUP CALLBACKS
# =========================================================

async def send_backup_file(message: Message, path: Path, caption: str, private_owner: bool = False):
    if not path.exists():
        await message.answer("❌ Файл резервной копии не найден.")
        return
    from aiogram.types import FSInputFile
    if private_owner and OWNER_ID:
        await bot.send_document(chat_id=OWNER_ID, document=FSInputFile(str(path)), caption=caption)
    else:
        await message.answer_document(FSInputFile(str(path)), caption=caption)

async def send_backup_to_owner(path: Path, caption: str):
    if not OWNER_ID or not path.exists():
        return
    from aiogram.types import FSInputFile
    await bot.send_document(chat_id=OWNER_ID, document=FSInputFile(str(path)), caption=caption)

async def automatic_backup_loop():
    """Create local recovery copies and periodically upload a DB snapshot to Telegram.
    Telegram is used as off-host backup storage; the live SQLite DB remains local because
    SQLite cannot safely operate as a database directly inside Telegram/Drive.
    """
    last_remote = None
    last_daily = None
    last_weekly = None
    tz = ZoneInfo(TIMEZONE)
    while True:
        try:
            now = datetime.now(tz)
            if BACKUP_INTERVAL_HOURS > 0 and (last_remote is None or (now - last_remote).total_seconds() >= BACKUP_INTERVAL_HOURS * 3600):
                path = create_backup()
                await send_backup_to_owner(path, f"☁️ <b>Off-host backup V7.1</b>\n<code>{html.escape(path.name)}</code>")
                last_remote = now
            if now.hour == 3 and now.minute == 0:
                key = now.date().isoformat()
                if last_daily != key:
                    path = create_backup()
                    await send_backup_to_owner(path, f"🗄 <b>Ежедневный бэкап V7.1</b>\n<code>{html.escape(path.name)}</code>")
                    last_daily = key
            if now.weekday() == 6 and now.hour == 3 and now.minute == 30:
                key = now.date().isoformat()
                if last_weekly != key:
                    path = create_backup()
                    await send_backup_to_owner(path, f"📦 <b>Еженедельный бэкап V7.1</b>\n<code>{html.escape(path.name)}</code>")
                    last_weekly = key
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка автоматического бэкапа")
        await asyncio.sleep(30)


@dp.callback_query(F.data == "owner_backups")
async def callback_owner_backups(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    files = backup_files()
    latest = files[0].name if files else "нет копий"
    await safe_edit(callback.message, 
        "💾 <b>РЕЗЕРВНЫЕ КОПИИ БАЗЫ</b>\n\n"
        f"📦 Копий: <b>{len(files)}</b>\n"
        f"🕒 Последняя: <code>{html.escape(latest)}</code>\n\n"
        "Копия создаётся из работающей SQLite БД без остановки бота.",
        reply_markup=backup_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == "owner_backup_create")
async def callback_owner_backup_create(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    try:
        path = create_backup()
        db.log("backup_created", callback.from_user.id, json.dumps({"file": path.name}, ensure_ascii=False))
        await send_backup_file(
            callback.message, path,
            f"💾 Бэкап базы создан: <code>{html.escape(path.name)}</code>",
            private_owner=True,
        )
        await callback.answer("Бэкап создан")
    except Exception as exc:
        logger.exception("Ошибка создания бэкапа")
        await callback.answer("Ошибка создания бэкапа", show_alert=True)
        await callback.message.answer(f"❌ Не удалось создать бэкап: <code>{html.escape(str(exc))}</code>")


@dp.callback_query(F.data == "owner_backup_latest")
async def callback_owner_backup_latest(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    files = backup_files()
    if not files:
        await callback.answer("Копий пока нет.", show_alert=True)
        return
    await send_backup_file(
        callback.message, files[0],
        f"📦 Последний бэкап: <code>{html.escape(files[0].name)}</code>",
        private_owner=True,
    )
    await callback.answer()


@dp.callback_query(F.data == "owner_backup_list")
async def callback_owner_backup_list(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    files = backup_files()
    if not files:
        text = "📚 <b>КОПИИ</b>\n\nНет сохранённых копий."
    else:
        lines = ["📚 <b>КОПИИ</b>", ""]
        for i, p in enumerate(files[:15], 1):
            size_mb = p.stat().st_size / 1024 / 1024
            lines.append(f"{i}. <code>{html.escape(p.name)}</code> — {size_mb:.2f} MB")
        text = "\n".join(lines)
    await safe_edit(callback.message, text, reply_markup=backup_keyboard())
    await callback.answer()


def backup_restore_keyboard(files):
    rows = [
        [InlineKeyboardButton(
            text=f"♻️ {i+1}. {path.name}",
            callback_data=f"owner_backup_restore_{i}",
        )]
        for i, path in enumerate(files[:10])
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="owner_backups")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "owner_backup_restore_list")
async def callback_owner_backup_restore_list(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    files = backup_files()
    if not files:
        await callback.answer("Копий пока нет.", show_alert=True)
        return
    await safe_edit(callback.message, 
        "♻️ <b>ВОССТАНОВЛЕНИЕ</b>\n\n"
        "Перед заменой текущей базы бот автоматически создаст аварийную копию.",
        reply_markup=backup_restore_keyboard(files),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("owner_backup_restore_"))
async def callback_owner_backup_restore_confirm(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    try:
        index = int(callback.data.rsplit("_", 1)[1])
        files = backup_files()
        source = files[index]
    except (ValueError, IndexError):
        await callback.answer("Копия не найдена.", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="⚠️ ДА, ВОССТАНОВИТЬ",
            callback_data=f"owner_backup_confirm_{index}",
        )],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="owner_backups")],
    ])
    await safe_edit(callback.message, 
        "⚠️ <b>ПОДТВЕРЖДЕНИЕ</b>\n\n"
        f"Восстановить: <code>{html.escape(source.name)}</code>?\n\n"
        "Текущая база будет сохранена автоматически перед заменой.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("owner_backup_confirm_"))
async def callback_owner_backup_restore(callback: CallbackQuery):
    if not owner_only(callback.from_user.id):
        await callback.answer("🔒 Только владелец.", show_alert=True)
        return
    try:
        index = int(callback.data.rsplit("_", 1)[1])
        files = backup_files()
        source = files[index]
        emergency = create_backup()
        db.restore_from(str(source))
        db.set_admin_role(OWNER_ID, 8)
        db.log(
            "backup_restored",
            callback.from_user.id,
            json.dumps({"source": source.name, "emergency_backup": emergency.name}, ensure_ascii=False),
        )
        await safe_edit(callback.message, 
            "✅ <b>БАЗА ВОССТАНОВЛЕНА</b>\n\n"
            f"📦 Источник: <code>{html.escape(source.name)}</code>\n"
            f"🛡 Аварийная копия: <code>{html.escape(emergency.name)}</code>\n\n"
            "Владелец автоматически восстановлен.",
        )
        await callback.answer("Восстановлено")
    except Exception as exc:
        logger.exception("Ошибка восстановления базы")
        await callback.answer("Ошибка восстановления", show_alert=True)
        await callback.message.answer(
            f"❌ Восстановление не выполнено: <code>{html.escape(str(exc))}</code>"
        )



# =========================================================
# GUEST / KV / MARRIAGE / RP MODULE
# =========================================================
GUILD_INFO = "🏰 <b>ШЛЮХ НАДЗОР</b>\n\n🎮 Гильдия Free Fire\n🤖 Бот: @Nadzo69rBot\n💬 Чат: @nadzor67\n📰 Новости: @ndzorsh"
RULES_TEXT = """🚨 <b>ПРАВИЛА «ШЛЮХ НАДЗОР»</b>\n\n1. 🤝 Уважение\nУважительно относимся к участникам. Шутки разрешены, но если человек просит остановиться — останавливаемся.\n\n2. 🚫 Никакой политики и национальной/религиозной вражды\nПолитические срачи и вражда → предупреждение / кик.\n\n3. 🎤 Будь частью команды\nПо возможности играем с микрофоном и общаемся.\n\n4. 🎮 Участие в жизни гильдии\nКВ, тренировки, совместные игры и мероприятия — желательно участвовать.\n\n5. 💤 AFK\nУходишь надолго — предупреди руководство.\n\n6. 💎 Награды\nПопрошайничество запрещено.\n\n7. 👑 Руководство\nРешения по составу и организации принимает руководство.\n\n8. 🧠 Адекватность\nЕсли от тебя постоянно проблемы — место в составе не гарантируется.\n\n9. 🕵️ Никакого намеренного саботажа\nНе мешаем КВ, тренировкам и другим участникам специально.\n\n10. 🔥 Главное правило\nНе будь просто цифрой в составе. Будь частью «Шлюх надзор».\n\n⚠️ За нарушение:\n1 предупреждение → ограничение → исключение. Тяжёлые нарушения могут привести к мгновенному исключению.\n\n⏳ Варны действуют 7 дней. 5 активных варнов → запрет писать в чате."""

KV_RULES_TEXT = """⚔️ <b>ПРАВИЛА КВ</b>

<b>Настройка комнаты</b>
• 13 раундов
• Без ограничений патронов
• Без ограничений стенок
• Только в голову
• Монеты — максимум
• Комната создаётся за 5 минут до начала
• Неявка за 10 минут → КВ не состоялось
• Игра 4 на 4

<b>Дополнительно</b>
• Первая игра — с теми, кто предложил КВ
• Вторая игра — с принявшими
• Третья игра — с теми, кто проиграл в первой
• Играем по правилам гильдии, которая открыла комнату
• 2:0 — конец КВ
• 1:1 — третья игра
• Пинг, AFK или другие проблемы — комнату не перезапускаем
• При неправильных настройках комнаты и запуске — комната создаётся заново

<b>Персонажи</b>
Активные: Алок, Тацуя, Кода
Пассивные: все, кроме Волчара и Тивы

<b>Оружие</b>
M1887, UMP, Desert Eagle

<b>Запрещено</b>
• ПК против телефонщиков
• Телефонщики против ПК
• Закрывать стенкой за зоной
• Наблюдатели только по согласию

<b>Наказания</b>
• Техническое поражение (ТП)
• Чёрный список для КВ
• Без доказательств ТП не засчитывается

<b>Реванши</b>
• Через 7 дней — бесплатно
• Сразу — 300 ₽
• 1 на 1 с человеком из состава — 100 ₽"""

RULES_GUILD_IMAGE = Path(__file__).resolve().parent / "assets" / "rules_guild.jpg"
RULES_KV_IMAGE = Path(__file__).resolve().parent / "assets" / "rules_kv.jpg"

def guest_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="vaka_flow_cancel")]])

async def send_rules_bundle(message: Message, kind: str, reply_markup=None):
    if kind == "kv":
        image = RULES_KV_IMAGE
        text = KV_RULES_TEXT
    else:
        image = RULES_GUILD_IMAGE
        text = RULES_TEXT_V71 if "RULES_TEXT_V71" in globals() else RULES_TEXT
    try:
        if image.exists():
            title = "📜 ПРАВИЛА ГИЛЬДИИ" if kind == "guild" else "⚔️ ПРАВИЛА КВ"
            await message.answer_photo(FSInputFile(str(image)), caption=title)
        await message.answer(text, reply_markup=reply_markup)
    except Exception:
        logger.exception("Не удалось отправить правила %s", kind)
        await message.answer(text, reply_markup=reply_markup)

def guest_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ℹ️ О гильдии",callback_data="guest_info"),InlineKeyboardButton(text="📜 Правила",callback_data="guest_rules")],
        [InlineKeyboardButton(text="📝 Регистрация",callback_data="guest_register"),InlineKeyboardButton(text="📩 Вступление",callback_data="guest_apply")],
        [InlineKeyboardButton(text="⚔️ Предложить КВ",callback_data="guest_kv"),InlineKeyboardButton(text="📱 Соцсети",callback_data="guest_social")]
    ])

def social_text(): return "📱 <b>НАШИ РЕСУРСЫ</b>\n\n🤖 Бот: @Nadzo69rBot\n💬 Чат: @nadzor67\n👑 Администрация: @Vavix @overside1 @swswswqqqq\n📰 Новости: @ndzorsh\n🎵 TikTok: https://www.tiktok.com/@nadzor_sh"

@dp.callback_query(F.data.in_({"guest_info","guest_rules","guest_social","guest_register","guest_apply","guest_kv"}))
async def guest_callbacks(callback: CallbackQuery, state: FSMContext):
    key=callback.data
    if key=="guest_info": text=GUILD_INFO
    elif key=="guest_rules": text=RULES_TEXT
    elif key=="guest_social": text=social_text()
    elif key=="guest_register": text="📝 <b>Регистрация</b>\n\nОтправь UID командой <code>регистрация 123456789</code>. Бот проверит принадлежность к гильдии."
    elif key=="guest_apply": text="📩 <b>Заявка</b>\n\nОтправь <code>заявка UID</code>."
    else: text="⚔️ <b>Предложить КВ</b>\n\nОтправь: <code>кв предложение</code> и бот проведёт тебя по шагам."
    await safe_edit(callback.message,text,reply_markup=guest_keyboard()); await callback.answer()

@dp.message(Command("legacy_guest"))
async def command_guest(message: Message):
    if db.get_player_by_telegram(message.from_user.id):
        await message.answer("Ты уже зарегистрирован.",reply_markup=panel_keyboard(message.from_user.id)); return
    await message.answer(GUILD_INFO+"\n\nВыбери действие:",reply_markup=guest_keyboard())

@dp.message(Command("legacy_apply"))
async def command_apply(message: Message):
    if db.get_player_by_telegram(message.from_user.id): await message.answer("Ты уже участник."); return
    parts=(message.text or "").split(maxsplit=1); uid=extract_uid(parts[1]) if len(parts)>1 else ""
    if not uid: await message.answer("❌ Укажи UID: <code>заявка 123456789</code>"); return
    try:
        data=await ff_client.get_player(uid)
        nick=(data or {}).get("nickname") or (data or {}).get("nick") or "Неизвестно"
    except Exception: nick="Не проверен"
    app=db.create_application(message.from_user.id,message.from_user.username,uid,nick)
    for admin in ADMIN_IDS:
        try: await bot.send_message(admin,f"📩 <b>Новая заявка #{app}</b>\n👤 {html.escape(message.from_user.full_name)}\n🆔 {uid}\n🎮 {html.escape(nick)}")
        except Exception: pass
    await message.answer(f"✅ Заявка #{app} отправлена администрации.")

@dp.message(Command("legacy_kv"))
async def command_kv(message: Message):
    if (message.text or "").lower().startswith(("/kv предложение","/kv offer")) or (message.text or "").lower().strip() in ("/kv предложение","/kv offer"):
        await message.answer("⚔️ Для предложения КВ используй: <code>предложить кв | Гильдия | дата | время | назначение | игрок1, игрок2, игрок3, игрок4</code>")
        return
    rows=db.get_kvs(limit=3)
    if not rows: await message.answer("⚔️ Активных КВ нет."); return
    out=["⚔️ <b>СОСТОЯНИЕ КВ</b>"]
    for r in rows:
        enemy=json.loads(r['enemy_members'] or '[]'); ours=json.loads(r['our_members'] or '[]')
        out.append(f"\n#{r['id']} — <b>{html.escape(r['title'])}</b>\n🕐 {r['match_date']} {r['match_time']}\n🎯 {html.escape(r['purpose'] or '—')}\n🏰 VS <b>{html.escape(r['enemy_guild'])}</b>\n👥 Наши: {', '.join(map(html.escape,ours[:4])) or '—'}\n👥 Противник: {', '.join(map(html.escape,enemy[:4])) or '—'}")
    await message.answer("\n".join(out))

@dp.message(Command("legacy_marry"))
async def command_marry(message: Message):
    target=message.reply_to_message.from_user if message.reply_to_message else None
    if not target:
        m=re.search(r"@([A-Za-z0-9_]{3,})",message.text or "")
        if m:
            try: target=(await bot.get_chat_member(message.chat.id,"@"+m.group(1))).user
            except Exception: target=None
    if not target: await message.answer("💍 Ответь на сообщение пользователя или укажи @username."); return
    if target.id==message.from_user.id: await message.answer("❌ На себе жениться нельзя."); return
    if db.active_marriage(message.from_user.id) or db.active_marriage(target.id): await message.answer("❌ Один из пользователей уже состоит в браке."); return
    mid=db.create_marriage(message.from_user.id,target.id,message.from_user.id)
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💍 Принять",callback_data=f"marry_yes_{mid}"),InlineKeyboardButton(text="❌ Отказать",callback_data=f"marry_no_{mid}")]])
    await message.answer(f"💍 {mention_user(message.from_user.id,message.from_user.full_name)} предлагает брак {mention_user(target.id,target.full_name)}",reply_markup=kb)

@dp.callback_query(F.data.startswith("marry_yes_") | F.data.startswith("marry_no_"))
async def marriage_callback(callback: CallbackQuery):
    mid=int(callback.data.rsplit('_',1)[1]); row=db.conn.execute("SELECT * FROM marriages WHERE id=?",(mid,)).fetchone()
    if not row or row['status']!='pending' or callback.from_user.id not in (row['user1_id'],row['user2_id']) or callback.from_user.id==row['proposer_id']:
        await callback.answer("Это предложение недоступно.",show_alert=True); return
    if callback.data.startswith("marry_yes_"): db.accept_marriage(mid); await callback.message.edit_text("💍 <b>БРАК ЗАРЕГИСТРИРОВАН</b> ❤️"); await callback.answer("Согласие принято")
    else: db.conn.execute("UPDATE marriages SET status='declined' WHERE id=?",(mid,)); db.conn.commit(); await callback.message.edit_text("❌ Предложение отклонено."); await callback.answer("Отклонено")

@dp.message(Command("divorce","развод"))
async def command_divorce(message: Message):
    if not db.active_marriage(message.from_user.id): await message.answer("💔 Активного брака нет."); return
    db.divorce(message.from_user.id); await message.answer("💔 Брак расторгнут.")



# =========================================================
# V7.1 FEATURE COMPLETION PACK
# =========================================================

# Feature-specific FSM states. They are intentionally separate from the
# legacy states so existing flows remain untouched.
class KVProposalStates(StatesGroup):
    guild = State()
    enemy_members = State()
    our_members = State()
    match_date = State()
    match_time = State()
    purpose = State()
    confirm = State()

class KVRosterStates(StatesGroup):
    slot = State()
    members = State()

class KVCreateStates(StatesGroup):
    title = State()
    enemy_guild = State()
    enemy_members = State()
    our_members = State()
    match_date = State()
    match_time = State()
    purpose = State()

class ApplicationReviewStates(StatesGroup):
    pass

RULES_BY_ID = {
    1: "🤝 Уважение — оскорбления/травля после просьбы остановиться",
    2: "🚫 Никакой политики и национальной/религиозной вражды",
    3: "🎤 Будь частью команды — систематическое игнорирование командного взаимодействия",
    4: "🎮 Участие в жизни гильдии — систематический отказ от жизни состава",
    5: "💤 AFK — длительное отсутствие без предупреждения",
    6: "💎 Награды — попрошайничество",
    7: "👑 Руководство — конфликтное/деструктивное оспаривание решений",
    8: "🧠 Адекватность — систематическое создание проблем",
    9: "🕵️ Саботаж — намеренное вмешательство в КВ/тренировки",
    10: "🔥 Главное правило — не быть источником постоянных проблем в составе",
}

# Broad, non-explicit RP action vocabulary. Explicit sexual acts are deliberately
# excluded; the action engine itself is extensible and stores every invocation.
RP_ACTIONS_V71 = RP_ACTIONS.copy()
RP_ALIASES_V71 = RP_ALIASES.copy()

GUILD_NAME_V71 = "ɯᴧюх нᴀдзоᴩ"

GUILD_INFO_V71 = (
    "🏰 <b>ШЛЮХ НАДЗОР</b>\n\n"
    "🎮 Free Fire гильдия\n"
    "🤖 Бот: @Nadzo69rBot\n"
    "💬 Чат: @nadzor67\n"
    "👑 Администрация: @Vavix @overside1 @swswswqqqq\n"
    "📰 Новости: @ndzorsh\n"
    "🎵 TikTok: @nadzor_sh"
)
RULES_TEXT_V71 = """🚨 <b>ПРАВИЛА «ШЛЮХ НАДЗОР»</b>

1. 🤝 <b>Уважение</b>
Уважительно относимся к участникам. Шутки разрешены, но если человек просит остановиться — останавливаемся.

2. 🚫 <b>Никакой политики и национальной/религиозной вражды</b>
Мы здесь играть, а не выяснять, кто лучше. Оскорбления по национальности, религии или политические срачи → предупреждение / кик в зависимости от ситуации.

3. 🎤 <b>Будь частью команды</b>
По возможности играем с микрофоном и общаемся. Постоянно сидеть молча и никогда не играть с другими участниками — не наша философия.

4. 🎮 <b>Участие в жизни гильдии</b>
КВ, тренировки, совместные игры, мероприятия и розыгрыши — желательно участвовать.

5. 💤 <b>AFK</b>
Уходишь надолго — предупреди руководство. Если человек пропал без предупреждения, руководство может убрать его из состава.

6. 💎 <b>Награды</b>
Награды получают участники, которые выполнили условия активности. Попрошайничество запрещено.

7. 👑 <b>Руководство</b>
Решения по составу и организации гильдии принимает руководство. Если не согласен — спокойно объясни свою позицию, а не устраивай конфликт.

8. 🧠 <b>Адекватность</b>
Неважно, насколько хорошо ты играешь. Если от тебя постоянно проблемы — место в составе не гарантируется.

9. 🕵️ <b>Никакого намеренного саботажа</b>
Не мешаем КВ, тренировкам и другим участникам специально.

10. 🔥 <b>Главное правило</b>
Не будь просто цифрой в составе. Будь частью «Шлюх надзор».

⚠️ <b>За нарушение:</b>
1 предупреждение → ограничение → исключение.
Тяжёлые нарушения могут привести к мгновенному исключению.

⏳ Варны хранятся <b>7 дней</b>.
🔇 При <b>5 активных предупреждениях</b> участнику запрещается писать в чате."""

def guest_keyboard_v71():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Регистрация", callback_data="v71_guest_register"), InlineKeyboardButton(text="📩 Запрос в ги", callback_data="v71_guest_apply")],
        [InlineKeyboardButton(text="⚔️ Предложить КВ", callback_data="v71_guest_kv"), InlineKeyboardButton(text="📜 Правила гильдии", callback_data="v71_guest_guild_rules")],
        [InlineKeyboardButton(text="⚔️ Правила КВ", callback_data="v71_guest_kv_rules"), InlineKeyboardButton(text="ℹ️ О НАС", callback_data="v71_guest_about")],
        [InlineKeyboardButton(text="❓ ПОМОЩЬ", callback_data="v71_guest_help")],
    ])

def social_text_v71():
    return ("📱 <b>НАШИ СОЦСЕТИ</b>\n\n"
            "🤖 Бот: @Nadzo69rBot\n"
            "💬 Чат: @nadzor67\n"
            "👑 Администрация: @Vavix @overside1\n"
            "📰 Новости: @ndzorsh\n"
            "🎵 TikTok: https://www.tiktok.com/@nadzor_sh")

def _admin_notify_ids(min_rank=4):
    return {uid for uid, level in ADMIN_ACCESS.items() if level >= int(min_rank)}

def _json4(raw):
    try:
        x = json.loads(raw or "[]")
        return x if isinstance(x, list) else []
    except Exception:
        return []

def _kv_count_active():
    return len(db.get_kvs(status="planned", limit=10))

def _kv_proposal_keyboard(app_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять КВ", callback_data=f"v71_kv_accept_{app_id}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"v71_kv_decline_{app_id}")]
    ])

# ---- Replace guest entry point with the final guest panel ----
@dp.message(Command("guest", "guestpanel", "гость"))
async def command_guest_v71(message: Message):
    if db.get_player_by_telegram(message.from_user.id):
        await message.answer("👤 Ты уже зарегистрирован.", reply_markup=panel_keyboard(message.from_user.id)); return
    await message.answer(GUILD_INFO_V71 + "\n\nВыбери действие:", reply_markup=guest_keyboard_v71())

@dp.callback_query(F.data.startswith("v71_guest_"))
async def callback_guest_v71(callback: CallbackQuery, state: FSMContext):
    key = callback.data
    if key == "v71_guest_about":
        text = GUILD_INFO_V71 + "\n\n" + social_text_v71()
    elif key == "v71_guest_help":
        text = ("❓ <b>ПОМОЩЬ</b>\n\n"
                "📝 Регистрация — привязать Free Fire UID к Telegram.\n"
                "📩 Запрос в ги — анкета для вступления в гильдию.\n"
                "⚔️ Предложить КВ — оформить предложение даже без регистрации.\n"
                "📜 Правила — правила гильдии и отдельные правила КВ.\n\n"
                "Команды: <code>/register UID</code>, <code>/заявка UID</code>, <code>/guest</code>, <code>/kv</code>, <code>/help</code>.")
    elif key == "v71_guest_register":
        await state.clear()
        await state.set_state(RegistrationStates.waiting_uid)
        await callback.message.answer("📝 <b>РЕГИСТРАЦИЯ</b>\n\nОтправь свой UID Free Fire (8–12 цифр).\n\nБот проверит игрока через Free Fire API и принадлежность к нашей гильдии.", reply_markup=guest_cancel_keyboard())
        await callback.answer()
        return
    elif key == "v71_guest_apply":
        await state.clear()
        await state.set_state(GuildApplicationStates.waiting_uid)
        await callback.message.answer("📩 <b>ЗАЯВКА В ГИЛЬДИЮ</b>\n\n1/4. Отправь свой UID Free Fire (8–12 цифр).", reply_markup=guest_cancel_keyboard())
        await callback.answer()
        return
    elif key == "v71_guest_kv":
        await state.clear()
        await state.set_state(KVProposalStates.guild)
        await callback.message.answer("⚔️ <b>ПРЕДЛОЖИТЬ КВ</b>\n\nСначала ознакомься с правилами КВ.\n\n1/6. Напиши название своей гильдии:", reply_markup=guest_cancel_keyboard())
        await callback.answer()
        return
    elif key == "v71_guest_guild_rules":
        await send_rules_bundle(callback.message, "guild", reply_markup=guest_keyboard_v71())
        await callback.answer()
        return
    elif key == "v71_guest_kv_rules":
        await send_rules_bundle(callback.message, "kv", reply_markup=guest_keyboard_v71())
        await callback.answer()
        return
    else:
        text = RULES_TEXT_V71
    await safe_edit(callback.message, text, reply_markup=guest_keyboard_v71())
    await callback.answer()

# ---- Applications ----
@dp.message(Command("apply", "заявка", "вступить", "join", "join_guild"))
async def command_apply_v71(message: Message, state: FSMContext):
    if db.get_player_by_telegram(message.from_user.id):
        await message.answer("Ты уже участник гильдии."); return
    uid = extract_uid(message.text or "")
    if not uid:
        await state.clear()
        await state.set_state(GuildApplicationStates.waiting_uid)
        await message.answer("📩 <b>ЗАЯВКА В ГИЛЬДИЮ</b>\n\n1/4. Отправь свой UID Free Fire (8–12 цифр).", reply_markup=guest_cancel_keyboard())
        return
    if db.conn.execute("SELECT 1 FROM guild_applications WHERE telegram_id=? AND status='pending'", (message.from_user.id,)).fetchone():
        await message.answer("⏳ У тебя уже есть ожидающая заявка."); return
    nick = "Не проверен"
    try:
        profile = await ff_client.get_player_profile(uid, FF_REGION)
        if profile: nick = profile.nickname
    except Exception: pass
    app_id = db.create_application(message.from_user.id, message.from_user.username, uid, nick)
    text = (f"📩 <b>НОВАЯ ЗАЯВКА #{app_id}</b>\n\n"
            f"👤 {html.escape(message.from_user.full_name)}\n"
            f"🆔 UID: <code>{uid}</code>\n🎮 Ник: <b>{html.escape(nick)}</b>")
    for admin in _admin_notify_ids(6):
        try: await bot.send_message(admin, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять",callback_data=f"v71_app_accept_{app_id}"),InlineKeyboardButton(text="❌ Отклонить",callback_data=f"v71_app_decline_{app_id}")]]))
        except Exception: pass
    await message.answer(f"✅ Заявка #{app_id} отправлена руководству.")

@dp.callback_query(F.data == "vaka_flow_cancel")
async def callback_vaka_flow_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if db.get_player_by_telegram(callback.from_user.id):
        await safe_edit(callback.message, build_admin_panel(callback.from_user.id), reply_markup=panel_keyboard(callback.from_user.id))
    else:
        await safe_edit(callback.message, GUILD_INFO_V71 + "\n\nВыбери действие:", reply_markup=guest_keyboard_v71())
    await callback.answer("Отменено")

@dp.message(RegistrationStates.waiting_uid)
async def registration_from_panel(message: Message, state: FSMContext):
    uid = extract_uid(message.text or "")
    if not uid or not uid.isdigit() or not 8 <= len(uid) <= 12:
        await message.answer("❌ UID должен содержать от 8 до 12 цифр.", reply_markup=guest_cancel_keyboard())
        return
    # Reuse the proven registration path by dispatching the existing handler.
    await state.clear()
    aliased = message.model_copy(update={"text": f"/register {uid}"})
    await command_register(aliased)

@dp.message(GuildApplicationStates.waiting_uid)
async def guild_application_uid(message: Message, state: FSMContext):
    uid = extract_uid(message.text or "")
    if not uid or not uid.isdigit() or not 8 <= len(uid) <= 12:
        await message.answer("❌ UID должен содержать от 8 до 12 цифр.", reply_markup=guest_cancel_keyboard())
        return
    if db.conn.execute("SELECT 1 FROM guild_applications WHERE telegram_id=? AND status='pending'", (message.from_user.id,)).fetchone():
        await state.clear(); await message.answer("⏳ У тебя уже есть ожидающая заявка."); return
    nick = "Не проверен"
    try:
        profile = await ff_client.get_player_profile(uid, FF_REGION)
        if profile: nick = profile.nickname
    except Exception: pass
    await state.update_data(uid=uid, nick=nick)
    await state.set_state(GuildApplicationStates.waiting_why)
    await message.answer(f"🎮 Ник: <b>{html.escape(nick)}</b>\n\n2/4. Почему хочешь вступить именно к нам?", reply_markup=guest_cancel_keyboard())

@dp.message(GuildApplicationStates.waiting_why)
async def guild_application_why(message: Message, state: FSMContext):
    text=(message.text or "").strip()
    if not text: await message.answer("❌ Напиши ответ.", reply_markup=guest_cancel_keyboard()); return
    await state.update_data(why_join=text)
    await state.set_state(GuildApplicationStates.waiting_found)
    await message.answer("3/4. Как ты нас нашёл?", reply_markup=guest_cancel_keyboard())

@dp.message(GuildApplicationStates.waiting_found)
async def guild_application_found(message: Message, state: FSMContext):
    text=(message.text or "").strip()
    if not text: await message.answer("❌ Напиши ответ.", reply_markup=guest_cancel_keyboard()); return
    await state.update_data(how_found=text)
    await state.set_state(GuildApplicationStates.waiting_extra)
    await message.answer("4/4. Есть ли что-то ещё, что хочешь рассказать о себе? Можно написать «нет».", reply_markup=guest_cancel_keyboard())

@dp.message(GuildApplicationStates.waiting_extra)
async def guild_application_extra(message: Message, state: FSMContext):
    extra=(message.text or "").strip() or "нет"
    data=await state.get_data()
    app_id=db.create_application(
        message.from_user.id, message.from_user.username, data["uid"], data.get("nick") or "Не проверен",
        data.get("why_join"), data.get("how_found"), extra
    )
    text=(f"📩 <b>НОВАЯ ЗАЯВКА В ГИЛЬДИЮ #{app_id}</b>\n\n"
          f"👤 {html.escape(message.from_user.full_name)}\n"
          f"🆔 UID: <code>{html.escape(data['uid'])}</code>\n"
          f"🎮 Ник: <b>{html.escape(data.get('nick') or 'Не проверен')}</b>\n\n"
          f"❓ <b>Почему к нам:</b>\n{html.escape(data.get('why_join') or '—')}\n\n"
          f"🔎 <b>Как нас нашёл:</b>\n{html.escape(data.get('how_found') or '—')}\n\n"
          f"📝 <b>Дополнительно:</b>\n{html.escape(extra)}")
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять",callback_data=f"v71_app_accept_{app_id}"),InlineKeyboardButton(text="❌ Отклонить",callback_data=f"v71_app_decline_{app_id}")]])
    for admin in _admin_notify_ids(6):
        try: await bot.send_message(admin,text,reply_markup=kb)
        except Exception: pass
    await state.clear()
    await message.answer(f"✅ Заявка #{app_id} отправлена руководству. Ожидай решения.")


@dp.callback_query(F.data.startswith("v71_app_accept_") | F.data.startswith("v71_app_decline_"))
async def callback_application_v71(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    try: app_id=int(callback.data.rsplit("_",1)[1])
    except ValueError: await callback.answer("Ошибка заявки",show_alert=True); return
    row=db.conn.execute("SELECT * FROM guild_applications WHERE id=?",(app_id,)).fetchone()
    if not row or row["status"]!="pending": await callback.answer("Заявка уже обработана.",show_alert=True); return
    if callback.data.startswith("v71_app_accept_"):
        # Acceptance only approves the application; registration still requires the user to pass the normal guild check.
        db.set_application_status(app_id,"approved",callback.from_user.id)
        try: await bot.send_message(row["telegram_id"], "✅ Твоя заявка одобрена. Теперь пройди регистрацию: <code>регистрация UID</code>")
        except Exception: pass
        await callback.message.edit_text(callback.message.text + "\n\n✅ <b>ОДОБРЕНО</b>")
    else:
        db.set_application_status(app_id,"declined",callback.from_user.id)
        try: await bot.send_message(row["telegram_id"], "❌ Заявка на вступление отклонена руководством.")
        except Exception: pass
        await callback.message.edit_text(callback.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>")
    await callback.answer()

def kv_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена",callback_data="guestkv:cancel")]])

def kv_our_kb(selected):
    rows=[]
    for p in db.get_all_players():
        uid=p["telegram_id"]
        if not uid: continue
        name=(p["nick"] or p["telegram_username"] or str(uid))[:30]
        mark="✅ " if name in selected else ""
        rows.append([InlineKeyboardButton(text=mark+name,callback_data=f"guestkv:our:{uid}")])
    rows.append([InlineKeyboardButton(text=f"➡️ Далее ({len(selected)}/4)",callback_data="guestkv:our_done")])
    rows.append([InlineKeyboardButton(text="❌ Отмена",callback_data="guestkv:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def guest_kv_roster_kb():
    rows=[]
    rosters=db.get_kv_rosters()
    for slot in (1,2,3):
        members=rosters.get(slot,[])
        label=f"⚔️ {KV_ROSTER_NAMES.get(slot, f'Состав {slot}')}"
        if members:
            label += " — " + ", ".join(str(x) for x in members[:4])
        else:
            label += " — не настроен"
        rows.append([InlineKeyboardButton(text=label[:60], callback_data=f"guestkv:roster:{slot}")])
    rows.append([InlineKeyboardButton(text="❌ Отмена",callback_data="guestkv:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

KV_ROSTER_NAMES = {1: "Телефоншики", 2: "ПК", 3: "ПК + Телефон"}

def kv_roster_admin_slots_kb():
    rows=[]
    rosters=db.get_kv_rosters()
    for slot in (1,2,3):
        members=rosters.get(slot,[])
        label=f"⚔️ {KV_ROSTER_NAMES.get(slot, f'Состав {slot}')} ({len(members)}/4)"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"kvroster:slot:{slot}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад",callback_data="menu_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kv_roster_players_kb(slot, selected):
    rows=[]
    selected={int(x) for x in selected}
    for p in db.get_all_players():
        uid=p["telegram_id"]
        if not uid: continue
        name=(p["nick"] or p["telegram_username"] or str(uid))[:28]
        mark="✅ " if int(uid) in selected else ""
        rows.append([InlineKeyboardButton(text=mark+name,callback_data=f"kvroster:pick:{slot}:{uid}")])
    rows.append([InlineKeyboardButton(text=f"💾 Сохранить ({len(selected)}/4)",callback_data=f"kvroster:save:{slot}")])
    rows.append([InlineKeyboardButton(text="⬅️ К составам",callback_data="menu_kv_rosters")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data=="menu_kv_rosters")
async def callback_kv_rosters(callback:CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).",show_alert=True); return
    await safe_edit(callback.message,"👥 <b>СОСТАВЫ КВ</b>\n\nНастрой три состава по 4 игрока. Эти составы будут показываться противнику при предложении КВ.",reply_markup=kv_roster_admin_slots_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("kvroster:slot:"))
async def callback_kv_roster_slot(callback:CallbackQuery,state:FSMContext):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).",show_alert=True); return
    slot=int(callback.data.rsplit(":",1)[1])
    await state.clear(); await state.set_state(KVRosterStates.members)
    await state.update_data(roster_slot=slot,roster_members=[int(x["telegram_id"]) for x in db.get_all_players() if False])
    current=[]
    # Existing roster stores names; map them back to Telegram IDs where possible.
    for p in db.get_all_players():
        uid=p["telegram_id"]; name=p["nick"] or p["telegram_username"] or str(uid)
        if uid and name in db.get_kv_roster(slot): current.append(int(uid))
    await state.update_data(roster_members=current)
    await safe_edit(callback.message, f"⚔️ <b>{KV_ROSTER_NAMES.get(slot, f'СОСТАВ {slot}')}</b>\n\nВыбери до 4 зарегистрированных участников:", reply_markup=kv_roster_players_kb(slot,current))
    await callback.answer()

@dp.callback_query(F.data.startswith("kvroster:pick:"))
async def callback_kv_roster_pick(callback:CallbackQuery,state:FSMContext):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    if await state.get_state()!=KVRosterStates.members.state:
        await callback.answer("Сессия настройки состава устарела.",show_alert=True); return
    _,_,slot_s,uid_s=callback.data.split(":")
    slot,uid=int(slot_s),int(uid_s)
    data=await state.get_data(); selected=[int(x) for x in data.get("roster_members",[])]
    if uid in selected: selected.remove(uid)
    elif len(selected)<4: selected.append(uid)
    else: await callback.answer("Можно выбрать только 4 игроков.",show_alert=True); return
    await state.update_data(roster_members=selected)
    await callback.message.edit_reply_markup(reply_markup=kv_roster_players_kb(slot,selected))
    await callback.answer(f"Выбрано {len(selected)}/4")

@dp.callback_query(F.data.startswith("kvroster:save:"))
async def callback_kv_roster_save(callback:CallbackQuery,state:FSMContext):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).",show_alert=True); return
    slot=int(callback.data.rsplit(":",1)[1]); data=await state.get_data(); selected=[int(x) for x in data.get("roster_members",[])]
    if len(selected)!=4:
        await callback.answer("Нужно выбрать ровно 4 игроков.",show_alert=True); return
    names=[]
    for uid in selected:
        p=db.get_player_by_telegram(uid)
        if p: names.append(p["nick"] or p["telegram_username"] or str(uid))
    if len(names)!=4:
        await callback.answer("Один из игроков больше не зарегистрирован.",show_alert=True); return
    db.set_kv_roster(slot,names); await state.clear()
    await safe_edit(callback.message,f"✅ <b>СОСТАВ {slot} СОХРАНЁН</b>\n\n👥 {', '.join(html.escape(x) for x in names)}",reply_markup=kv_roster_admin_slots_kb())
    await callback.answer("Сохранено")

@dp.callback_query(F.data.startswith("guestkv:roster:"))
async def guest_kv_roster(callback:CallbackQuery,state:FSMContext):
    if await state.get_state()!=KVProposalStates.our_members.state:
        await callback.answer("Сейчас выбор состава недоступен.",show_alert=True); return
    slot=int(callback.data.rsplit(":",1)[1]); members=db.get_kv_roster(slot)
    if len(members)!=4:
        await callback.answer("Этот состав ещё не настроен администрацией.",show_alert=True); return
    await state.update_data(our_roster=slot,our_members=members)
    await state.set_state(KVProposalStates.match_date)
    await callback.message.edit_text(f"⚔️ Выбран состав: {KV_ROSTER_NAMES.get(slot, f'Состав {slot}')}:\n👥 {', '.join(html.escape(x) for x in members)}\n\n4/6. Выбери дату КВ:",reply_markup=kv_date_kb())
    await callback.answer("Состав выбран")

def kv_date_kb():
    d=datetime.now(ZoneInfo(TIMEZONE)).date()
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"Сегодня · {d:%d.%m}",callback_data=f"guestkv:date:{d.isoformat()}")],[InlineKeyboardButton(text=f"Завтра · {(d+timedelta(days=1)):%d.%m}",callback_data=f"guestkv:date:{(d+timedelta(days=1)).isoformat()}")],[InlineKeyboardButton(text="📅 Другая дата",callback_data="guestkv:date_other")],[InlineKeyboardButton(text="❌ Отмена",callback_data="guestkv:cancel")]])

def kv_time_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="18:00",callback_data="guestkv:time:18:00"),InlineKeyboardButton(text="19:00",callback_data="guestkv:time:19:00")],[InlineKeyboardButton(text="20:00",callback_data="guestkv:time:20:00"),InlineKeyboardButton(text="21:00",callback_data="guestkv:time:21:00")],[InlineKeyboardButton(text="🕐 Другое время",callback_data="guestkv:time_other")],[InlineKeyboardButton(text="❌ Отмена",callback_data="guestkv:cancel")]])

def kv_purpose_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⚔️ Обычный КВ",callback_data="guestkv:purpose:КВ")],[InlineKeyboardButton(text="🎯 Тренировочный КВ",callback_data="guestkv:purpose:Тренировочный КВ")],[InlineKeyboardButton(text="📝 Другое",callback_data="guestkv:purpose_other")],[InlineKeyboardButton(text="❌ Отмена",callback_data="guestkv:cancel")]])

@dp.callback_query(F.data == "guestkv:cancel")
async def guest_kv_cancel(callback:CallbackQuery,state:FSMContext):
    await state.clear(); await callback.message.edit_text("❌ Создание КВ отменено.",reply_markup=guest_keyboard_v71()); await callback.answer()

@dp.callback_query(F.data.startswith("guestkv:our:"))
async def guest_kv_select_our(callback:CallbackQuery,state:FSMContext):
    if await state.get_state()!=KVProposalStates.our_members.state: await callback.answer("Сейчас выбор недоступен.",show_alert=True); return
    uid=int(callback.data.rsplit(":",1)[1]); row=db.get_player_by_telegram(uid)
    if not row: await callback.answer("Игрок не найден.",show_alert=True); return
    name=row["nick"] or row["telegram_username"] or str(uid); data=await state.get_data(); selected=list(data.get("our_members",[]))
    if name in selected: selected.remove(name)
    elif len(selected)<4: selected.append(name)
    else: await callback.answer("Можно выбрать только 4 игроков.",show_alert=True); return
    await state.update_data(our_members=selected); await callback.message.edit_reply_markup(reply_markup=kv_our_kb(selected)); await callback.answer(f"Выбрано {len(selected)}/4")

@dp.callback_query(F.data=="guestkv:our_done")
async def guest_kv_our_done(callback:CallbackQuery,state:FSMContext):
    selected=(await state.get_data()).get("our_members",[])
    if len(selected)!=4: await callback.answer("Выбери ровно 4 игроков.",show_alert=True); return
    await state.set_state(KVProposalStates.match_date); await callback.message.edit_text("4/6. Выбери дату КВ:",reply_markup=kv_date_kb()); await callback.answer()

@dp.callback_query(F.data.startswith("guestkv:date:"))
async def guest_kv_date_button(callback:CallbackQuery,state:FSMContext):
    val=callback.data.split(":",2)[2]
    await state.update_data(match_date=val); await state.set_state(KVProposalStates.match_time); await callback.message.edit_text("5/6. Выбери время КВ (МСК):",reply_markup=kv_time_kb()); await callback.answer()

@dp.callback_query(F.data=="guestkv:date_other")
async def guest_kv_date_other(callback:CallbackQuery,state:FSMContext):
    await callback.message.edit_text("📅 Введи дату YYYY-MM-DD:",reply_markup=kv_cancel_kb()); await callback.answer()

@dp.callback_query(F.data.startswith("guestkv:time:"))
async def guest_kv_time_button(callback:CallbackQuery,state:FSMContext):
    val=callback.data.split(":",2)[2]; await state.update_data(match_time=val); await state.set_state(KVProposalStates.purpose); await callback.message.edit_text("6/6. Выбери назначение КВ:",reply_markup=kv_purpose_kb()); await callback.answer()

@dp.callback_query(F.data=="guestkv:time_other")
async def guest_kv_time_other(callback:CallbackQuery,state:FSMContext):
    await callback.message.edit_text("🕐 Введи время HH:MM (МСК):",reply_markup=kv_cancel_kb()); await callback.answer()

@dp.callback_query(F.data.startswith("guestkv:purpose:"))
async def guest_kv_purpose_button(callback:CallbackQuery,state:FSMContext):
    await state.update_data(purpose=callback.data.split(":",2)[2]); await finalize_guest_kv(callback.message,state,callback.from_user.id); await callback.answer("Заявка отправлена")

@dp.callback_query(F.data=="guestkv:purpose_other")
async def guest_kv_purpose_other(callback:CallbackQuery,state:FSMContext):
    await callback.message.edit_text("📝 Напиши назначение КВ:",reply_markup=kv_cancel_kb()); await callback.answer()

async def finalize_guest_kv(message,state,proposer_id):
    data=await state.get_data()
    if _kv_count_active()>=3: await state.clear(); await message.answer("❌ Сейчас заняты все 3 слота КВ."); return
    kid=db.create_kv("Предложение КВ",data["match_date"],data["match_time"],data["purpose"],data["enemy_guild"],data["enemy_members"],data["our_members"],0,proposer_id); db.set_kv_status(kid,"proposal")
    text=(f"⚔️ <b>ПРЕДЛОЖЕНИЕ КВ #{kid}</b>\n\n🏰 Гильдия противника: <b>{html.escape(data['enemy_guild'])}</b>\n👥 <b>Наш состав:</b> {', '.join(map(html.escape,data['our_members']))}\n👥 <b>Состав противника:</b> {', '.join(map(html.escape,data['enemy_members']))}\n🕐 {data['match_date']} {data['match_time']} МСК\n🎯 {html.escape(data['purpose'])}\n📩 От: {mention_user(proposer_id,str(proposer_id))}")
    for admin in _admin_notify_ids(4):
        try: await bot.send_message(admin,text,reply_markup=_kv_proposal_keyboard(kid))
        except Exception: logger.exception("Не удалось отправить КВ админу %s",admin)
    await state.clear(); await message.answer("✅ Предложение КВ отправлено руководству.")

# ---- Guest KV proposal FSM ----
@dp.message(KVProposalStates.guild)
async def v71_kv_guild(message: Message, state: FSMContext):
    guild = (message.text or "").strip()
    if len(guild) < 2 or len(guild) > 80:
        await message.answer("❌ Название гильдии: 2–80 символов.")
        return
    await state.update_data(enemy_guild=guild)
    await state.set_state(KVProposalStates.enemy_members)
    await message.answer("2/6. Введи состав гильдии противника — 4 игрока через запятую:")

@dp.message(KVProposalStates.enemy_members)
async def v71_kv_enemy_members(message: Message, state: FSMContext):
    members = [x.strip() for x in re.split(r"[,;\n]+", message.text or "") if x.strip()]
    if len(members) != 4:
        await message.answer("❌ Нужно ровно 4 игрока противника.")
        return
    await state.update_data(enemy_members=members)
    await state.set_state(KVProposalStates.our_members)
    await message.answer("3/4. Выбери, какой из наших трёх составов нужен противнику:", reply_markup=guest_kv_roster_kb())

@dp.message(KVProposalStates.our_members)
async def v71_kv_our_members(message:Message,state:FSMContext):
    await message.answer("👇 Выбери один из трёх готовых составов КВ:", reply_markup=guest_kv_roster_kb())
@dp.message(KVProposalStates.match_date)
async def v71_kv_date(message:Message,state:FSMContext):
    try: datetime.strptime((message.text or "").strip(),"%Y-%m-%d")
    except ValueError: await message.answer("❌ Формат YYYY-MM-DD",reply_markup=kv_date_kb()); return
    await state.update_data(match_date=message.text.strip()); await state.set_state(KVProposalStates.match_time); await message.answer("5/6. Выбери время:",reply_markup=kv_time_kb())
@dp.message(KVProposalStates.match_time)
async def v71_kv_time(message:Message,state:FSMContext):
    try: datetime.strptime((message.text or "").strip(),"%H:%M")
    except ValueError: await message.answer("❌ Формат HH:MM",reply_markup=kv_time_kb()); return
    await state.update_data(match_time=message.text.strip()); await state.set_state(KVProposalStates.purpose); await message.answer("6/6. Выбери назначение:",reply_markup=kv_purpose_kb())
@dp.message(KVProposalStates.purpose)
async def v71_kv_purpose(message:Message,state:FSMContext):
    purpose=(message.text or "").strip()
    if not purpose: await message.answer("❌ Назначение не может быть пустым.",reply_markup=kv_purpose_kb()); return
    await state.update_data(purpose=purpose); await finalize_guest_kv(message,state,message.from_user.id)

@dp.callback_query(F.data.startswith("v71_kv_accept_") | F.data.startswith("v71_kv_decline_"))
async def callback_kv_proposal_v71(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True)
        return
    try:
        kid = int(callback.data.rsplit("_", 1)[1])
    except ValueError:
        await callback.answer("Ошибка", show_alert=True)
        return
    row = db.conn.execute("SELECT * FROM kv_matches WHERE id=?", (kid,)).fetchone()
    if not row or row["status"] != "proposal":
        await callback.answer("Предложение уже обработано.", show_alert=True)
        return
    if callback.data.startswith("v71_kv_decline_"):
        db.set_kv_status(kid, "declined")
        await callback.message.edit_text(callback.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>")
        await callback.answer()
        return
    if _kv_count_active() >= 3:
        await callback.answer("Все 3 слота КВ уже заняты.", show_alert=True)
        return
    enemy = _json4(row["enemy_members"])
    ours = _json4(row["our_members"])
    publish_text = (
        f"⚔️ <b>КВ НАЗНАЧЕНО</b>\n\n"
        f"🏰 Наша гильдия: <b>{html.escape(GUILD_NAME_V71)}</b>\n"
        f"⚔️ Против: <b>{html.escape(row['enemy_guild'] or '—')}</b>\n\n"
        f"👥 <b>Наш состав:</b> {', '.join(html.escape(x) for x in ours) if ours else '—'}\n"
        f"👥 <b>Состав противника:</b> {', '.join(html.escape(x) for x in enemy) if enemy else '—'}\n\n"
        f"🕐 <b>{row['match_date']} {row['match_time']} МСК</b>\n"
        f"🎯 {html.escape(row['purpose'] or '—')}\n"
        f"🆔 КВ #{kid}"
    )
    published_ok = False
    publish_errors = 0
    for kv_chat_id in KV_PUBLISH_CHAT_IDS:
        try:
            await bot.send_message(kv_chat_id, publish_text)
            published_ok = True
        except Exception as exc:
            publish_errors += 1
            logger.exception("KV publish failed for chat %s: %s", kv_chat_id, exc)
    if publish_errors == len(KV_PUBLISH_CHAT_IDS):
        published_ok = False
    db.set_kv_status(kid, "planned")
    suffix = "\n📢 Опубликовано в чате." if published_ok else "\n⚠️ Не удалось автоматически опубликовать в чат. Проверь GUILD_CHAT_ID."
    await callback.message.edit_text(callback.message.text + "\n\n✅ <b>КВ ПРИНЯТО</b>" + suffix)
    if row["proposer_id"]:
        try:
            await bot.send_message(row["proposer_id"], f"✅ Твоё предложение КВ #{kid} принято руководством.")
        except Exception:
            pass
    await callback.answer("КВ создано")

# ---- Admin KV creation / management ----
@dp.message(Command("kvrosters","составыкв"))
async def command_kvrosters(message: Message):
    if not management_admin(message.from_user.id): await message.answer("🔒 Только руководство (6–8)."); return
    await message.answer("👥 <b>СОСТАВЫ КВ</b>\n\nНастрой три состава по 4 игрока:", reply_markup=kv_roster_admin_slots_kb())

@dp.message(Command("kvcreate","создатькв","квсоздать"))
async def command_kvcreate_v71(message: Message, state: FSMContext):
    if not management_admin(message.from_user.id): await message.answer("🔒 Только руководство (6–8)."); return
    if _kv_count_active()>=3: await message.answer("❌ Все 3 слота КВ заняты."); return
    await state.clear(); await state.set_state(KVCreateStates.title); await message.answer("⚔️ Создание КВ 1/7. Название КВ:")

@dp.message(KVCreateStates.title)
async def v71_kvc_title(message: Message,state:FSMContext): await state.update_data(title=message.text.strip()); await state.set_state(KVCreateStates.enemy_guild); await message.answer("2/7. Название гильдии противника:")
@dp.message(KVCreateStates.enemy_guild)
async def v71_kvc_enemy_guild(message: Message,state:FSMContext): await state.update_data(enemy_guild=message.text.strip()); await state.set_state(KVCreateStates.enemy_members); await message.answer("3/7. Состав противника — ровно 4 игрока через запятую:")
@dp.message(KVCreateStates.enemy_members)
async def v71_kvc_enemy_members(message: Message,state:FSMContext):
    a=[x.strip() for x in re.split(r"[,;\n]+",message.text or "") if x.strip()]
    if len(a)!=4: await message.answer("❌ Нужно ровно 4 игрока."); return
    await state.update_data(enemy_members=a); await state.set_state(KVCreateStates.our_members); await message.answer("4/7. Наш состав — ровно 4 игрока через запятую:")
@dp.message(KVCreateStates.our_members)
async def v71_kvc_our_members(message: Message,state:FSMContext):
    a=[x.strip() for x in re.split(r"[,;\n]+",message.text or "") if x.strip()]
    if len(a)!=4: await message.answer("❌ Нужно ровно 4 игрока."); return
    await state.update_data(our_members=a); await state.set_state(KVCreateStates.match_date); await message.answer("5/7. Дата YYYY-MM-DD:")
@dp.message(KVCreateStates.match_date)
async def v71_kvc_date(message: Message,state:FSMContext):
    try: datetime.strptime(message.text.strip(),"%Y-%m-%d")
    except ValueError: await message.answer("❌ Формат YYYY-MM-DD"); return
    await state.update_data(match_date=message.text.strip()); await state.set_state(KVCreateStates.match_time); await message.answer("6/7. Время HH:MM МСК:")
@dp.message(KVCreateStates.match_time)
async def v71_kvc_time(message: Message,state:FSMContext):
    try: datetime.strptime(message.text.strip(),"%H:%M")
    except ValueError: await message.answer("❌ Формат HH:MM"); return
    await state.update_data(match_time=message.text.strip()); await state.set_state(KVCreateStates.purpose); await message.answer("7/7. Назначение КВ:")
@dp.message(KVCreateStates.purpose)
async def v71_kvc_purpose(message: Message,state:FSMContext):
    d=await state.update_data(purpose=message.text.strip()); d=await state.get_data()
    kid=db.create_kv(d["title"],d["match_date"],d["match_time"],d["purpose"],d["enemy_guild"],d["enemy_members"],d["our_members"],message.from_user.id)
    await state.clear(); await message.answer(f"✅ КВ #{kid} создан. Используй <code>кв</code> для просмотра.")

def _format_kv_scheduled(row):
    enemy=", ".join(_json4(row["enemy_members"])) or "—"
    ours=", ".join(_json4(row["our_members"])) or "—"
    return (
        "⚔️ <b>КВ НАЗНАЧЕНО</b>\n\n"
        f"🏰 Наша гильдия: <b>{html.escape(GUILD_NAME_V71)}</b>\n"
        f"⚔️ Против: <b>{html.escape(row['enemy_guild'] or '—')}</b>\n\n"
        f"👥 Наш состав: {html.escape(ours)}\n"
        f"👥 Состав противника: {html.escape(enemy)}\n\n"
        f"🕐 {html.escape(str(row['match_date'] or '—'))} {html.escape(str(row['match_time'] or '—'))} МСК\n"
        f"🆔 КВ #{row['id']}"
    )

@dp.message(Command("kv_scheduled","назначенныекв","назначенные","назначеные"))
async def command_kv_scheduled(message: Message):
    rows=db.get_kvs(status="planned",limit=20)
    if not rows:
        await message.answer("📭 Назначенных КВ пока нет.")
        return
    await message.answer("\n\n".join(_format_kv_scheduled(r) for r in rows))

@dp.message(Command("kv_history","историякв"))
async def command_kv_history(message: Message):
    rows=db.get_kv_history(limit=20)
    if not rows:
        await message.answer("📚 История КВ пока пуста.")
        return
    wins=sum(1 for r in rows if str(r["result"]).lower()=="победа")
    losses=sum(1 for r in rows if str(r["result"]).lower()=="поражение")
    out=["📚 <b>ИСТОРИЯ КВ</b>",""]
    for r in rows:
        label="Победа" if str(r["result"]).lower()=="победа" else "Поражение"
        out.append(
            f"<b>{label} {html.escape(str(r['match_date']))}</b>\n"
            f"Счёт\n"
            f"наша гильдия    противник\n"
            f"{int(r['our_score'])}                 {int(r['enemy_score'])}\n"
        )
    out.append(f"🏆 <b>Общий:</b> Поражений {losses} · Побед {wins}")
    await message.answer("\n".join(out))

@dp.message(Command("kvresult","кврезультат"))
async def command_kv_result(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Только руководство (6–8)."); return
    parts=(message.text or "").split()
    if len(parts)!=4 or not all(x.lstrip("-").isdigit() for x in parts[1:]):
        await message.answer("Формат: <code>кв результат ID НАШ_СЧЁТ СЧЁТ_ПРОТИВНИКА</code>"); return
    kid, our_score, enemy_score=map(int,parts[1:])
    row=db.conn.execute("SELECT * FROM kv_matches WHERE id=?",(kid,)).fetchone()
    if not row:
        await message.answer("❌ КВ не найдено."); return
    result="победа" if our_score>enemy_score else "поражение" if our_score<enemy_score else "ничья"
    db.add_kv_history(kid, datetime.now(ZoneInfo(TIMEZONE)).date().isoformat(), our_score, enemy_score, result)
    await message.answer(f"✅ Результат КВ #{kid} сохранён: <b>{our_score}:{enemy_score}</b> — {result}.")

@dp.callback_query(F.data == "admin_kv_create")
async def callback_admin_kv_create(callback: CallbackQuery, state: FSMContext):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await state.clear()
    await state.set_state(KVCreateStates.title)
    await safe_edit(callback.message, "⚔️ <b>СОЗДАНИЕ КВ</b>\n\n1/7. Название КВ:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    await callback.answer()

@dp.callback_query(F.data == "menu_kv_scheduled")
async def callback_menu_kv_scheduled(callback: CallbackQuery):
    rows=db.get_kvs(status="planned",limit=20)
    if not rows:
        await safe_edit(callback.message,"📭 <b>НАЗНАЧЕННЫХ КВ НЕТ</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    else:
        await safe_edit(callback.message,"\n\n".join(_format_kv_scheduled(r) for r in rows),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    await callback.answer()

@dp.callback_query(F.data == "menu_kv_history")
async def callback_menu_kv_history(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    rows=db.get_kv_history(limit=20)
    if not rows:
        text="📚 <b>ИСТОРИЯ КВ</b>\n\nПока пуста."
    else:
        wins=sum(1 for r in rows if str(r["result"]).lower()=="победа")
        losses=sum(1 for r in rows if str(r["result"]).lower()=="поражение")
        out=["📚 <b>ИСТОРИЯ КВ</b>",""]
        for r in rows:
            out.append(f"{'Победа' if str(r['result']).lower()=='победа' else 'Поражение'} {r['match_date']}\nСчёт: {r['our_score']} : {r['enemy_score']}\n")
        out.append(f"Общий: Поражений {losses} · Побед {wins}")
        text="\n".join(out)
    await safe_edit(callback.message,text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    await callback.answer()

# ---- Application/KV admin list commands ----
@dp.message(Command("applications","заявки","applications_list"))
async def command_applications_v71(message: Message):
    if not management_admin(message.from_user.id): await message.answer("🔒 Нет прав."); return
    rows=db.get_applications("pending",30)
    if not rows: await message.answer("📭 Ожидающих заявок нет."); return
    out=["📩 <b>ЗАЯВКИ</b>",""]
    for r in rows: out.append(f"#{r['id']} — {html.escape(r['nick'] or '—')} | UID <code>{r['uid']}</code> | @{html.escape(r['username'] or '—')}")
    await message.answer("\n".join(out))


# ---- Participant / admin KV & application panels ----
@dp.callback_query(F.data == "menu_kv")
async def callback_menu_kv(callback: CallbackQuery):
    if is_admin(callback.from_user.id):
        rows = db.get_kvs(limit=10)
        lines = ["⚔️ <b>УПРАВЛЕНИЕ КВ</b>", ""]
        if not rows:
            lines.append("📭 КВ пока нет.")
        else:
            for r in rows:
                lines.append(f"#{r['id']} • <b>{html.escape(r['enemy_guild'] or '—')}</b> • {r['match_date']} {r['match_time']} • {r['status']}")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📩 Заявки КВ", callback_data="menu_kv_proposals")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")]
        ])
    else:
        rows = db.get_kvs(status="planned", limit=10)
        lines = ["⚔️ <b>КВ ГИЛЬДИИ</b>", ""]
        if not rows:
            lines.append("📭 Назначенных КВ пока нет.")
        for r in rows:
            enemy = ", ".join(_json4(r["enemy_members"])) or "—"
            lines.extend([f"🏰 <b>{html.escape(r['enemy_guild'] or '—')}</b>", f"👥 {html.escape(enemy)}", f"🕐 {r['match_date']} {r['match_time']} МСК", f"🎯 {html.escape(r['purpose'] or '—')}", ""])
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_main")]])
    await safe_edit(callback.message, "\n".join(lines), reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "menu_kv_proposals")
async def callback_menu_kv_proposals(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Нет прав.", show_alert=True); return
    rows = db.get_kvs(status="proposal", limit=10)
    if not rows:
        await safe_edit(callback.message, "📭 Предложений КВ нет.", reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer(); return
    lines = ["📩 <b>ПРЕДЛОЖЕНИЯ КВ</b>", ""]
    buttons=[]
    for r in rows:
        lines.append(f"#{r['id']} • <b>{html.escape(r['enemy_guild'] or '—')}</b> • {r['match_date']} {r['match_time']}")
        buttons.append([InlineKeyboardButton(text=f"✅ #{r['id']}", callback_data=f"v71_kv_accept_{r['id']}"), InlineKeyboardButton(text=f"❌ #{r['id']}", callback_data=f"v71_kv_decline_{r['id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_kv")])
    await safe_edit(callback.message, "\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)); await callback.answer()

@dp.callback_query(F.data == "menu_applications")
async def callback_menu_applications(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Нет прав.", show_alert=True); return
    rows = db.get_applications("pending", 20)
    if not rows:
        await safe_edit(callback.message, "📭 Ожидающих заявок нет.", reply_markup=admin_keyboard(callback.from_user.id)); await callback.answer(); return
    lines=["📩 <b>ЗАЯВКИ В ГИЛЬДИЮ</b>", ""]
    buttons=[]
    for r in rows:
        lines.append(f"#{r['id']} • {html.escape(r['nick'] or '—')} • UID <code>{r['uid']}</code>")
        buttons.append([InlineKeyboardButton(text=f"✅ #{r['id']}", callback_data=f"v71_app_accept_{r['id']}"), InlineKeyboardButton(text=f"❌ #{r['id']}", callback_data=f"v71_app_decline_{r['id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")])
    await safe_edit(callback.message, "\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)); await callback.answer()

@dp.callback_query(F.data == "menu_apply")
async def callback_menu_apply(callback: CallbackQuery):
    await safe_edit(callback.message, "📩 <b>ЗАПРОС В ГИ</b>\n\nОтправь <code>/заявка UID</code> или <code>/apply UID</code>.", reply_markup=back_keyboard())
    await callback.answer()

@dp.message(Command("kvrules", "kv_rules", "правилакв", "правила_кв", "квправила"))
async def command_kv_rules(message: Message):
    await send_rules_bundle(message, "kv", reply_markup=back_keyboard())

@dp.message(Command("guildrules", "guild_rules", "правилагильдии", "правила_гильдии"))
async def command_guild_rules(message: Message):
    await send_rules_bundle(message, "guild", reply_markup=back_keyboard())

# ---- SiamBhau API commands ----
@dp.message(Command("ffstats", "ffstat", "статфф"))
async def command_ffstats(message: Message):
    parts=(message.text or "").split()
    if len(parts)<2 or not parts[1].isdigit():
        await message.answer("🎮 Использование: <code>/ffstats UID [br|cs]</code>"); return
    uid=parts[1]; mode=(parts[2].lower() if len(parts)>2 else "br")
    if mode not in {"br","cs"}: await message.answer("❌ Режим: br или cs"); return
    data=await ff_client.get_stats(uid, FF_REGION, mode, "RANKED" if mode=="cs" else "CAREER")
    if not data: await message.answer("❌ Статистика не получена из SiamBhau API."); return
    await message.answer("🎮 <b>FF СТАТИСТИКА</b>\n<pre>"+html.escape(json.dumps(data,ensure_ascii=False,indent=2)[:3800])+"</pre>")

@dp.message(Command("ffoutfit", "outfit", "образ"))
async def command_ffoutfit(message: Message):
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("👗 Использование: <code>/ffoutfit UID</code>")
        return
    uid = parts[1]
    profile = await ff_client.get_player_profile(uid, FF_REGION)
    if not profile:
        await message.answer("❌ Игрок не найден.")
        return
    outfit = await ff_client.get_outfit(uid, profile.region)
    if not outfit:
        await message.answer("❌ SiamBhau не вернул изображение образа.")
        return
    await message.answer_photo(
        BufferedInputFile(outfit, filename=f"ff_outfit_{uid}.png"),
        caption=f"👗 <b>FREE FIRE OUTFIT</b>\n🎮 {html.escape(profile.nickname)}\n🆔 <code>{uid}</code>"
    )


@dp.message(Command("guildinfo", "гильдияinfo", "гильдия"))
async def command_guildinfo(message: Message):
    parts=(message.text or "").split()
    clan_id=parts[1] if len(parts)>1 else FF_GUILD_ID
    if not clan_id.isdigit(): await message.answer("❌ ID гильдии должен быть числом."); return
    data=await ff_client.get_guild_info(clan_id)
    if not data: await message.answer("❌ Данные гильдии не получены."); return
    await message.answer("🏰 <b>ИНФОРМАЦИЯ О ГИЛЬДИИ</b>\n<pre>"+html.escape(json.dumps(data,ensure_ascii=False,indent=2)[:3800])+"</pre>")

@dp.message(Command("bancheck", "проверкабана"))
async def command_bancheck(message: Message):
    parts=(message.text or "").split()
    if len(parts)!=2 or not parts[1].isdigit(): await message.answer("Использование: <code>/bancheck UID</code>"); return
    data=await ff_client.ban_check(parts[1], FF_REGION)
    if not data: await message.answer("❌ Ответ Ban Check не получен."); return
    await message.answer("🛡 <b>BAN CHECK</b>\n<pre>"+html.escape(json.dumps(data,ensure_ascii=False,indent=2)[:3800])+"</pre>")

@dp.message(Command("ffapi", "siambhau"))
async def command_ffapi(message: Message):
    await message.answer("🔌 <b>SiamBhau API</b>\n\n🌐 Base: <code>https://siambhau69.eu.cc</code>\n🌍 Default region: <b>BD</b>\n🇮🇳 India: автоматический fallback на <b>IND</b>\n\n/ff UID — профиль + BR/CS + Ban Check\n/ffstats UID [br|cs] — статистика\n/guildinfo [CLAN_ID] — гильдия\n/bancheck UID — проверка бана")

# ---- Rule-linked warnings ----
def _extract_rule(reason: str):
    m=re.search(r"(?:правил(?:о|а)?|rule)\s*#?\s*(\d{1,2})",reason,re.I)
    if not m: return None
    n=int(m.group(1)); return n if 1<=n<=10 else None

# Rebind the moderation command with explicit rule awareness and the same 7-day/5-warn logic.
@dp.message(Command("warn","пред","варн","предупреждение"))
async def command_warn_v71(message: Message):
    if not moderation_admin(message.from_user.id): await message.answer("❌ Нет прав на модерацию."); return
    parts=(message.text or "").split(maxsplit=2)
    token=parts[1] if len(parts)>1 and not message.reply_to_message else None
    reason=parts[2].strip() if token and len(parts)>2 else (parts[1].strip() if len(parts)>1 and message.reply_to_message else "Нарушение правил")
    target=await resolve_moderation_target(message,token)
    if not target: await message.answer("❌ Укажи @username/Telegram ID или ответь на сообщение."); return
    if get_admin_rank(target.id)>=4: await message.answer("❌ Нельзя наказать администратора такого же/более высокого уровня."); return
    rule=_extract_rule(reason)
    if rule: reason=f"Правило №{rule}: {RULES_BY_ID[rule]}"
    count=db.add_warning(message.chat.id,target.id,message.from_user.id,reason,1)
    text=f"⚠️ <b>ПРЕДУПРЕЖДЕНИЕ</b>\n\n{mention_user(target.id,target.full_name)}\n📜 {html.escape(reason)}\n\nАктивных: <b>{count}/5</b>"
    if count>=5:
        try:
            await bot.restrict_chat_member(message.chat.id,target.id,permissions=ChatPermissions(can_send_messages=False))
            text+="\n🔇 <b>5/5 — запрет писать в чате.</b>"
        except Exception as exc: text+=f"\n⚠️ Не удалось применить ограничение: {html.escape(str(exc))}"
    db.log("warn",message.from_user.id,{"chat_id":message.chat.id,"user_id":target.id,"reason":reason,"rule":rule,"count":count})
    await message.answer(text)

# ---- Manual chat cleanup --------------------------------------------------
# Only messages observed by this bot instance are candidates. Pinned messages
# are explicitly skipped. Ordinary conversation is never classified as trash.
CLEANUP_MAX_MESSAGES = 300

def _cleanup_is_command_or_trash(message: Message) -> bool:
    """Classify only commands and the explicitly requested chat trash."""
    if getattr(message, "sticker", None):
        return True
    if getattr(message, "animation", None):  # GIF
        return True
    if getattr(message, "voice", None):
        return True
    if getattr(message, "video_note", None):
        return True
    text = (message.text or message.caption or "").strip()
    if not text:
        return False
    low = text.lower()
    if re.match(r"^(?:/|!|\.)(?:\w+)", low, flags=re.UNICODE):
        return True
    aliases=globals().get("V71_NO_SLASH", {})
    if low in aliases:
        return True
    for phrase in sorted(aliases,key=len,reverse=True):
        if low.startswith(str(phrase).lower()+" "):
            return True
    if len(text) >= 80 and len(set(text.replace(" ", ""))) <= 2:
        return True
    return False


async def _get_pinned_ids(chat_id: int) -> set[int]:
    pinned = set()
    try:
        chat = await bot.get_chat(chat_id)
        pm = getattr(chat, "pinned_message", None)
        if pm and getattr(pm, "message_id", None):
            pinned.add(int(pm.message_id))
    except Exception as exc:
        logger.warning("Could not inspect pinned message in %s: %s", chat_id, exc)
    return pinned


@dp.message(Command("clear", "clean", "очистить"))
async def command_clear_chat(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Нет прав. Очистка доступна только руководству 6–8.")
        return

    limit = CLEANUP_MAX_MESSAGES
    nums = re.findall(r"\b(\d{1,3})\b", message.text or "")
    if nums:
        limit = max(1, min(CLEANUP_MAX_MESSAGES, int(nums[-1])))

    chat_id = message.chat.id
    pinned_ids = await _get_pinned_ids(chat_id)
    candidate_ids = list(_cleanup_candidate_messages.get(chat_id, ()))
    # Include the invoking command itself even if middleware registration was late.
    if message.message_id not in candidate_ids:
        candidate_ids.append(message.message_id)
    # Bot messages are tracked separately and are always eligible.
    candidate_ids.extend(x for x in _cleanup_bot_messages.get(chat_id, ()) if x not in candidate_ids)
    candidate_ids = candidate_ids[-limit:]

    deleted = 0
    skipped_pinned = 0
    failed = 0
    for mid in reversed(candidate_ids):
        if mid in pinned_ids:
            skipped_pinned += 1
            continue
        # We only delete known command/trash messages or messages sent by this bot.
        # For incoming messages, classification is based on a small conservative set.
        if mid == message.message_id:
            should_delete = True
        else:
            should_delete = (
                mid in _cleanup_bot_messages.get(chat_id, ())
                or mid in _cleanup_candidate_messages.get(chat_id, ())
            )
        if not should_delete:
            continue
        try:
            await bot.delete_message(chat_id, mid)
            deleted += 1
        except TelegramBadRequest as exc:
            if "message to delete not found" not in str(exc).lower():
                failed += 1
        except Exception:
            failed += 1

    await message.answer(
        f"🧹 <b>Очистка чата</b>\n\n"
        f"Удалено: <b>{deleted}</b>\n"
        f"📌 Закреплённых пропущено: <b>{skipped_pinned}</b>\n"
        f"⚠️ Не удалось удалить: <b>{failed}</b>"
    )

# ---- Unified no-slash dispatcher: longest alias first, then exact command token ----
V71_NO_SLASH = {
    "снять мут":"unmute", "снять варн":"unwarn", "снять варны":"unwarn", "снять предупреждения":"unwarn",
    "мой профиль":"profile", "мой акк":"profile", "кто я":"whoami", "кто админ":"admins",
    "чат инфо":"chatinfo", "инфо чата":"chatinfo", "правила гильдии":"rules", "предупреждения":"warnings",
    "кто здесь власть":"admins", "предложить кв":"kvproposal", "создать кв":"kvcreate",
    "гостевая панель":"guest", "гостевая":"guest", "кв предложение":"kvproposal",
    "квшки":"kvs", "браки":"marry", "мой брак":"marry", "развод":"divorce", "развестись":"divorce",
    "заявки":"applications", "админка":"adminpanel", "админ панель":"adminpanel", "коин топ":"coinstop",
    "правила кв":"kvrules", "кв правила":"kvrules", "kv rules":"kvrules", "правила гильдии":"guildrules", "guild rules":"guildrules",
    "реферальная ссылка":"ref", "мой баланс":"coins", "активность":"activity", "стата":"stats",
    "статистика":"stats", "рейтинг":"top", "участники":"users", "ники":"users", "помощь":"help",
    "команды":"help", "регистрация":"register", "вступить":"apply", "заявка":"apply", "гость":"guest",
    "панель":"panel", "профиль":"profile", "топ":"top", "история":"history", "неделя":"week",
    "правила":"rules", "рефералы":"ref", "коины":"coins", "монеты":"coins", "магазин":"shop",
    "созыв":"summon", "созвать":"summon", "очистить чат":"clear", "очистить":"clear", "чистка чата":"clear", "варн":"warn", "пред":"warn", "мут":"mute", "бан":"ban", "разбан":"unban", "кик":"kick",
    "админы":"admins", "администраторы":"admins", "права":"adminpanel", "роль":"setrole", "повысить":"promote", "понизить":"demote",
    "обновить":"refresh", "логи":"logs", "итого":"total", "удалить":"removeplayer", "отвязать":"unbind",
    "whoami":"whoami", "who":"who", "profile":"profile", "top":"top", "stats":"stats", "users":"users", "rules":"rules",
    "help":"help", "coins":"coins", "shop":"shop", "ff":"ff", "register":"register", "summon":"summon",
    "warn":"warn", "mute":"mute", "unmute":"unmute", "unwarn":"unwarn", "ban":"ban", "unban":"unban", "kick":"kick",
    "marry":"marry", "divorce":"divorce", "kv":"kv", "kvs":"kvs", "guest":"guest", "apply":"apply", "applications":"applications",
}
V71_FN = {
    "unmute":command_unmute,"unwarn":command_unwarn,"profile":command_profile,"whoami":command_whoami,"admins":command_admins,
    "chatinfo":command_chatinfo,"rules":command_rules,"warnings":command_warnings,"kv":command_kv,"kvs":command_kv,"marry":command_marry,
    "divorce":command_divorce,"guest":command_guest_v71,"apply":command_apply_v71,"applications":command_applications_v71,"panel":command_panel,
    "coinstop":command_coin_top,"ref":command_ref,"coins":command_coins,"activity":command_activity,"stats":command_stats,"top":command_top,
    "users":command_users,"help":command_help,"register":command_register,"history":command_history,"week":command_week,"shop":command_shop,
    "summon":command_summon,"clear":command_clear_chat,"warn":command_warn_v71,"mute":command_mute,"ban":command_ban,"unban":command_unban,"kick":command_kick,
    "adminpanel":command_rank_adminpanel,"setrole":command_setrole,"promote":command_promote,"demote":command_demote,"refresh":command_refresh,
    "logs":command_logs,"total":command_total_activity,"removeplayer":command_removeplayer,"unbind":command_unbind,"who":command_who,"ff":command_ff,
    "yesno":command_yesno,"random":command_random,"choose":command_choose,"ping":command_ping,"chatinfo":command_chatinfo,"kvcreate":command_kvcreate_v71,
}


# =========================================================
# PRIORITY IRIS MARRIAGE DISPATCHER
# Must be registered before the generic no-slash dispatcher.
# =========================================================














# =========================================================
# CONFIRM / CANCEL
# =========================================================





# =========================================================
# LIFETIME ACTIVITY COMMAND
# =========================================================




# =========================================================
# RANK / ADMIN MANAGEMENT
# =========================================================















# =========================================================
# ADMIN COMMANDS
# =========================================================












# =========================================================
# OWNER BACKUP CALLBACKS
# =========================================================






















# =========================================================
# GUEST / KV / MARRIAGE / RP MODULE
# =========================================================
GUILD_INFO = "🏰 <b>ШЛЮХ НАДЗОР</b>\n\n🎮 Гильдия Free Fire\n🤖 Бот: @Nadzo69rBot\n💬 Чат: @nadzor67\n📰 Новости: @ndzorsh"
RULES_TEXT = """🚨 <b>ПРАВИЛА «ШЛЮХ НАДЗОР»</b>\n\n1. 🤝 Уважение\nУважительно относимся к участникам. Шутки разрешены, но если человек просит остановиться — останавливаемся.\n\n2. 🚫 Никакой политики и национальной/религиозной вражды\nПолитические срачи и вражда → предупреждение / кик.\n\n3. 🎤 Будь частью команды\nПо возможности играем с микрофоном и общаемся.\n\n4. 🎮 Участие в жизни гильдии\nКВ, тренировки, совместные игры и мероприятия — желательно участвовать.\n\n5. 💤 AFK\nУходишь надолго — предупреди руководство.\n\n6. 💎 Награды\nПопрошайничество запрещено.\n\n7. 👑 Руководство\nРешения по составу и организации принимает руководство.\n\n8. 🧠 Адекватность\nЕсли от тебя постоянно проблемы — место в составе не гарантируется.\n\n9. 🕵️ Никакого намеренного саботажа\nНе мешаем КВ, тренировкам и другим участникам специально.\n\n10. 🔥 Главное правило\nНе будь просто цифрой в составе. Будь частью «Шлюх надзор».\n\n⚠️ За нарушение:\n1 предупреждение → ограничение → исключение. Тяжёлые нарушения могут привести к мгновенному исключению.\n\n⏳ Варны действуют 7 дней. 5 активных варнов → запрет писать в чате."""












# =========================================================
# V7.1 FEATURE COMPLETION PACK
# =========================================================

# Feature-specific FSM states. They are intentionally separate from the
# legacy states so existing flows remain untouched.
class KVProposalStates(StatesGroup):
    guild = State()
    enemy_members = State()
    our_members = State()
    match_date = State()
    match_time = State()
    purpose = State()
    confirm = State()

class KVRosterStates(StatesGroup):
    slot = State()
    members = State()

class KVCreateStates(StatesGroup):
    title = State()
    enemy_guild = State()
    enemy_members = State()
    our_members = State()
    match_date = State()
    match_time = State()
    purpose = State()

class ApplicationReviewStates(StatesGroup):
    pass

RULES_BY_ID = {
    1: "🤝 Уважение — оскорбления/травля после просьбы остановиться",
    2: "🚫 Никакой политики и национальной/религиозной вражды",
    3: "🎤 Будь частью команды — систематическое игнорирование командного взаимодействия",
    4: "🎮 Участие в жизни гильдии — систематический отказ от жизни состава",
    5: "💤 AFK — длительное отсутствие без предупреждения",
    6: "💎 Награды — попрошайничество",
    7: "👑 Руководство — конфликтное/деструктивное оспаривание решений",
    8: "🧠 Адекватность — систематическое создание проблем",
    9: "🕵️ Саботаж — намеренное вмешательство в КВ/тренировки",
    10: "🔥 Главное правило — не быть источником постоянных проблем в составе",
}

# Broad, non-explicit RP action vocabulary. Explicit sexual acts are deliberately
# excluded; the action engine itself is extensible and stores every invocation.
RP_ACTIONS_V71 = RP_ACTIONS.copy()
RP_ALIASES_V71 = RP_ALIASES.copy()

GUILD_NAME_V71 = "ɯᴧюх нᴀдзоᴩ"

GUILD_INFO_V71 = (
    "🏰 <b>ШЛЮХ НАДЗОР</b>\n\n"
    "🎮 Free Fire гильдия\n"
    "🤖 Бот: @Nadzo69rBot\n"
    "💬 Чат: @nadzor67\n"
    "👑 Администрация: @Vavix @overside1 @swswswqqqq\n"
    "📰 Новости: @ndzorsh\n"
    "🎵 TikTok: @nadzor_sh"
)
RULES_TEXT_V71 = """🚨 <b>ПРАВИЛА «ШЛЮХ НАДЗОР»</b>

1. 🤝 <b>Уважение</b>
Уважительно относимся к участникам. Шутки разрешены, но если человек просит остановиться — останавливаемся.

2. 🚫 <b>Никакой политики и национальной/религиозной вражды</b>
Мы здесь играть, а не выяснять, кто лучше. Оскорбления по национальности, религии или политические срачи → предупреждение / кик в зависимости от ситуации.

3. 🎤 <b>Будь частью команды</b>
По возможности играем с микрофоном и общаемся. Постоянно сидеть молча и никогда не играть с другими участниками — не наша философия.

4. 🎮 <b>Участие в жизни гильдии</b>
КВ, тренировки, совместные игры, мероприятия и розыгрыши — желательно участвовать.

5. 💤 <b>AFK</b>
Уходишь надолго — предупреди руководство. Если человек пропал без предупреждения, руководство может убрать его из состава.

6. 💎 <b>Награды</b>
Награды получают участники, которые выполнили условия активности. Попрошайничество запрещено.

7. 👑 <b>Руководство</b>
Решения по составу и организации гильдии принимает руководство. Если не согласен — спокойно объясни свою позицию, а не устраивай конфликт.

8. 🧠 <b>Адекватность</b>
Неважно, насколько хорошо ты играешь. Если от тебя постоянно проблемы — место в составе не гарантируется.

9. 🕵️ <b>Никакого намеренного саботажа</b>
Не мешаем КВ, тренировкам и другим участникам специально.

10. 🔥 <b>Главное правило</b>
Не будь просто цифрой в составе. Будь частью «Шлюх надзор».

⚠️ <b>За нарушение:</b>
1 предупреждение → ограничение → исключение.
Тяжёлые нарушения могут привести к мгновенному исключению.

⏳ Варны хранятся <b>7 дней</b>.
🔇 При <b>5 активных предупреждениях</b> участнику запрещается писать в чате."""







# ---- Replace guest entry point with the final guest panel ----


# ---- Applications ----





KV_ROSTER_NAMES = {1: "Телефоншики", 2: "ПК", 3: "ПК + Телефон"}





















# ---- Guest KV proposal FSM ----




# ---- Admin KV creation / management ----



def _format_kv_scheduled(row):
    enemy=", ".join(_json4(row["enemy_members"])) or "—"
    ours=", ".join(_json4(row["our_members"])) or "—"
    return (
        "⚔️ <b>КВ НАЗНАЧЕНО</b>\n\n"
        f"🏰 Наша гильдия: <b>{html.escape(GUILD_NAME_V71)}</b>\n"
        f"⚔️ Против: <b>{html.escape(row['enemy_guild'] or '—')}</b>\n\n"
        f"👥 Наш состав: {html.escape(ours)}\n"
        f"👥 Состав противника: {html.escape(enemy)}\n\n"
        f"🕐 {html.escape(str(row['match_date'] or '—'))} {html.escape(str(row['match_time'] or '—'))} МСК\n"
        f"🆔 КВ #{row['id']}"
    )

@dp.message(Command("kv_scheduled","назначенныекв","назначенные","назначеные"))
async def command_kv_scheduled(message: Message):
    rows=db.get_kvs(status="planned",limit=20)
    if not rows:
        await message.answer("📭 Назначенных КВ пока нет.")
        return
    await message.answer("\n\n".join(_format_kv_scheduled(r) for r in rows))

@dp.message(Command("kv_history","историякв"))
async def command_kv_history(message: Message):
    rows=db.get_kv_history(limit=20)
    if not rows:
        await message.answer("📚 История КВ пока пуста.")
        return
    wins=sum(1 for r in rows if str(r["result"]).lower()=="победа")
    losses=sum(1 for r in rows if str(r["result"]).lower()=="поражение")
    out=["📚 <b>ИСТОРИЯ КВ</b>",""]
    for r in rows:
        label="Победа" if str(r["result"]).lower()=="победа" else "Поражение"
        out.append(
            f"<b>{label} {html.escape(str(r['match_date']))}</b>\n"
            f"Счёт\n"
            f"наша гильдия    противник\n"
            f"{int(r['our_score'])}                 {int(r['enemy_score'])}\n"
        )
    out.append(f"🏆 <b>Общий:</b> Поражений {losses} · Побед {wins}")
    await message.answer("\n".join(out))

@dp.message(Command("kvresult","кврезультат"))
async def command_kv_result(message: Message):
    if not management_admin(message.from_user.id):
        await message.answer("🔒 Только руководство (6–8)."); return
    parts=(message.text or "").split()
    if len(parts)!=4 or not all(x.lstrip("-").isdigit() for x in parts[1:]):
        await message.answer("Формат: <code>кв результат ID НАШ_СЧЁТ СЧЁТ_ПРОТИВНИКА</code>"); return
    kid, our_score, enemy_score=map(int,parts[1:])
    row=db.conn.execute("SELECT * FROM kv_matches WHERE id=?",(kid,)).fetchone()
    if not row:
        await message.answer("❌ КВ не найдено."); return
    result="победа" if our_score>enemy_score else "поражение" if our_score<enemy_score else "ничья"
    db.add_kv_history(kid, datetime.now(ZoneInfo(TIMEZONE)).date().isoformat(), our_score, enemy_score, result)
    await message.answer(f"✅ Результат КВ #{kid} сохранён: <b>{our_score}:{enemy_score}</b> — {result}.")

@dp.callback_query(F.data == "admin_kv_create")
async def callback_admin_kv_create(callback: CallbackQuery, state: FSMContext):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    await state.clear()
    await state.set_state(KVCreateStates.title)
    await safe_edit(callback.message, "⚔️ <b>СОЗДАНИЕ КВ</b>\n\n1/7. Название КВ:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    await callback.answer()

@dp.callback_query(F.data == "menu_kv_scheduled")
async def callback_menu_kv_scheduled(callback: CallbackQuery):
    rows=db.get_kvs(status="planned",limit=20)
    if not rows:
        await safe_edit(callback.message,"📭 <b>НАЗНАЧЕННЫХ КВ НЕТ</b>",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    else:
        await safe_edit(callback.message,"\n\n".join(_format_kv_scheduled(r) for r in rows),reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    await callback.answer()

@dp.callback_query(F.data == "menu_kv_history")
async def callback_menu_kv_history(callback: CallbackQuery):
    if not management_admin(callback.from_user.id):
        await callback.answer("🔒 Только руководство (6–8).", show_alert=True); return
    rows=db.get_kv_history(limit=20)
    if not rows:
        text="📚 <b>ИСТОРИЯ КВ</b>\n\nПока пуста."
    else:
        wins=sum(1 for r in rows if str(r["result"]).lower()=="победа")
        losses=sum(1 for r in rows if str(r["result"]).lower()=="поражение")
        out=["📚 <b>ИСТОРИЯ КВ</b>",""]
        for r in rows:
            out.append(f"{'Победа' if str(r['result']).lower()=='победа' else 'Поражение'} {r['match_date']}\nСчёт: {r['our_score']} : {r['enemy_score']}\n")
        out.append(f"Общий: Поражений {losses} · Побед {wins}")
        text="\n".join(out)
    await safe_edit(callback.message,text,reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_admin_back()]]))
    await callback.answer()

# ---- Application/KV admin list commands ----


# ---- Participant / admin KV & application panels ----




# ---- SiamBhau API commands ----






# ---- Rule-linked warnings ----

# Rebind the moderation command with explicit rule awareness and the same 7-day/5-warn logic.

# ---- Unified no-slash dispatcher: longest alias first, then exact command token ----
V71_NO_SLASH = {
    "снять мут":"unmute", "снять варн":"unwarn", "снять варны":"unwarn", "снять предупреждения":"unwarn",
    "мой профиль":"profile", "мой акк":"profile", "кто я":"whoami", "кто админ":"admins",
    "чат инфо":"chatinfo", "инфо чата":"chatinfo", "правила гильдии":"rules", "предупреждения":"warnings",
    "кто здесь власть":"admins", "предложить кв":"kvproposal", "создать кв":"kvcreate",
    "гостевая панель":"guest", "гостевая":"guest", "кв предложение":"kvproposal",
    "квшки":"kvs", "браки":"marry", "мой брак":"marry", "развод":"divorce", "развестись":"divorce",
    "заявки":"applications", "админка":"panel", "админ панель":"panel", "коин топ":"coinstop",
    "реферальная ссылка":"ref", "мой баланс":"coins", "активность":"activity", "стата":"stats",
    "статистика":"stats", "рейтинг":"top", "участники":"users", "ники":"users", "помощь":"help",
    "команды":"help", "регистрация":"register", "вступить":"apply", "заявка":"apply", "гость":"guest",
    "панель":"panel", "профиль":"profile", "топ":"top", "история":"history", "неделя":"week",
    "правила":"rules", "рефералы":"ref", "коины":"coins", "монеты":"coins", "магазин":"shop",
    "созыв":"summon", "созвать":"summon", "варн":"warn", "пред":"warn", "мут":"mute", "бан":"ban", "разбан":"unban", "кик":"kick",
    "админы":"admins", "администраторы":"admins", "права":"adminpanel", "роль":"setrole", "повысить":"promote", "понизить":"demote",
    "обновить":"refresh", "логи":"logs", "итого":"total", "удалить":"removeplayer", "отвязать":"unbind",
    "whoami":"whoami", "who":"who", "profile":"profile", "top":"top", "stats":"stats", "users":"users", "rules":"rules",
    "help":"help", "coins":"coins", "shop":"shop", "ff":"ff", "register":"register", "summon":"summon",
    "warn":"warn", "mute":"mute", "unmute":"unmute", "unwarn":"unwarn", "ban":"ban", "unban":"unban", "kick":"kick",
    "marry":"marry", "divorce":"divorce", "kv":"kv", "kvs":"kvs", "guest":"guest", "apply":"apply", "applications":"applications",
}
V71_FN = {
    "unmute":command_unmute,"unwarn":command_unwarn,"profile":command_profile,"whoami":command_whoami,"admins":command_admins,
    "chatinfo":command_chatinfo,"rules":command_rules,"warnings":command_warnings,"kv":command_kv,"kvs":command_kv,"marry":command_marry,
    "divorce":command_divorce,"guest":command_guest_v71,"apply":command_apply_v71,"applications":command_applications_v71,"panel":command_panel,
    "coinstop":command_coin_top,"ref":command_ref,"coins":command_coins,"activity":command_activity,"stats":command_stats,"top":command_top,
    "users":command_users,"help":command_help,"register":command_register,"history":command_history,"week":command_week,"shop":command_shop,
    "summon":command_summon,"warn":command_warn_v71,"mute":command_mute,"ban":command_ban,"unban":command_unban,"kick":command_kick,
    "adminpanel":command_rank_adminpanel,"setrole":command_setrole,"promote":command_promote,"demote":command_demote,"refresh":command_refresh,
    "logs":command_logs,"total":command_total_activity,"removeplayer":command_removeplayer,"unbind":command_unbind,"who":command_who,"ff":command_ff,
    "yesno":command_yesno,"random":command_random,"choose":command_choose,"ping":command_ping,"chatinfo":command_chatinfo,"kvcreate":command_kvcreate_v71,
}

@dp.message(StateFilter(None), F.text.regexp(r"^(?!/).+$", flags=re.S))
async def v71_no_slash_fallback(message: Message, state: FSMContext):
    # Do not steal messages from an active FSM state.
    current = await state.get_state()
    if current: return
    text=(message.text or "").strip()
    # Iris accepts !, . and the word "Ирис" as command prefixes; keep the same
    # behavior without disturbing Vaka slash commands.
    normalized=re.sub(r"^(?:!|\.|Ирис\s+)","",text,flags=re.I).strip()
    low=normalized.lower()
    for phrase in sorted(list(RP_ACTIONS_V71)+list(RP_ALIASES_V71), key=len, reverse=True):
        if low == phrase or low.startswith(phrase+" "):
            action=RP_ALIASES_V71.get(phrase,phrase)
            rest=normalized[len(phrase):].strip()
            target=message.reply_to_message.from_user if message.reply_to_message else None
            if not target:
                m=re.search(r"@([A-Za-z0-9_]{3,})",rest)
                if m:
                    try: target=(await bot.get_chat_member(message.chat.id,"@"+m.group(1))).user
                    except Exception: target=None
            await _run_rp(message,action,target); return
    cmd=None
    for phrase in sorted(V71_NO_SLASH,key=len,reverse=True):
        if low==phrase or low.startswith(phrase+" "):
            cmd=V71_NO_SLASH[phrase]; original=text[len(phrase):].strip(); break
    if not cmd: return
    if cmd=="kvproposal":
        await state.set_state(KVProposalStates.guild); await message.answer("⚔️ <b>Предложение КВ</b>\n\n1/5. Название вашей гильдии:"); return
    fn=V71_FN.get(cmd)
    if not fn: return
    aliased=message.model_copy(update={"text":"/"+cmd+(" "+original if original else "")})
    await fn(aliased)


# Additive completion of the unified no-slash command registry. Nothing above is removed.
V71_NO_SLASH.update({
    "правила кв": "kvrules", "кв правила": "kvrules", "kv rules": "kvrules",
    "правила гильдии": "guildrules", "guild rules": "guildrules", "гильдия правила": "guildrules",
    "главная": "panel", "основная панель": "panel", "main": "panel", "main panel": "panel",
    "рег": "register", "registration": "register",
    "join": "apply", "join guild": "apply",
    "marriages": "marry", "statistics": "stats", "achievements": "achievements",
    "progress": "progress", "streak": "streak", "tournaments": "tournaments",
    "referrals": "ref", "coin top": "coinstop", "coinstop": "coinstop",
    "ffstats": "ffstats", "ffoutfit": "ffoutfit", "guildinfo": "guildinfo",
    "bancheck": "bancheck", "ffapi": "ffapi", "rp": "rp", "rpcommands": "rpcommands",
    "админ панель": "adminpanel", "admin panel": "adminpanel",
})
V71_FN.update({
    "achievements": command_achievements, "progress": command_progress, "streak": command_streak,
    "tournaments": command_tournaments, "kvrules": command_kv_rules, "guildrules": command_guild_rules,
    "coinstop": command_coin_top, "ffstats": command_ffstats, "ffoutfit": command_ffoutfit,
    "guildinfo": command_guildinfo, "bancheck": command_bancheck, "ffapi": command_ffapi,
})

# Final additive command completion. Existing handlers stay intact.
V71_NO_SLASH.update({
    "сказать всем":"sayall", "say all":"sayall", "sayall":"sayall",
    "стоп чат":"stopchat", "stop chat":"stopchat", "stopchat":"stopchat",
    "запуск чат":"startchat", "start chat":"startchat", "startchat":"startchat",
    "снять все ограничения":"clearall", "убрать все ограничения":"clearall",
    "clear all restrictions":"clearall", "clearall":"clearall",
    "моя статистика":"mystats", "моя стата":"mystats", "my stats":"mystats", "mystats":"mystats",
    "моя активность":"myactivity", "my activity":"myactivity", "myactivity":"myactivity",
    "состав кв":"kvrosters", "составы кв":"kvrosters", "kv rosters":"kvrosters", "кв состав":"kvrosters",
    "кто":"who", "кто я":"whoami", "who am i":"whoami", "кто админ":"admins",
    "рег":"register", "регистрация":"register", "register":"register", "registration":"register",
    "панель":"panel", "main panel":"panel", "mainpanel":"panel",
    "команды":"help", "help":"help",
    "команды админов":"admincommands", "admin commands":"admincommands", "admincommands":"admincommands",
    "созыв":"summon", "калл":"summon", "call":"summon", "summon":"summon",
    "призывать всех":"summon", "призвать":"summon",
    "назначенные кв":"kv_scheduled", "назначеные кв":"kv_scheduled", "scheduled kv":"kv_scheduled",
    "история кв":"kv_history", "kv history":"kv_history",
})
V71_FN.update({
    "sayall":command_say_all, "stopchat":command_stop_chat, "startchat":command_start_chat,
    "clearall":command_clear_all_restrictions, "mystats":command_my_stats, "myactivity":command_my_activity,
    "kvrosters":command_kvrosters, "admincommands":command_admin_commands, "summon":command_summon,
    "kv_scheduled":command_kv_scheduled, "kv_history":command_kv_history,
})
UNIFIED_SLASH_ALIASES={
    "сказатьвсем":"sayall", "sayall":"sayall", "стопчат":"stopchat", "stopchat":"stopchat",
    "командыадминов":"admincommands", "admincommands":"admincommands",
    "созыв":"summon", "калл":"summon", "call":"summon", "summon":"summon",
    "назначенные":"kv_scheduled", "историякв":"kv_history",
    "запускчат":"startchat", "startchat":"startchat", "снятьвсеограничения":"clearall",
    "убратьвсеограничения":"clearall", "clearall":"clearall", "моястатистика":"mystats",
    "mystats":"mystats", "мояактивность":"myactivity", "myactivity":"myactivity",
    "составкв":"kvrosters", "kvrosters":"kvrosters",
}

# Complete the public command vocabulary for both languages and both forms.
# These entries are additive and point to the existing handlers whenever one exists.
V71_NO_SLASH.update({
    # Main/user
    "мой профиль":"profile", "my profile":"profile", "мой акк":"profile",
    "моя статистика":"mystats", "моя стата":"mystats", "my stats":"mystats",
    "моя активность":"myactivity", "my activity":"myactivity",
    "профиль":"profile", "profile":"profile", "стата":"stats", "статистика":"stats", "statistics":"stats",
    "топ":"top", "рейтинг":"top", "top":"top", "участники":"users", "ники":"users", "users":"users",
    "история":"history", "history":"history", "неделя":"week", "week":"week",
    "активность":"activity", "activity":"activity", "помощь":"help", "команды":"help", "help":"help",
    "гость":"guest", "гостевая":"guest", "гостевая панель":"guest", "guest":"guest", "guestpanel":"guest",
    "рег":"register", "регистрация":"register", "зарегистрироваться":"register", "register":"register", "registration":"register",
    "заявка":"apply", "вступить":"apply", "join":"apply", "join guild":"apply",
    # Rules / KV / applications
    "правила":"rules", "rules":"rules", "правила гильдии":"guildrules", "guild rules":"guildrules",
    "правила кв":"kvrules", "кв правила":"kvrules", "kv rules":"kvrules",
    "кв":"kv", "kvs":"kvs", "kv":"kv", "квшки":"kvs",
    "предложить кв":"kvproposal", "кв предложение":"kvproposal", "создать кв":"kvcreate",
    "создатькв":"kvcreate", "состав кв":"kvrosters", "составы кв":"kvrosters", "kv rosters":"kvrosters",
    "заявки":"applications", "applications":"applications",
    # Marriage
    "брак":"marry", "marry":"marry", "браки":"marry", "marriages":"marry",
    "развод":"divorce", "развестись":"divorce", "divorce":"divorce",
    # Moderation / management
    "варн":"warn", "пред":"warn", "предупреждение":"warn", "warn":"warn",
    "мут":"mute", "mute":"mute", "бан":"ban", "ban":"ban", "разбан":"unban", "unban":"unban",
    "кик":"kick", "kick":"kick", "снять мут":"unmute", "размут":"unmute", "unmute":"unmute",
    "снять варн":"unwarn", "снять варны":"unwarn", "снять предупреждения":"unwarn", "unwarn":"unwarn",
    "преды":"warnings", "предупреждения":"warnings", "наказания":"warnings", "warnings":"warnings", "warns":"warnings",
    "созыв":"summon", "созвать":"summon", "call":"summon", "summon":"summon",
    "очистить чат":"clear", "очистить":"clear", "чистка чата":"clear", "clear":"clear", "clean":"clear",
    "сказать всем":"sayall", "say all":"sayall", "sayall":"sayall",
    "стоп чат":"stopchat", "stop chat":"stopchat", "stopchat":"stopchat",
    "запуск чат":"startchat", "start chat":"startchat", "startchat":"startchat",
    "снять все ограничения":"clearall", "убрать все ограничения":"clearall", "clear all restrictions":"clearall", "clearall":"clearall",
    "админка":"adminpanel", "админ панель":"adminpanel", "admin panel":"adminpanel", "adminpanel":"adminpanel", "права":"adminpanel",
    "админы":"admins", "администраторы":"admins", "кто админ":"admins", "whoadmins":"admins", "admins":"admins",
    "роль":"setrole", "назначить роль":"setrole", "setrole":"setrole", "повысить":"promote", "promote":"promote",
    "понизить":"demote", "demote":"demote", "итого":"total", "total":"total",
    "добавить":"adduser", "adduser":"adduser", "добавить списком":"addlist", "addlist":"addlist",
    "отвязать":"unbind", "unbind":"unbind", "удалить":"removeplayer", "removeplayer":"removeplayer",
    "обновить":"refresh", "refresh":"refresh", "логи":"logs", "logs":"logs",
    # Coins/shop/activity admin
    "коины":"coins", "монеты":"coins", "мой баланс":"coins", "coins":"coins",
    "магазин":"shop", "shop":"shop", "добавить товар":"shopadd", "shopadd":"shopadd",
    "удалить товар":"shopremove", "shopremove":"shopremove", "setcoins":"setcoins", "выдать коины":"setcoins",
    "товары магазина":"shopproducts", "shopproducts":"shopproducts", "заявки магазина":"shoprequests", "shoprequests":"shoprequests",
    "реферал":"ref", "рефералы":"ref", "реферальная ссылка":"ref", "ref":"ref", "referral":"ref",
    "достижения":"achievements", "achievements":"achievements", "прогресс":"progress", "progress":"progress",
    "серия":"streak", "streak":"streak", "турниры":"tournaments", "tournaments":"tournaments",
    "коин топ":"coinstop", "coin top":"coinstop", "coinstop":"coinstop",
    "установить активность":"set", "set":"set", "публикация":"publish", "publish":"publish",
    # FF/API/utilities
    "ff":"ff", "free fire":"ff", "ffstats":"ffstats", "статфф":"ffstats", "ffoutfit":"ffoutfit",
    "outfit":"ffoutfit", "образ":"ffoutfit", "guildinfo":"guildinfo", "инфо гильдии":"guildinfo", "гильдия":"guildinfo",
    "bancheck":"bancheck", "проверка бана":"bancheck", "ffapi":"ffapi", "siambhau":"ffapi",
    "кто я":"whoami", "whoami":"whoami", "кто":"who", "who":"who", "whois":"who",
    "ид":"userid", "айди":"userid", "userid":"userid", "пинг":"ping", "ping":"ping",
    "чат инфо":"chatinfo", "инфо чата":"chatinfo", "chatinfo":"chatinfo", "рандом":"random", "random":"random",
    "выбери":"choose", "choose":"choose", "данет":"yesno", "yesno":"yesno",
    # RP controls
    "рп":"rp", "rp":"rp", "рп команды":"rpcommands", "rp commands":"rpcommands", "rpcommands":"rpcommands",
    "+рп":"rp_on", "рпон":"rp_on", "rp_on":"rp_on", "-рп":"rp_off", "рпоф":"rp_off", "rp_off":"rp_off",
    # Greetings
    "привет":"greet", "greet":"greet",
})
V71_FN.update({
    "shopadd":command_shopadd, "shopremove":command_shopremove, "setcoins":command_setcoins,
    "shopproducts":command_shopproducts, "shoprequests":command_shoprequests, "greet":command_greet,
    "set":command_set_activity, "publish":command_publish,
    "addlist":command_addlist, "adduser":command_adduser, "userid":command_user_id,
})
V71_NO_SLASH.update({
    "h":"help", "pan":"panel", "prof":"profile", "t":"top", "st":"stats", "hist":"history",
    "us":"users", "act":"activity", "wk":"week", "pub":"publish", "ach":"achievements", "prog":"progress",
    "coin_top":"coinstop", "coin":"coins", "tournament":"tournaments", "clearwarnings":"unwarn",
    "id":"userid", "administrators":"admins", "join_guild":"apply", "kvcreate":"kvcreate", "квсоздать":"kvcreate",
    "applications_list":"applications", "kvrules":"kvrules", "kv_rules":"kvrules", "правилакв":"kvrules",
    "правила_кв":"kvrules", "квправила":"kvrules", "guildrules":"guildrules", "guild_rules":"guildrules",
    "правилагильдии":"guildrules", "правила_гильдии":"guildrules", "ffstat":"ffstats",
    "гильдияinfo":"guildinfo", "проверкабана":"bancheck", "рпкоманды":"rpcommands",
})
UNIFIED_SLASH_ALIASES.update({
    "панель":"panel", "mainpanel":"panel", "главная":"panel", "main":"panel",
    "профиль":"profile", "мойпрофиль":"profile", "стата":"stats", "статистика":"stats", "statistics":"stats",
    "моястатистика":"mystats", "mystats":"mystats", "мояактивность":"myactivity", "myactivity":"myactivity",
    "правила":"rules", "правилагильдии":"guildrules", "guildrules":"guildrules", "правилакв":"kvrules", "kvrules":"kvrules",
    "рег":"register", "регистрация":"register", "registration":"register", "заявка":"apply", "вступить":"apply", "join":"apply",
    "кв":"kv", "kvs":"kvs", "браки":"marry", "marriages":"marry", "брак":"marry", "marry":"marry", "развод":"divorce", "divorce":"divorce",
    "квсоздать":"kvcreate", "создатькв":"kvcreate", "составкв":"kvrosters", "kvrosters":"kvrosters",
    "sayall":"sayall", "сказатьвсем":"sayall", "stopchat":"stopchat", "стопчат":"stopchat",
    "startchat":"startchat", "запускчат":"startchat", "clearall":"clearall", "снятьвсеограничения":"clearall", "убратьвсеограничения":"clearall",
    "mystats":"mystats", "myactivity":"myactivity", "команды":"help", "помощь":"help", "help":"help",
})


# =========================================================
# IRIS COMPATIBILITY MODULE (allowed modules only)
# =========================================================
# The public Iris documentation describes RP actions, moderation, command
# access, chat settings, anti-spam/SCAM, VIP bonuses, marriage, reputation,
# rewards, bookmarks/notes/timers/catalog/exchange, map, and mini-games.
# The project requirements explicitly exclude the latter storage/economy
# modules listed by the guild owner. This compatibility layer therefore adds
# the requested RP engine, RP toggle, custom RP commands and Iris-style
# aliases without removing Vaka's native systems.

IRIS_RP_HELP = (
    "🎭 <b>RP-КОМАНДЫ VAKA / IRIS</b>\n\n"
    "Можно написать команду без /, через !, . или ответом на сообщение.\n"
    "Пример: <code>Ударить</code> в ответ на сообщение пользователя.\n\n"
    "Используй <code>рп команды</code>, чтобы увидеть доступные действия.\n"
    "<code>+рп</code> / <code>-рп</code> — включить/выключить RP в чате.\n"
    "<code>+мрп название / текст</code> — создать личное RP-действие.\n"
    "<code>мрп</code> — список своих RP-действий."
)

@dp.message(Command("rp", "рп", "rpcommands", "рпкоманды"))
async def iris_rp_help(message: Message):
    names=sorted(RP_ACTIONS.keys())
    await message.answer(IRIS_RP_HELP+"\n\n"+" • ".join(names))

@dp.message(Command("rp_on", "рпон"))
async def iris_rp_on(message: Message):
    if message.chat.type == "private":
        await message.answer("🎭 RP в личных сообщениях доступен всегда."); return
    if not moderation_admin(message.from_user.id):
        await message.answer("🔒 Только администрация."); return
    db.iris_set_rp(message.chat.id, True); await message.answer("🎭 RP-команды включены.")

@dp.message(Command("rp_off", "рпоф"))
async def iris_rp_off(message: Message):
    if message.chat.type == "private":
        await message.answer("🎭 RP в личных сообщениях отключить нельзя."); return
    if not moderation_admin(message.from_user.id):
        await message.answer("🔒 Только администрация."); return
    db.iris_set_rp(message.chat.id, False); await message.answer("🎭 RP-команды отключены.")

# Generic Iris-style command aliases for the already implemented Vaka moderation.
IRIS_ALIAS_MAP={
    "кто админ":"/admins","а судьи кто":"/admins","кто здесь власть":"/admins",
    "варны":"/warnings","банлист":"/banlist","разбан":"/unban","кик":"/kick",
    "мут":"/mute","-мут":"/unmute","снять варн":"/unwarn",
    "анкета":"/profile","мой профиль":"/profile","статистика":"/stats","ид":"/id",
    "брак":"/marry","развод":"/divorce","кв":"/kv","квшки":"/kvs",
}

@dp.message(F.text.regexp(r"^(?:!|\.)", flags=re.S))
async def iris_prefix_aliases(message: Message, state: FSMContext):
    # Prefix compatibility: !команда and .команда. We only dispatch aliases that
    # already have a Vaka handler; unknown Iris modules are not fabricated.
    current=await state.get_state()
    if current: return
    raw=(message.text or "").strip()[1:].strip()
    if not raw: return
    low=raw.lower()
    if low in IRIS_ALIAS_MAP:
        cmd=IRIS_ALIAS_MAP[low]
        name=cmd[1:].split()[0]
        fn=V71_FN.get(name)
        if fn:
            await fn(message.model_copy(update={"text":cmd})); return
    # RP prefixes are handled by the same engine.
    registry = globals().get("ALL_RP_ACTIONS", RP_ACTIONS_V71)
    aliases = globals().get("ALL_RP_ALIASES", RP_ALIASES_V71)
    for phrase in sorted(set(registry) | set(aliases), key=len, reverse=True):
        if low==phrase or low.startswith(phrase+" "):
            action=aliases.get(phrase,phrase); rest=raw[len(phrase):].strip()
            target=message.reply_to_message.from_user if message.reply_to_message else None
            if not target:
                m=re.search(r"@([A-Za-z0-9_]{3,})",rest)
                if m:
                    try: target=(await bot.get_chat_member(message.chat.id,"@"+m.group(1))).user
                    except Exception: target=None
            await _run_rp(message,action,target); return


# Complete no-slash RP command aliases. This supplements the existing RP action engine
# and does not remove or replace any legacy/18+ action.
V71_NO_SLASH.update({
    "рп": "rp", "rp": "rp", "рп команды": "rpcommands", "rp commands": "rpcommands",
    "+рп": "rp_on", "рпон": "rp_on", "-рп": "rp_off", "рпоф": "rp_off",
})
V71_FN.update({
    "rp": iris_rp_help, "rpcommands": iris_rp_help, "rp_on": iris_rp_on, "rp_off": iris_rp_off,
})

# =========================================================
# VAKA 8.1.8 — IRIS EXTRAS (additive, no legacy removal)
# =========================================================

# Safe RP vocabulary. Explicit sexual/sexual-violence actions are intentionally
# not implemented. Existing Vaka RP actions remain untouched.
IRIS_EXTRA_RP = {
    "записать на ноготочки": ("💅", "записал на ноготочки"),
    "дать пять": ("✋", "дал пять"),
    "испугать": ("😱", "испугал"),
    "извиниться": ("🙏", "извинился перед"),
    "обнять": ("🤗", "обнял"),
    "отравить": ("☠️", "отравил"),
    "поздравить": ("🎉", "поздравил"),
    "прижать": ("🫂", "прижал"),
    "потрогать": ("👉", "потрогал"),
    "пожать руку": ("🤝", "пожал руку"),
    "послать нахуй": ("🖕", "послал нахуй"),
    "похвалить": ("👏", "похвалил"),
    "понюхать": ("👃", "понюхал"),
    "погладить": ("🫳", "погладил"),
    "пригласить на чаёк": ("☕", "пригласил на чаёк"),
    "пнуть": ("🦵", "пнул"),
    "покормить": ("🍕", "покормил"),
    "расстрелять": ("🔫", "расстрелял"),
    "сжечь": ("🔥", "сжёг"),
    "ущипнуть": ("🤏", "ущипнул"),
    "ударить": ("🤜", "ударил"),
    "убить": ("💀", "убил"),
    "шлепнуть": ("🖐️", "шлёпнул"),
    "пригласить на чай": ("☕", "пригласил на чай"),
    "укусить": ("🦷", "укусил"),
    "облизать": ("👅", "облизал"),
    "лизнуть": ("👅", "лизнул"),
    # For safety, "кастрировать" and similar sexualized violence are omitted.
    "сделать большой подарок": ("🎁", "сделал большой подарок для"),
    "устроить сюрприз": ("🎁", "устроил сюрприз для"),
    "пригласить в клуб": ("🎵", "пригласил в клуб"),
    "поговорить по душам": ("💬", "поговорил по душам с"),
    "сходить в кино": ("🎬", "сходил в кино с"),
    "подарить конфеты": ("🍬", "подарил конфеты"),
    "сделать завтрак": ("🥞", "сделал завтрак для"),
    "пригласить погулять": ("🌳", "пригласил погулять"),
    "подарить шоколадку": ("🍫", "подарил шоколадку"),
    "обнимать": ("🤗", "обнял"),
    "поговорить": ("💬", "поговорил с"),
    "кинуть мем": ("😂", "кинул мем"),
    "поделиться едой": ("🍽️", "поделился едой с"),
    "рассказать анекдот": ("😄", "рассказал анекдот"),
    "сделать комплимент": ("✨", "сделал комплимент"),
}

IRIS_EXTRA_ALIASES = {
    "обними":"обнять", "обнимашки":"обнять", "поцелуй":"поцеловать",
    "поцелуйчик":"поцеловать", "чмок":"поцеловать", "куснуть":"укусить",
    "кусь":"укусить", "лизь":"лизнуть", "дай пять":"дать пять",
    "пятюню":"дать пять", "шлеп":"шлепнуть", "шлёп":"шлепнуть",
}

def _extra_rp_parse(text):
    raw=(text or "").strip()
    raw=re.sub(r"^(?:[/!.]|Ирис\s+)","",raw,flags=re.I).strip()
    low=raw.lower()
    vocab={**IRIS_EXTRA_RP, **{k: IRIS_EXTRA_RP.get(v) for k,v in IRIS_EXTRA_ALIASES.items()}}
    for phrase in sorted(vocab,key=len,reverse=True):
        if low==phrase or low.startswith(phrase+" "):
            canonical=IRIS_EXTRA_ALIASES.get(phrase,phrase)
            return canonical, raw[len(phrase):].strip()
    return None, ""

async def _extra_rp_target(message, rest):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    m=re.search(r"@([A-Za-z0-9_]{3,})", rest or "")
    if m:
        try:
            row=db.get_player_by_telegram_username(m.group(1))
            if row and row["telegram_id"]:
                return type("RPUser",(),{"id":int(row["telegram_id"]),"full_name":row["nick"] or row["telegram_username"] or m.group(1)})()
        except Exception:
            pass
        try:
            return (await bot.get_chat("@"+m.group(1)))
        except Exception:
            pass
    return None


# =========================================================
# V8.1.13 — UNIFIED RP REGISTRY
# =========================================================
# IMPORTANT: do not replace/clear legacy RP dictionaries. We merge them.
# This keeps user-added entries intact and makes IRIS_EXTRA_RP actually
# visible to the authoritative dispatcher.

# =========================================================
# ЗАГРУЗКА БОЛЬШОГО СПИСКА ИЗ ФАЙЛА СПИСОК.txt
# =========================================================
# ВСЕ ДЕЙСТВИЯ ИЗ ТВОЕГО СПИСКА УЖЕ ЗДЕСЬ
# Я ИХ ПРЯМО ВСТАВИЛ В ЭТОТ БЛОК
# =========================================================

IRIS_EXTRA_RP = {
    # === Обычные действия ===
    "избить": ("💅", "сломал ебало"),
    "послать нахуй": ("✋", "послал куда по дальше"),
    "унизить": ("😱", "сделал петушком"),
    "записать на ноготочки": ("💅", "записал на ноготочки"),
    "дать пять": ("✋", "дал пять"),
    "испугать": ("😱", "испугал"),
    "извиниться": ("🙏", "извинился перед"),
    "обнять": ("🤗", "обнял"),
    "поздравить": ("🎉", "поздравил"),
    "прижать": ("🫂", "прижал"),
    "потрогать": ("👉", "потрогал"),
    "пожать руку": ("🤝", "пожал руку"),
    "похвалить": ("👏", "похвалил"),
    "понюхать": ("👃", "понюхал"),
    "погладить": ("🫳", "погладил"),
    "пригласить на чаёк": ("☕", "пригласил на чаёк"),
    "пнуть": ("🦵", "пнул"),
    "покормить": ("🍕", "покормил"),
    "сжечь": ("🔥", "сжёг"),
    "ущипнуть": ("🤏", "ущипнул"),
    "ударить": ("🤜", "ударил"),
    "шлепнуть": ("🖐️", "шлёпнул"),
    "пригласить на чай": ("☕", "пригласил на чай"),
    "укусить": ("🦷", "укусил"),
    "облизать": ("👅", "облизал"),
    "лизнуть": ("👅", "лизнул"),
    "сделать большой подарок": ("🎁", "сделал большой подарок для"),
    "устроить сюрприз": ("🎁", "устроил сюрприз для"),
    "пригласить в клуб": ("🎵", "пригласил в клуб"),
    "поговорить по душам": ("💬", "поговорил по душам с"),
    "сходить в кино": ("🎬", "сходил в кино с"),
    "подарить конфеты": ("🍬", "подарил конфеты"),
    "сделать завтрак": ("🥞", "сделал завтрак для"),
    "пригласить погулять": ("🌳", "пригласил погулять"),
    "подарить шоколадку": ("🍫", "подарил шоколадку"),
    "обнимать": ("🤗", "обнял"),
    "поговорить": ("💬", "поговорил с"),
    "кинуть мем": ("😂", "кинул мем"),
    "поделиться едой": ("🍽️", "поделился едой с"),
    "рассказать анекдот": ("😄", "рассказал анекдот"),
    "сделать комплимент": ("✨", "сделал комплимент"),

    # === ВЕСЬ ТВОЙ 18+ СПИСОК ===
    "выебать": ("🔞", "выебал"),
    "трахнуть": ("🔞", "трахнул"),
    "секс": ("🔞", "занялся сексом с"),
    "выебать в жопу": ("🔞", "выебал в жопу"),
    "порвать очко": ("🤩", "порвал задний проход"),
    "отсосать": ("🔞", "отсосал"),
    "отлизать": ("🔞", "отлизал"),
    "подрочить": ("🔞", "сделал приятно"),
    "изнасиловать": ("🔞", "не повредило"),
    "куни": ("🔞", "отлизал"),
    "минет": ("🔞", "отсосал"),
    "69": ("🔞", "занялись сексом в позе 69"),
    "раком": ("🔞", "занялись сексом в позе раком"),
    "низким раком": ("😈", "занялись любовью в любимой позе лидера"),
    "потрахаться": ("🔞", "провели бурную ночь вместе"),
    "раздеть": ("🔞", "оголил"),
    "узнать фетиши": ("🔞", "выяснил тайну"),
    "потрогать сиси": ("🔞", "коснулся божественного дара"),
    "потрогать хуй": ("🔞", "коснулся хуя (не уверен, что он там был)"),
    "потрогать писю": ("🔞", "коснулся сокровенного"),
    "принудить": ("🔞", "занялся сексом по принуждению"),
    "БДСМ": ("🔞", "принудил к интиму"),
    "стать наездницей": ("🔞", "сел(а) на хуй"),
    "Наездница": ("🔞", "выебал в позе наездницы"),
    "оргазм": ("🔞", "довёл до оргазма"),
    "снять трусы": ("🔞", "снял трусы"),
    "снять лифчик": ("🔞", "расстегнул лифчик"),
    "поцеловать в шею": ("🔞", "поцеловал в шею"),
    "оставить засос": ("🔞", "оставил засос"),
    "кусать губы": ("🔞", "покусал губы"),
    "вставить член": ("🔞", "вставил член"),
    "войти в писю": ("🔞", "вошёл в писю"),
    "войти в жопу": ("🔞", "вошёл в жопу"),
    "кончить": ("🔞", "кончил"),
    "кончить внутрь": ("🔞", "кончил внутрь"),
    "кончить на лицо": ("🔞", "кончил на лицо"),
    "кончить на грудь": ("🔞", "кончил на грудь"),
    "кончить на спину": ("🔞", "кончил на спину"),
    "кончить на живот": ("🔞", "кончил на живот"),
    "кончить на попку": ("🔞", "кончил на попку"),
    "кончить в рот": ("🔞", "кончил в рот"),
    "кончить в ротик": ("🔞", "кончил в ротик"),
    "сперма": ("🔞", "кончил спермой"),
    "мастурбировать": ("🔞", "мастурбировал"),
    "дрочить": ("🔞", "подрочил"),
    "ласкать": ("🔞", "ласкал"),
    "ласкать грудь": ("🔞", "ласкал грудь"),
    "ласкать клитор": ("🔞", "ласкал клитор"),
    "ласкать соски": ("🔞", "ласкал соски"),
    "ласкать яйца": ("🔞", "ласкал яйца"),
    "ласкать член": ("🔞", "ласкал член"),
    "лизать писю": ("🔞", "лизал писю"),
    "лизать яйца": ("🔞", "лизал яйца"),
    "лизать член": ("🔞", "лизал член"),
    "лизать анус": ("🔞", "лизал анус"),
    "лизать попу": ("🔞", "лизал попу"),
    "сосать член": ("🔞", "сосал член"),
    "сосать яйца": ("🔞", "сосал яйца"),
    "глубокий минет": ("🔞", "сделал глубокий минет"),
    "рукой": ("🔞", "удовлетворил рукой"),
    "руками": ("🔞", "удовлетворил руками"),
    "прелюдия": ("🔞", "занялся прелюдией"),
    "начать": ("🔞", "начал интим"),
    "закончить": ("🔞", "закончил интим"),
    "поменять позу": ("🔞", "поменял позу"),
    "встать раком": ("🔞", "встал раком"),
    "на колени": ("🔞", "встал на колени"),
    "лечь на спину": ("🔞", "лёг на спину"),
    "развести ноги": ("🔞", "развёл ноги"),
    "широко развести ноги": ("🔞", "широко развёл ноги"),
    "задрать юбку": ("🔞", "задрал юбку"),
    "спустить штаны": ("🔞", "спустил штаны"),
    "стянуть трусы": ("🔞", "стянул трусы"),
    "смазка": ("🔞", "использовал смазку"),
    "презерватив": ("🔞", "надел презерватив"),
    "без презерватива": ("🔞", "занялся сексом без презерватива"),
    "грубо": ("🔞", "грубо овладел"),
    "нежно": ("🔞", "нежно прикоснулся"),
    "страстно": ("🔞", "страстно поцеловал"),
    "жёстко": ("🔞", "жёстко трахнул"),
    "медленно": ("🔞", "медленно вошёл"),
    "быстро": ("🔞", "быстро двигался"),
    "громко": ("🔞", "громко стонал"),
    "стон": ("🔞", "заставил стонать"),
    "шлепнуть": ("🔞", "шлепнул по попе"),
    "отшлепать": ("🔞", "отшлепал"),
    "связать": ("🔞", "связал руки"),
    "завязать глаза": ("🔞", "завязал глаза"),
    "кляп": ("🔞", "вставил кляп"),
    "плетка": ("🔞", "ударил плеткой"),
    "наручники": ("🔞", "пристегнул наручниками"),
    "ролевая игра": ("🔞", "сыграл ролевую игру"),
    "господин": ("🔞", "выступил в роли господина"),
    "раб": ("🔞", "выступил в роли раба"),
    "подчинить": ("🔞", "подчинил партнёра"),
    "доминировать": ("🔞", "доминировал"),
    "поддаться": ("🔞", "поддался партнёру"),
    "довести до пика": ("🔞", "довёл до пика"),
    "пик": ("🔞", "достиг пика"),
    "вкус": ("🔞", "почувствовал вкус"),
    "запах": ("🔞", "вдохнул запах"),
    "шептать": ("🔞", "шептал грязные слова"),
    "грязные слова": ("🔞", "говорил грязные слова"),
    "комплимент": ("🔞", "сделал грязный комплимент"),
    "взаимный": ("🔞", "занялся взаимной мастурбацией"),
    "вдвоём": ("🔞", "занялся этим вдвоём"),
    "повторно": ("🔞", "повторил ещё раз"),
    "эякуляция": ("🔞", "вызвал эякуляцию"),
    "влажность": ("🔞", "почувствовал влажность"),
    "возбуждение": ("🔞", "довёл до возбуждения"),
    "прелюбодеяние": ("🔞", "совершил прелюбодеяние"),
    "изменить": ("🔞", "изменил партнёру"),
    "соблазнить": ("🔞", "соблазнил"),
    "соблазнение": ("🔞", "занялся соблазнением"),
    "флирт": ("🔞", "занялся флиртом"),
    "разврат": ("🔞", "занялся развратом"),
    "пошлость": ("🔞", "сказал пошлость"),
    "непристойность": ("🔞", "совершил непристойность"),
    "интим": ("🔞", "занялся интимом"),
    "близость": ("🔞", "приблизился к партнёру"),
    "тело": ("🔞", "коснулся тела"),
    "половой орган": ("🔞", "коснулся полового органа"),
    "возбудиться": ("🔞", "возбудился"),
    "возбуждённый": ("🔞", "был возбуждён"),
    "мокрый": ("🔞", "стал мокрым"),
    "твёрдый": ("🔞", "стал твёрдым"),
    "влажный": ("🔞", "почувствовал влажность"),
    "горячий": ("🔞", "был горячим"),
    "температура": ("🔞", "почувствовал тепло тела"),
    "дрожь": ("🔞", "вызвал дрожь"),
    "мурашки": ("🔞", "вызвал мурашки"),
    "экстаз": ("🔞", "довёл до экстаза"),
    "блаженство": ("🔞", "доставил блаженство"),
    "наслаждение": ("🔞", "доставил наслаждение"),
    "удовольствие": ("🔞", "доставил удовольствие"),
    "сладострастие": ("🔞", "предался сладострастию"),
    "похоть": ("🔞", "предался похоти"),
    "страсть": ("🔞", "предался страсти"),
    "анал": ("🔞", "занялся анальным сексом с"),
    "анальный секс": ("🔞", "занялся анальным сексом с"),
    "оральный секс": ("🔞", "занялся оральным сексом с"),
    "минет делать": ("🔞", "сделал минет"),
    "кунилингус": ("🔞", "сделал кунилингус"),
    "сосать": ("🔞", "сосал"),
    "лизать": ("🔞", "лизал"),
    "трахать": ("🔞", "трахал"),
    "ебать": ("🔞", "ебал"),
    "жёсткий секс": ("🔞", "занялся жёстким сексом с"),
    "групповой секс": ("🔞", "занялся групповым сексом с"),
    "оргия": ("🔞", "участвовал в оргии с"),
    "ласки": ("🔞", "занимался ласками с"),
    "нежные ласки": ("🔞", "нежно ласкал"),
    "страстные ласки": ("🔞", "страстно ласкал"),
    "играть с сосками": ("🔞", "играл с сосками"),
    "сосать грудь": ("🔞", "сосал грудь"),
    "лизать грудь": ("🔞", "лизал грудь"),
    "целовать грудь": ("🔞", "целовал грудь"),
    "поцеловать в губы": ("🔞", "поцеловал в губы"),
    "поцеловать страстно": ("🔞", "страстно поцеловал"),
    "поцеловать нежно": ("🔞", "нежно поцеловал"),
    "французский поцелуй": ("🔞", "поцеловал в французском стиле"),
    "поцеловать в лоб": ("🔞", "поцеловал в лоб"),
    "поцеловать в руку": ("🔞", "поцеловал в руку"),
    "заняться сексом": ("🔞", "занялся сексом с"),
    "заняться любовью": ("🔞", "занялся любовью с"),
    "переспать": ("🔞", "переспал с"),
    "провести ночь": ("🔞", "провёл ночь с"),
    "утренний секс": ("🔞", "занялся утренним сексом с"),
    "вечерний секс": ("🔞", "занялся вечерним сексом с"),
    "секс на кухне": ("🔞", "занялся сексом на кухне с"),
    "секс в душе": ("🔞", "занялся сексом в душе с"),
    "секс в постели": ("🔞", "занялся сексом в постели с"),
    "секс на полу": ("🔞", "занялся сексом на полу с"),
    "секс в машине": ("🔞", "занялся сексом в машине с"),
    "секс на природе": ("🔞", "занялся сексом на природе с"),
    "секс на пляже": ("🔞", "занялся сексом на пляже с"),
    "уединиться": ("🔞", "уединился с"),
    "раздеться": ("🔞", "разделся перед"),
    "оголиться": ("🔞", "оголился перед"),
    "снять": ("🔞", "снял одежду с"),
    "обнажить": ("🔞", "обнажил"),
    "прикоснуться": ("🔞", "прикоснулся к"),
    "дотронуться": ("🔞", "дотронулся до"),
    "погладить по спине": ("🔞", "погладил по спине"),
    "погладить по попе": ("🔞", "погладил по попе"),
    "погладить по бедру": ("🔞", "погладил по бедру"),
    "погладить по ноге": ("🔞", "погладил по ноге"),
    "массаж": ("🔞", "сделал массаж"),
    "эротический массаж": ("🔞", "сделал эротический массаж"),
    "массаж спины": ("🔞", "сделал массаж спины"),
    "массаж ног": ("🔞", "сделал массаж ног"),
    "массаж тела": ("🔞", "сделал массаж тела"),
    "потереть": ("🔞", "потёрся о"),
    "прижаться": ("🔞", "прижался к"),
    "прильнуть": ("🔞", "прильнул к"),
    "обнять страстно": ("🔞", "страстно обнял"),
    "обнять нежно": ("🔞", "нежно обнял"),
    "приобнять": ("🔞", "приобнял"),
    "сжать в объятиях": ("🔞", "сжал в объятиях"),
    "шептать на ухо": ("🔞", "шептал на ухо"),
    "шептать ласково": ("🔞", "шептал ласковые слова"),
    "шептать пошлости": ("🔞", "шептал пошлости"),
    "пошлый разговор": ("🔞", "вёл пошлый разговор с"),
    "грязный разговор": ("🔞", "вёл грязный разговор с"),
    "раздеть взглядом": ("🔞", "раздел взглядом"),
    "смотреть похотливо": ("🔞", "похотливо смотрел на"),
    "соблазнять": ("🔞", "соблазнял"),
    "обольстить": ("🔞", "обольстил"),
    "завлечь": ("🔞", "завлёк"),
    "прельстить": ("🔞", "прельстил"),
    "флиртовать": ("🔞", "флиртовал с"),
    "кокетничать": ("🔞", "кокетничал с"),
    "заигрывать": ("🔞", "заигрывал с"),
    "приставать": ("🔞", "приставал к"),
    "домогаться": ("🔞", "домогался"),
    "прикоснуться интимно": ("🔞", "интимно прикоснулся"),
    "интимная близость": ("🔞", "вступил в интимную близость с"),
    "душевная близость": ("🔞", "сблизился душевно с"),
    "эмоциональная связь": ("🔞", "установил эмоциональную связь с"),
    "страстный поцелуй": ("🔞", "поцеловал страстно"),
    "нежный поцелуй": ("🔞", "поцеловал нежно"),
    "долгий поцелуй": ("🔞", "долго целовал"),
    "глубокий поцелуй": ("🔞", "глубоко поцеловал"),
    "целовать взасос": ("🔞", "целовал взасос"),
    "засос": ("🔞", "оставил засос на"),
    "укус": ("🔞", "укусил"),
    "покусывать": ("🔞", "покусывал"),
    "покусывать губы": ("🔞", "покусывал губы"),
    "покусывать шею": ("🔞", "покусывал шею"),
    "покусывать плечо": ("🔞", "покусывал плечо"),
    "погладить по голове": ("🔞", "погладил по голове"),
    "погладить по щеке": ("🔞", "погладил по щеке"),
    "взять за руку": ("🔞", "взял за руку"),
    "взять за талию": ("🔞", "взял за талию"),
    "обнять за талию": ("🔞", "обнял за талию"),
    "положить руку на бедро": ("🔞", "положил руку на бедро"),
    "положить руку на талию": ("🔞", "положил руку на талию"),
    "положить руку на плечо": ("🔞", "положил руку на плечо"),
    "провести рукой": ("🔞", "провёл рукой по"),
    "провести пальцем": ("🔞", "провёл пальцем по"),
    "провести по спине": ("🔞", "провёл по спине"),
    "провести по груди": ("🔞", "провёл по груди"),
    "провести по животу": ("🔞", "провёл по животу"),
    "провести по ноге": ("🔞", "провёл по ноге"),
    "пройтись рукой": ("🔞", "прошёлся рукой по"),
    "прикоснуться губами": ("🔞", "прикоснулся губами к"),
    "поцеловать в плечо": ("🔞", "поцеловал в плечо"),
    "поцеловать в ключицу": ("🔞", "поцеловал в ключицу"),
    "поцеловать в живот": ("🔞", "поцеловал в живот"),
    "поцеловать в спину": ("🔞", "поцеловал в спину"),
    "поцеловать в поясницу": ("🔞", "поцеловал в поясницу"),
    "поцеловать между лопаток": ("🔞", "поцеловал между лопаток"),
    "поцеловать руку": ("🔞", "поцеловал руку"),
    "поцеловать плечо": ("🔞", "поцеловал плечо"),
    "дразнить": ("🔞", "дразнил"),
    "дразнить поцелуями": ("🔞", "дразнил поцелуями"),
    "дразнить прикосновениями": ("🔞", "дразнил прикосновениями"),
    "сводить с ума": ("🔞", "сводил с ума"),
    "возбуждать": ("🔞", "возбуждал"),
    "заводить": ("🔞", "заводил"),
    "завести": ("🔞", "завёл"),
    "довести до оргазма": ("🔞", "довёл до оргазма"),
    "кончить вместе": ("🔞", "кончил вместе с"),
    "совместный оргазм": ("🔞", "испытал совместный оргазм с"),
    "множественный оргазм": ("🔞", "испытал множественный оргазм"),
    "сильный оргазм": ("🔞", "испытал сильный оргазм"),
    "долгий оргазм": ("🔞", "испытал долгий оргазм"),
    "прерванный половой акт": ("🔞", "прервал половой акт"),
    "быстрый секс": ("🔞", "занялся быстрым сексом с"),
    "долгий секс": ("🔞", "занялся долгим сексом с"),
    "ночной секс": ("🔞", "занялся ночным сексом с"),
    "страстная ночь": ("🔞", "провёл страстную ночь с"),
    "незабываемая ночь": ("🔞", "провёл незабываемую ночь с"),
    "первый раз": ("🔞", "впервые занялся сексом с"),
    "совратить": ("🔞", "совратил"),
    "развратить": ("🔞", "развратил"),
    "испортить": ("🔞", "испортил"),
    "сделать грязно": ("🔞", "сделал грязно с"),
    "грязные игры": ("🔞", "играл в грязные игры с"),
    "пошлые игры": ("🔞", "играл в пошлые игры с"),
    "эротические игры": ("🔞", "играл в эротические игры с"),
    "ролевые игры": ("🔞", "играл в ролевые игры с"),
    "сыграть роль": ("🔞", "сыграл роль"),
    "переодеться": ("🔞", "переоделся для"),
    "надеть костюм": ("🔞", "надел костюм для"),
    "роль медсестры": ("🔞", "сыграл роль медсестры"),
    "роль учителя": ("🔞", "сыграл роль учителя"),
    "роль студента": ("🔞", "сыграл роль студента"),
    "роль полицейского": ("🔞", "сыграл роль полицейского"),
    "роль заключённого": ("🔞", "сыграл роль заключённого"),
    "роль начальника": ("🔞", "сыграл роль начальника"),
    "роль подчинённого": ("🔞", "сыграл роль подчинённого"),
    "роль врача": ("🔞", "сыграл роль врача"),
    "роль пациента": ("🔞", "сыграл роль пациента"),
    "игра в доктора": ("🔞", "играл в доктора с"),
    "игра в школу": ("🔞", "играл в школу с"),
    "игра в тюрьму": ("🔞", "играл в тюрьму с"),
    "игра в полицию": ("🔞", "играл в полицию с"),
    "игра в подчинение": ("🔞", "играл в подчинение с"),
    "игра в господство": ("🔞", "играл в господство с"),
    "доминирование": ("🔞", "доминировал над"),
    "подчинение": ("🔞", "подчинился"),
    "господство": ("🔞", "господствовал над"),
    "порабощение": ("🔞", "поработил"),
    "унижение": ("🔞", "унизил"),
    "оскорбление": ("🔞", "оскорбил"),
    "грубое обращение": ("🔞", "грубо обращался с"),
    "нежное обращение": ("🔞", "нежно обращался с"),
    "бережное обращение": ("🔞", "бережно обращался с"),
    "забота": ("🔞", "проявил заботу о"),
    "нежность": ("🔞", "проявил нежность к"),
    "ласка": ("🔞", "проявил ласку к"),
    "трепет": ("🔞", "трепетно относился к"),
    "поклонение": ("🔞", "поклонялся телу"),
    "обожание": ("🔞", "обожал"),
    "страстное желание": ("🔞", "испытал страстное желание к"),
    "вожделение": ("🔞", "испытал вожделение к"),
    "жажда": ("🔞", "испытал жажду к"),
    "влечение": ("🔞", "испытал влечение к"),
    "притяжение": ("🔞", "испытал притяжение к"),
    "химия": ("🔞", "почувствовал химию с"),
    "искра": ("🔞", "почувствовал искру с"),
    "огонь": ("🔞", "почувствовал огонь с"),
    "чувственность": ("🔞", "предался чувственности с"),
    "чувственный": ("🔞", "был чувственным с"),
    "порочный": ("🔞", "был порочным с"),
    "развратный": ("🔞", "был развратным с"),
    "похотливый": ("🔞", "был похотливым к"),
    "страстный": ("🔞", "был страстным с"),
    "горячий": ("🔞", "был горячим с"),
    "темпераментный": ("🔞", "был темпераментным с"),
    "испанский стыд": ("🔞", "занялся сексом в испанском стиле"),
    "догги-стайл": ("🔞", "занялся сексом в позе догги-стайл"),
    "миссионерская поза": ("🔞", "занялся сексом в миссионерской позе"),
    "поза наездницы": ("🔞", "занялся сексом в позе наездницы"),
    "поза ложек": ("🔞", "занялся сексом в позе ложек"),
    "сидячая поза": ("🔞", "занялся сексом в сидячей позе"),
    "стоячая поза": ("🔞", "занялся сексом в стоячей позе"),
    "поза бабочки": ("🔞", "занялся сексом в позе бабочки"),
    "поза ножницы": ("🔞", "занялся сексом в позе ножницы"),
    "поза колесо": ("🔞", "занялся сексом в позе колесо"),
    "поза мостик": ("🔞", "занялся сексом в позе мостик"),
    "поза лотоса": ("🔞", "занялся сексом в позе лотоса"),
    "амазонка": ("🔞", "занялся сексом в позе амазонки"),
    "ложечки": ("🔞", "занялся сексом в позе ложечек"),
    "верхом": ("🔞", "занялся сексом в позе верховой езды"),
    "лицом к лицу": ("🔞", "занялся сексом лицом к лицу"),
    "сзади": ("🔞", "занялся сексом сзади"),
    "спереди": ("🔞", "занялся сексом спереди"),
    "сбоку": ("🔞", "занялся сексом сбоку"),
    "на боку": ("🔞", "занялся сексом на боку"),
    "на спине": ("🔞", "занялся сексом на спине"),
    "на животе": ("🔞", "занялся сексом на животе"),
    "на коленях": ("🔞", "занялся сексом на коленях"),
    "на четвереньках": ("🔞", "занялся сексом на четвереньках"),
    "стоя": ("🔞", "занялся сексом стоя"),
    "сидя": ("🔞", "занялся сексом сидя"),
    "лёжа": ("🔞", "занялся сексом лёжа"),
    "в воде": ("🔞", "занялся сексом в воде"),
    "под водой": ("🔞", "занялся сексом под водой"),
    "в бассейне": ("🔞", "занялся сексом в бассейне"),
    "в ванной": ("🔞", "занялся сексом в ванной"),
    "в сауне": ("🔞", "занялся сексом в сауне"),
    "в лесу": ("🔞", "занялся сексом в лесу"),
    "в парке": ("🔞", "занялся сексом в парке"),
    "в гостинице": ("🔞", "занялся сексом в гостинице"),
    "на работе": ("🔞", "занялся сексом на работе"),
    "в офисе": ("🔞", "занялся сексом в офисе"),
    "в кабинете": ("🔞", "занялся сексом в кабинете"),
    "в лифте": ("🔞", "занялся сексом в лифте"),
    "в подъезде": ("🔞", "занялся сексом в подъезде"),
    "на крыше": ("🔞", "занялся сексом на крыше"),
    "на балконе": ("🔞", "занялся сексом на балконе"),
    "под звёздами": ("🔞", "занялся сексом под звёздами"),
    "при свечах": ("🔞", "занялся сексом при свечах"),
    "в темноте": ("🔞", "занялся сексом в темноте"),
    "с музыкой": ("🔞", "занялся сексом под музыку"),
    "смотреть порно": ("🔞", "смотрел порно с"),
    "вместе смотреть": ("🔞", "смотрел вместе с"),
    "дрочить вместе": ("🔞", "дрочил вместе с"),
    "взаимная мастурбация": ("🔞", "занялся взаимной мастурбацией с"),
    "показать себя": ("🔞", "показал себя"),
    "показать тело": ("🔞", "показал тело"),
    "показать интим": ("🔞", "показал интимное место"),
    "показать грудь": ("🔞", "показал грудь"),
    "показать член": ("🔞", "показал член"),
    "показать писю": ("🔞", "показал писю"),
    "показать попу": ("🔞", "показал попу"),
    "сфотографировать": ("🔞", "сфотографировал"),
    "снять на видео": ("🔞", "снял на видео"),
    "интимное фото": ("🔞", "сделал интимное фото"),
    "эротическое фото": ("🔞", "сделал эротическое фото"),
    "отправить фото": ("🔞", "отправил интимное фото"),
    "секстинг": ("🔞", "занимался секстингом с"),
    "секс по телефону": ("🔞", "занялся сексом по телефону"),
    "виртуальный секс": ("🔞", "занялся виртуальным сексом с"),
    "онлайн секс": ("🔞", "занялся онлайн сексом с"),
    "вебкам": ("🔞", "занимался вебкамом с"),
    "стриптиз": ("🔞", "устроил стриптиз для"),
    "танцевать эротично": ("🔞", "эротично танцевал для"),
    "эротический танец": ("🔞", "исполнил эротический танец для"),
    "танец живота": ("🔞", "танцевал танец живота для"),
    "танец на шесте": ("🔞", "танцевал на шесте для"),
    "раздеться под музыку": ("🔞", "разделся под музыку для"),
    "медленный танец": ("🔞", "медленно танцевал с"),
    "повальсировать": ("🔞", "вальсировал с"),
    "пригласить на танец": ("🔞", "пригласил на танец"),
    "обнять в танце": ("🔞", "обнял в танце"),
    "прижаться в танце": ("🔞", "прижался в танце"),
    "танцевать в обнимку": ("🔞", "танцевал в обнимку с"),
    "смотреть в глаза": ("🔞", "смотрел в глаза"),
    "заглянуть в глаза": ("🔞", "заглянул в глаза"),
    "смотреть с вожделением": ("🔞", "смотрел с вожделением"),
    "смотреть с желанием": ("🔞", "смотрел с желанием"),
    "смотреть с любовью": ("🔞", "смотрел с любовью"),
    "улыбнуться": ("🔞", "улыбнулся"),
    "загадочная улыбка": ("🔞", "загадочно улыбнулся"),
    "похотливая улыбка": ("🔞", "похотливо улыбнулся"),
    "прикусить губу": ("🔞", "прикусил губу"),
    "облизать губы": ("🔞", "облизал губы"),
    "провести языком": ("🔞", "провёл языком по"),
    "провести языком по губам": ("🔞", "провёл языком по губам"),
    "провести языком по шее": ("🔞", "провёл языком по шее"),
    "вкусный": ("🔞", "насладился вкусом"),
    "сладкий": ("🔞", "насладился сладостью"),
    "солёный": ("🔞", "почувствовал солёный вкус"),
    "ароматный": ("🔞", "вдохнул аромат"),
    "запах духов": ("🔞", "вдохнул запах духов"),
    "запах тела": ("🔞", "вдохнул запах тела"),
    "естественный запах": ("🔞", "почувствовал естественный запах"),
    "феромоны": ("🔞", "почувствовал феромоны"),
    "химия тела": ("🔞", "почувствовал химию тела"),
    "тепло тела": ("🔞", "почувствовал тепло тела"),
    "жар": ("🔞", "почувствовал жар"),
    "озноб": ("🔞", "почувствовал озноб"),
    "дрожь по телу": ("🔞", "почувствовал дрожь по телу"),
    "мурашки по коже": ("🔞", "почувствовал мурашки по коже"),
    "прилив крови": ("🔞", "почувствовал прилив крови"),
    "сердцебиение": ("🔞", "услышал сердцебиение"),
    "учащённое дыхание": ("🔞", "услышал учащённое дыхание"),
    "тяжёлое дыхание": ("🔞", "услышал тяжёлое дыхание"),
    "прерывистое дыхание": ("🔞", "услышал прерывистое дыхание"),
    "стоны": ("🔞", "услышал стоны"),
    "вздохи": ("🔞", "услышал вздохи"),
    "всхлипы": ("🔞", "услышал всхлипы"),
    "крики удовольствия": ("🔞", "услышал крики удовольствия"),
    "стон наслаждения": ("🔞", "услышал стон наслаждения"),
    "громкий стон": ("🔞", "услышал громкий стон"),
    "тихий стон": ("🔞", "услышал тихий стон"),
    "вздох удовольствия": ("🔞", "услышал вздох удовольствия"),
    "вздох облегчения": ("🔞", "услышал вздох облегчения"),
    "выдох": ("🔞", "выдохнул"),
    "вдох": ("🔞", "вдохнул"),
    "задержать дыхание": ("🔞", "задержал дыхание"),
    "сбитое дыхание": ("🔞", "услышал сбитое дыхание"),
    "глубокий вдох": ("🔞", "сделал глубокий вдох"),
    "короткий выдох": ("🔞", "сделал короткий выдох"),
    "пот": ("🔞", "почувствовал пот"),
    "влажная кожа": ("🔞", "почувствовал влажную кожу"),
    "горячая кожа": ("🔞", "почувствовал горячую кожу"),
    "мягкая кожа": ("🔞", "почувствовал мягкую кожу"),
    "гладкая кожа": ("🔞", "почувствовал гладкую кожу"),
    "шёлковая кожа": ("🔞", "почувствовал шёлковую кожу"),
    "бархатная кожа": ("🔞", "почувствовал бархатную кожу"),
    "нежная кожа": ("🔞", "почувствовал нежную кожу"),
    "чувствительная кожа": ("🔞", "почувствовал чувствительную кожу"),
    "мускулы": ("🔞", "почувствовал мускулы"),
    "сильные руки": ("🔞", "почувствовал сильные руки"),
    "крепкие объятия": ("🔞", "почувствовал крепкие объятия"),
    "нежные руки": ("🔞", "почувствовал нежные руки"),
    "тёплые руки": ("🔞", "почувствовал тёплые руки"),
    "холодные руки": ("🔞", "почувствовал холодные руки"),
    "влажные руки": ("🔞", "почувствовал влажные руки"),
    "потные руки": ("🔞", "почувствовал потные руки"),
    "руки на талии": ("🔞", "положил руки на талию"),
    "руки на бёдрах": ("🔞", "положил руки на бёдра"),
    "руки на груди": ("🔞", "положил руки на грудь"),
    "руки на попе": ("🔞", "положил руки на попу"),
    "руки на коленях": ("🔞", "положил руки на колени"),
    "руки на плечах": ("🔞", "положил руки на плечи"),
    "руки на шее": ("🔞", "положил руки на шею"),
    "руки на лице": ("🔞", "положил руки на лицо"),
    "руки на волосах": ("🔞", "положил руки на волосы"),
    "перебирать волосы": ("🔞", "перебирал волосы"),
    "погладить волосы": ("🔞", "погладил волосы"),
    "запутаться в волосах": ("🔞", "запутался в волосах"),
    "взъерошить волосы": ("🔞", "взъерошил волосы"),
    "откинуть волосы": ("🔞", "откинул волосы"),
    "собрать волосы": ("🔞", "собрал волосы"),
    "распустить волосы": ("🔞", "распустил волосы"),
    "заплести косу": ("🔞", "заплёл косу"),
    "сделать причёску": ("🔞", "сделал причёску"),
    "поправить волосы": ("🔞", "поправил волосы"),
    "убрать волосы с лица": ("🔞", "убрал волосы с лица"),
    "заправить за ухо": ("🔞", "заправил за ухо"),
    "потрепать по голове": ("🔞", "потрепал по голове"),
    "поцеловать в макушку": ("🔞", "поцеловал в макушку"),
    "поцеловать в темечко": ("🔞", "поцеловал в темечко"),
    "поцеловать в затылок": ("🔞", "поцеловал в затылок"),
    "поцеловать в висок": ("🔞", "поцеловал в висок"),
    "поцеловать в щёку": ("🔞", "поцеловал в щёку"),
    "поцеловать в нос": ("🔞", "поцеловал в нос"),
    "поцеловать в уголок губ": ("🔞", "поцеловал в уголок губ"),
    "поцеловать в ямочку": ("🔞", "поцеловал в ямочку"),
    "поцеловать в родинку": ("🔞", "поцеловал в родинку"),
    "поцеловать в шрам": ("🔞", "поцеловал в шрам"),
    "поцеловать в тату": ("🔞", "поцеловал тату"),
    "поцеловать в пирсинг": ("🔞", "поцеловал пирсинг"),
    "поцеловать в серьгу": ("🔞", "поцеловал серьгу"),
    "поцеловать в украшение": ("🔞", "поцеловал украшение"),
    "целовать руки": ("🔞", "целовал руки"),
    "целовать пальцы": ("🔞", "целовал пальцы"),
    "целовать ладони": ("🔞", "целовал ладони"),
    "целовать запястья": ("🔞", "целовал запястья"),
    "целовать предплечья": ("🔞", "целовал предплечья"),
    "целовать локти": ("🔞", "целовал локти"),
    "целовать плечи": ("🔞", "целовал плечи"),
    "целовать ключицы": ("🔞", "целовал ключицы"),
    "целовать шею": ("🔞", "целовал шею"),
    "целовать за ухом": ("🔞", "целовал за ухом"),
    "целовать мочку уха": ("🔞", "целовал мочку уха"),
    "целовать ухо": ("🔞", "целовал ухо"),
    "шептать в ухо": ("🔞", "шептал в ухо"),
    "дышать в ухо": ("🔞", "дышал в ухо"),
    "подуть в ухо": ("🔞", "подул в ухо"),
    "укусить за ухо": ("🔞", "укусил за ухо"),
    "лизать ухо": ("🔞", "лизал ухо"),
}

IRIS_EXTRA_ALIASES = {
    "обними":"обнять",
    "обнимашки":"обнять",
    "поцелуй":"поцеловать",
    "поцелуйчик":"поцеловать",
    "чмок":"поцеловать",
    "куснуть":"укусить",
    "кусь":"укусить",
    "лизь":"лизнуть",
    "дай пять":"дать пять",
    "пятюню":"дать пять",
    "шлеп":"шлепнуть",
    "шлёп":"шлепнуть",
}


# --- ОБЪЕДИНЕНИЕ ВСЕХ ДЕЙСТВИЙ В ОДИН СЛОВАРЬ ---
# Сначала копируем существующие RP_ACTIONS
try:
    _LEGACY_RP = dict(RP_ACTIONS)
except Exception:
    _LEGACY_RP = {}

# Создаём единый словарь ВСЕХ действий
ALL_RP_ACTIONS = dict(_LEGACY_RP)

# Добавляем IRIS_EXTRA_RP (твой большой список)
for _k, _v in IRIS_EXTRA_RP.items():
    # Если действие уже есть, не перезаписываем (сохраняем оригинал)
    if _k.lower() not in ALL_RP_ACTIONS:
        ALL_RP_ACTIONS[_k.lower()] = _v

# Добавляем алиасы
try:
    _LEGACY_ALIASES = dict(RP_ALIASES)
except Exception:
    _LEGACY_ALIASES = {}

ALL_RP_ALIASES = {str(k).lower(): str(v).lower() for k, v in _LEGACY_ALIASES.items()}
ALL_RP_ALIASES.update({str(k).lower(): str(v).lower() for k, v in IRIS_EXTRA_ALIASES.items()})

# Приводим все действия к формату (emoji, verb)
for _k, _v in list(ALL_RP_ACTIONS.items()):
    if not isinstance(_v, tuple):
        ALL_RP_ACTIONS[_k] = (RP_EMOJI.get(_k, "✨"), _v)

# Список всех фраз для распознавания
_ALL_RP_PHRASES = sorted(
    set(ALL_RP_ACTIONS) | set(ALL_RP_ALIASES),
    key=len,
    reverse=True,
)

def _unified_rp_parse(text):
    raw = (text or "").strip()
    normalized = re.sub(r"^(?:[/!.]|Ирис\s+)", "", raw, flags=re.I).strip()
    low = normalized.lower()
    for phrase in _ALL_RP_PHRASES:
        if low == phrase or low.startswith(phrase + " "):
            canonical = ALL_RP_ALIASES.get(phrase, phrase)
            return canonical, normalized[len(phrase):].strip()
    return None, ""

UNIFIED_RP_PATTERN = r"^(?:[/!.]|Ирис\s+)?(?:" + "|".join(
    re.escape(x) for x in _ALL_RP_PHRASES
) + r")(?:\s+.*)?$"

@dp.message(StateFilter(None), F.text.regexp(UNIFIED_RP_PATTERN, flags=re.I))
async def unified_rp_handler_v813(message: Message):
    action, rest = _unified_rp_parse(message.text or "")
    if not action:
        return
    # Keep the canonical action in the unified registry.
    if action not in ALL_RP_ACTIONS:
        return
    target = await _rp_target(message, rest)
    await _run_rp(message, action, target)

async def _run_extra_rp(message, action, target):
    if not target:
        await message.answer("↩️ Ответь на сообщение пользователя или укажи @username.")
        return
    if target.id==message.from_user.id:
        await message.answer("❌ Нельзя выполнить действие на себе.")
        return
    if message.chat.type!="private" and db.iris_chat_settings(message.chat.id)["rp_enabled"] is False:
        return
    if action in IRIS_EXTRA_RP:
        emoji, verb = IRIS_EXTRA_RP[action]
    else:
        emoji, verb = RP_EMOJI.get(action, "✨"), action
    await message.answer(f"{emoji} | {mention_user(message.from_user.id,message.from_user.full_name)} {verb} {mention_user(target.id,target.full_name)}")
    db.log_rp_action(message.from_user.id,target.id,action)


# ---- Unified command aliases (additive; legacy handlers remain untouched) ----
# These aliases ensure the main public functions have Russian + English slash names.
@dp.message(Command(
    "панель", "mainpanel", "главная", "main",
    "профиль", "profile", "мойпрофиль",
    "стата", "статистика", "statistics",
    "рейтинг", "участники", "ники",
    "правила", "rules", "правилагильдии", "guildrules",
    "правилакв", "kvrules", "kvrule",
    "рег", "регистрация", "registration",
    "заявка", "вступить", "apply", "join",
    "браки", "marriages", "брак", "marry",
    "развод", "divorce",
    "кв", "kvs", "kv",
    "помощь", "команды", "help",
    "коины", "coins", "монеты", "магазин", "shop",
    "рефералы", "referrals", "ref",
    "достижения", "achievements", "прогресс", "progress", "серия", "streak",
    "турниры", "tournaments", "coin_top", "coinstop",
    "гость", "гостевая", "guest", "guestpanel",
    "ffstats", "ffoutfit", "guildinfo", "bancheck", "ffapi"
))
async def unified_public_slash_aliases(message: Message, state: FSMContext):
    # Only handle aliases that are not already consumed by a more specific handler.
    name=(message.text or "").split()[0].lstrip("/").split("@",1)[0].lower()
    aliases={
        "панель":"panel", "mainpanel":"panel", "главная":"panel", "main":"panel",
        "профиль":"profile", "мойпрофиль":"profile", "стата":"stats", "статистика":"stats", "statistics":"stats",
        "рейтинг":"top", "участники":"users", "ники":"users",
        "правила":"rules", "guildrules":"guildrules", "правилагильдии":"guildrules",
        "правилакв":"kvrules", "правилакв":"kvrules", "kvrule":"kvrules",
        "рег":"register", "регистрация":"register", "registration":"register",
        "заявка":"apply", "вступить":"apply", "join":"apply",
        "браки":"marry", "marriages":"marry", "брак":"marry", "развод":"divorce",
        "кв":"kv", "kvs":"kv", "помощь":"help", "команды":"help",
        "коины":"coins", "монеты":"coins", "магазин":"shop", "рефералы":"ref", "referrals":"ref",
        "достижения":"achievements", "прогресс":"progress", "серия":"streak", "турниры":"tournaments",
        "coin_top":"coinstop", "гость":"guest", "гостевая":"guest", "guestpanel":"guest",
        "ffstats":"ffstats", "ffoutfit":"ffoutfit", "guildinfo":"guildinfo", "bancheck":"bancheck", "ffapi":"ffapi",
        "sayall":"sayall", "сказатьвсем":"sayall", "stopchat":"stopchat", "стопчат":"stopchat",
        "startchat":"startchat", "запускчат":"startchat", "clearall":"clearall",
        "снятьвсеограничения":"clearall", "убратьвсеограничения":"clearall",
        "mystats":"mystats", "моястатистика":"mystats", "myactivity":"myactivity", "мояактивность":"myactivity",
        "kvrosters":"kvrosters", "составкв":"kvrosters"
    }
    cmd=aliases.get(name)
    if not cmd: return
    fn={
        "panel":command_panel,"profile":command_profile,"stats":command_stats,"top":command_top,"users":command_users,
        "rules":command_rules,"guildrules":command_guild_rules,"kvrules":command_kv_rules,"register":command_register,
        "apply":command_apply_v71,"marry":command_marry,"divorce":command_divorce,"kv":command_kv,"help":command_help,
        "coins":command_coins,"shop":command_shop,"ref":command_ref,"achievements":command_achievements,"progress":command_progress,
        "streak":command_streak,"tournaments":command_tournaments,"coinstop":command_coin_top,"guest":command_guest_v71,
        "ffstats":command_ffstats,"ffoutfit":command_ffoutfit,"guildinfo":command_guildinfo,"bancheck":command_bancheck,"ffapi":command_ffapi,
        "sayall":command_say_all,"stopchat":command_stop_chat,"startchat":command_start_chat,"clearall":command_clear_all_restrictions,
        "mystats":command_my_stats,"myactivity":command_my_activity,"kvrosters":command_kvrosters
    }.get(cmd)
    if fn:
        await fn(message)

# =========================================================
# FINAL COMMAND REGISTRY (V2.8)
# =========================================================
# This is additive. Existing dictionaries/handlers above remain intact.
V71_NO_SLASH.update({
    "команды админов":"admincommands", "admin commands":"admincommands", "admincommands":"admincommands",
    "назначенные кв":"kv_scheduled", "назначеные кв":"kv_scheduled", "scheduled kv":"kv_scheduled",
    "история кв":"kv_history", "kv history":"kv_history",
    "кв результат":"kvresult", "кврезультат":"kvresult", "кв result":"kvresult",
    "призвать":"summon", "призывать всех":"summon", "калл":"summon",
    "сказать всем":"sayall", "стоп чат":"stopchat", "запуск чат":"startchat",
    "снять все ограничения":"clearall", "убрать все ограничения":"clearall",
})
V71_FN.update({
    "admincommands":command_admin_commands,
    "kv_scheduled":command_kv_scheduled,
    "kv_history":command_kv_history,
    "kvresult":command_kv_result,
    "summon":command_summon,
    "sayall":command_say_all,
    "stopchat":command_stop_chat,
    "startchat":command_start_chat,
    "clearall":command_clear_all_restrictions,
})
UNIFIED_SLASH_ALIASES.update({
    "командыадминов":"admincommands", "admincommands":"admincommands",
    "назначенные":"kv_scheduled", "назначенныекв":"kv_scheduled",
    "историякв":"kv_history", "kvhistory":"kv_history",
    "кврезультат":"kvresult", "kvresult":"kvresult",
    "призыватьвсех":"summon", "призвать":"summon",
})

# =========================================================
# BACKGROUND CLEANUP
# =========================================================

def _cleanup_message_is_important(message: Message) -> bool:
    """Protect KV and other important bot announcements from automatic cleanup."""
    text = (message.text or message.caption or "").lower()
    important_markers = (
        "кв назначено", "кв принято", "кв одобрено", "кв создано", "кв #",
        "гильдия противника", "состав противника", "наш состав", "кв предложение",
        "кв подтверждено", "важное объявление", "объявление гильдии",
    )
    return any(marker in text for marker in important_markers)


async def _automatic_chat_cleanup():
    """Every CLEANUP_INTERVAL_HOURS remove observed trash older than the interval.
    Bot messages have their own five-minute expiry and are not bulk-deleted here
    unless they are already expired and unprotected.
    """
    chat_id = int(CLEANUP_CHAT_ID)
    cutoff = datetime.now(ZoneInfo(TIMEZONE)).timestamp() - max(1, int(CLEANUP_INTERVAL_HOURS))*3600
    pinned_ids = await _get_pinned_ids(chat_id)
    deleted = skipped = failed = 0

    candidates=list(_cleanup_candidate_messages.get(chat_id, ()))
    objects={m.message_id:m for m in _cleanup_message_objects.get(chat_id, ())}
    for mid in reversed(candidates):
        if mid in pinned_ids:
            skipped += 1; continue
        obj=objects.get(mid)
        if obj is not None:
            try:
                stamp=obj.date.timestamp()
                if stamp > cutoff:
                    continue
            except Exception:
                continue
        try:
            await bot.delete_message(chat_id, mid)
            deleted += 1
        except TelegramBadRequest as exc:
            msg=str(exc).lower()
            if "message to delete not found" not in msg and "message can't be deleted" not in msg:
                failed += 1
        except Exception:
            failed += 1
    logger.info("Automatic trash cleanup chat=%s deleted=%s skipped=%s failed=%s", chat_id, deleted, skipped, failed)


async def cleanup_loop():
    """Run real chat cleanup every 24 hours, without touching pinned/important posts."""
    interval_seconds = max(1, int(CLEANUP_INTERVAL_HOURS)) * 3600
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            await _automatic_chat_cleanup()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка автоматической очистки чата")


async def _delete_bot_message_later(message: Message, delay: int = 300):
    await asyncio.sleep(delay)
    try:
        if _cleanup_message_is_important(message):
            return
        if message.chat.type in ("group", "supergroup"):
            pinned=await _get_pinned_ids(message.chat.id)
            if message.message_id in pinned:
                return
        await bot.delete_message(message.chat.id, message.message_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


async def _kv_warning_loop():
    """Warn participants 30 minutes before every planned KV, once per match."""
    while True:
        try:
            now=datetime.now(ZoneInfo(TIMEZONE))
            for row in db.get_kvs(status="planned", limit=100):
                if not row["match_date"] or not row["match_time"] or int(row["warning_sent"] or 0):
                    continue
                try:
                    match_dt=datetime.strptime(f"{row['match_date']} {row['match_time']}", "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(TIMEZONE))
                except ValueError:
                    continue
                delta=(match_dt-now).total_seconds()
                if 0 <= delta <= 30*60:
                    ours=_json4(row["our_members"])
                    mentions=[]
                    for nick in ours:
                        player=db.get_player_by_nick(nick)
                        if player and player["telegram_id"]:
                            mentions.append(mention_user(int(player["telegram_id"]),player["nick"]))
                        else:
                            mentions.append(html.escape(str(nick)))
                    text=("⚠️ <b>КВ НАЧНЁТСЯ ЧЕРЕЗ 30 МИНУТ</b>\n\n"
                          f"⚔️ Против: <b>{html.escape(row['enemy_guild'] or '—')}</b>\n"
                          f"👥 Участники: {', '.join(mentions) or '—'}\n"
                          f"🕐 {row['match_date']} {row['match_time']} МСК\n"
                          f"🆔 КВ #{row['id']}")
                    try:
                        await bot.send_message(GUILD_CHAT_ID,text)
                        for nick in ours:
                            player=db.get_player_by_nick(nick)
                            if player and player["telegram_id"]:
                                try:
                                    await bot.send_message(int(player["telegram_id"]),
                                        f"⚠️ КВ #{row['id']} начнётся через 30 минут — {row['match_date']} {row['match_time']} МСК.")
                                except Exception:
                                    pass
                        db.mark_kv_warning_sent(row["id"])
                    except Exception:
                        logger.exception("Не удалось отправить предупреждение о КВ #%s", row["id"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка KV warning loop")
        await asyncio.sleep(20)


# =========================================================
# START BOT
# =========================================================

async def main():
    global bot

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    # Track bot-generated messages so manual cleanup can remove them later.
    _original_send_message = bot.send_message

    async def _tracked_send_message(*args, **kwargs):
        sent = await _original_send_message(*args, **kwargs)
        try:
            chat_id = sent.chat.id
            _cleanup_bot_messages[chat_id].append(sent.message_id)
            _cleanup_message_objects[chat_id].append(sent)
            # Bot messages are ephemeral by default. Important/pinned messages are
            # protected by the delayed cleanup routine.
            asyncio.create_task(_delete_bot_message_later(sent))
        except Exception:
            pass
        return sent

    bot.send_message = _tracked_send_message

    scheduler = WeeklyScheduler(db, send_publish_warning, publish_week, weekly_rollover)
    asyncio.create_task(scheduler.run())
    asyncio.create_task(automatic_backup_loop())
    asyncio.create_task(activity_report_loop())
    asyncio.create_task(cleanup_loop())
    asyncio.create_task(_kv_warning_loop())

    logger.info("✅ Vaka V7.1 запущен (SiamBhau API)")

    try:
        await dp.start_polling(bot)
    finally:
        db.close()
        await ff_client.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
