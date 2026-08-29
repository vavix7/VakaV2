import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=False)


def get_int_env(name: str, default: int = 0) -> int:
    value = os.getenv(name, str(default)).strip()
    try:
        return int(value)
    except ValueError:
        return default


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False
    return default


# =========================================================
# БОТ
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не указан в .env")


# =========================================================
# АДМИНИСТРАТОРЫ
# =========================================================

# Permanent owner bootstrap ID. The owner role is restored on every startup.
# Vavix / Telegram ID: 8930370348
OWNER_ID = get_int_env("OWNER_ID", 8930370348)

# Optional bootstrap IDs from .env; roles are stored in SQLite.
ADMIN_IDS = set()

_admin_ids_raw = os.getenv("ADMIN_IDS", "").strip()
if _admin_ids_raw:
    for value in _admin_ids_raw.split(","):
        value = value.strip()
        if value:
            try:
                ADMIN_IDS.add(int(value))
            except ValueError:
                pass

legacy_admin_id = get_int_env("ADMIN_ID", 0)
if legacy_admin_id:
    ADMIN_IDS.add(legacy_admin_id)


# =========================================================
# FREE FIRE API (SiamBhau)
# =========================================================

FF_API_KEY = os.getenv("FF_API_KEY", "").strip()
FF_REGION = os.getenv("FF_REGION", "BD").strip().upper()
FF_GUILD_ID = os.getenv("FF_GUILD_ID", "3101503290").strip()
# New Free Fire API. The old SiamBhau endpoint is no longer used.
FF_API_PROVIDER = "siambhau"
FF_API_BASE = os.getenv("FF_API_BASE", "https://siambhau69.eu.cc").strip().rstrip("/")


# =========================================================
# TELEGRAM
# =========================================================

GUILD_CHAT_ID = get_int_env("GUILD_CHAT_ID", -5467735741)
# Chat where the ADB/Free Fire monitoring payload is accepted.
# Falls back to the guild chat for backward compatibility.
MONITORING_CHAT_ID = get_int_env("MONITORING_CHAT_ID", GUILD_CHAT_ID)
PUBLISH_HOUR = 4
PUBLISH_MINUTE = 10
TIMEZONE = "Europe/Moscow"
PUBLISH_WARNING_HOURS = get_int_env("PUBLISH_WARNING_HOURS", 6)
REWARD_WEEKLY_THRESHOLD = get_int_env("REWARD_WEEKLY_THRESHOLD", 5000)
REWARD_MONTHLY_THRESHOLD = get_int_env("REWARD_MONTHLY_THRESHOLD", 12000)
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/guild_activity.db").strip() or "data/guild_activity.db"

# =========================================================
# COMMUNITY / REFERRALS / MODERATION
# =========================================================
REFERRAL_COINS_PER_INVITE = get_int_env("REFERRAL_COINS_PER_INVITE", 20)
ACTIVITY_COINS_PER_100 = get_int_env("ACTIVITY_COINS_PER_100", 60)
ACTIVITY_REPORT_INTERVAL_HOURS = get_int_env("ACTIVITY_REPORT_INTERVAL_HOURS", 3)
DIAMOND_EXCHANGE_COINS = get_int_env("DIAMOND_EXCHANGE_COINS", 1400)
DIAMOND_EXCHANGE_AMOUNT = get_int_env("DIAMOND_EXCHANGE_AMOUNT", 450)
UNBAN_EXCHANGE_COINS = get_int_env("UNBAN_EXCHANGE_COINS", 1500)
SHOP_LITE_COINS = get_int_env("SHOP_LITE_COINS", 1400)
SHOP_CLASSIC_COINS = get_int_env("SHOP_CLASSIC_COINS", 4500)
SHOP_PRO_COINS = get_int_env("SHOP_PRO_COINS", 9600)
SHOP_KB_COINS = get_int_env("SHOP_KB_COINS", 4000)
SHOP_BO_COINS = get_int_env("SHOP_BO_COINS", 3600)
SHOP_LVL_BOT_COINS = get_int_env("SHOP_LVL_BOT_COINS", 10000)
SHOP_ALL_LOCKS_COINS = get_int_env("SHOP_ALL_LOCKS_COINS", 2000)
SHOP_UNMUTE_COINS = get_int_env("SHOP_UNMUTE_COINS", 1200)
SHOP_UNWARN_COINS = get_int_env("SHOP_UNWARN_COINS", 400)
REFERRAL_MIN_ACCOUNT_AGE_DAYS = get_int_env("REFERRAL_MIN_ACCOUNT_AGE_DAYS", 0)
SUMMON_COOLDOWN_SECONDS = get_int_env("SUMMON_COOLDOWN_SECONDS", 300)
WELCOME_ENABLED = get_bool_env("WELCOME_ENABLED", True)
RULES_TITLE = os.getenv("RULES_TITLE", "📜 ПРАВИЛА ГИЛЬДИИ").strip()
AI_ENABLED = get_bool_env("AI_ENABLED", True)
AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini").strip()

# Optional off-host backup delivery. Telegram is backup storage, not the live transactional DB.
BACKUP_CHAT_ID = get_int_env("BACKUP_CHAT_ID", OWNER_ID)
BACKUP_INTERVAL_HOURS = get_int_env("BACKUP_INTERVAL_HOURS", 6)


# =========================================================
# АКТИВНОСТЬ
# =========================================================

LOW_ACTIVITY_LIMIT = 1000
HIGH_ACTIVITY_LIMIT = 5000


# =========================================================
# ЗВАНИЯ (настраиваемые)
# =========================================================

RANKS = [
    (0, "💤 Новичок"),
    (1000, "🔰 Ученик"),
    (3000, "⚔️ Боец"),
    (5000, "🔥 Активист"),
    (7500, "⚡ Опытный"),
    (10000, "💪 Сильный игрок"),
    (15000, "🏆 Ветеран"),
    (20000, "👑 Элитный игрок"),
    (30000, "💎 Мастер"),
    (50000, "🔥 Легенда"),
    (75000, "👑 Грандмастер"),
    (100000, "💀 Бог активности"),
    (150000, "☠️ Абсолют"),
    (200000, "🌌 Легенда гильдии")
]


def get_rank(activity: int) -> str:
    """Получить звание по активности"""
    rank = RANKS[0][1]
    for threshold, rank_name in RANKS:
        if activity >= threshold:
            rank = rank_name
    return rank


# FF_API_KEY is optional for local/offline operation; FF commands report a clear error when unavailable.


# =========================================================
# AI FALLBACK CHAIN / CHAT CLEANUP
# =========================================================
AI_FALLBACK_MODELS = [
    x.strip() for x in os.getenv(
        "AI_FALLBACK_MODELS",
        "gemini-1.5-flash,llama-3.3-70b-versatile,mistral-small-latest,@cf/meta/llama-3.1-8b-instruct,meta-llama/llama-3.1-8b-instruct:free"
    ).split(",") if x.strip()
]
AI_GEMINI_API_KEY = os.getenv("AI_GEMINI_API_KEY", "").strip()
AI_GROQ_API_KEY = os.getenv("AI_GROQ_API_KEY", "").strip()
AI_MISTRAL_API_KEY = os.getenv("AI_MISTRAL_API_KEY", "").strip()
AI_OPENROUTER_API_KEY = os.getenv("AI_OPENROUTER_API_KEY", "").strip()
AI_CLOUDFLARE_API_TOKEN = os.getenv("AI_CLOUDFLARE_API_TOKEN", "").strip()
AI_CLOUDFLARE_ACCOUNT_ID = os.getenv("AI_CLOUDFLARE_ACCOUNT_ID", "").strip()
CLEANUP_CHAT_ID = get_int_env("CLEANUP_CHAT_ID", -1004361184660)
CLEANUP_INTERVAL_HOURS = get_int_env("CLEANUP_INTERVAL_HOURS", 12)
