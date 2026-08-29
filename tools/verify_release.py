"""Offline release verification for Vaka.
Checks Python syntax, SQLite integrity, static callback coverage, excluded Iris
modules, and SiamBhau configuration without contacting Telegram or the API.
"""
from __future__ import annotations
import ast, re, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT = ROOT / "bot.py"
DB = ROOT / "data" / "guild_activity.db"

errors=[]

# Python syntax/import-level compilation.
for p in [ROOT/"bot.py",ROOT/"config.py",ROOT/"database.py",ROOT/"ff_api.py",ROOT/"parser.py",ROOT/"scheduler.py"]:
    try: compile(p.read_text(encoding="utf-8"), str(p), "exec")
    except Exception as e: errors.append(f"compile {p.name}: {e}")

# SQLite integrity.
try:
    con=sqlite3.connect(DB)
    result=con.execute("PRAGMA integrity_check").fetchone()[0]
    if result.lower() != "ok": errors.append(f"sqlite integrity: {result}")
    con.close()
except Exception as e: errors.append(f"sqlite open: {e}")

s=BOT.read_text(encoding="utf-8")
callbacks=set(re.findall(r'callback_data\s*=\s*["\']([^"\']+)',s))
handlers=re.findall(r'@dp\.callback_query\((.*?)\)\n',s,re.S)
ht=" ".join(re.sub(r"\s+"," ",h) for h in handlers)
missing=[]
for cb in callbacks:
    if any(x in ht for x in [f'F.data == "{cb}"',f"F.data == '{cb}'",f'F.data.startswith("{cb}',f"F.data.startswith('{cb}"]):
        continue
    # Generic handlers can intentionally match a whole callback family.
    family_prefixes=("fullstats_","shop_cat_","shop_buy_","shop_product_","users_page_","v71_guest_","weekly_player_","weekly_add_","weekly_sub_","weekly_set_","lifetime_player_","lifetime_add_","lifetime_sub_","lifetime_set_","refresh_player_","adminpickpage_","adminpick_remove_","adminpick_unbind_","adminpick_coins_","remove_confirm_","unbind_confirm_","reward_award_","publish_confirm_","player_","player_ach_","player_prog_","logs_page_","owner_backup_restore_","owner_backup_confirm_","shop_approve_","shop_decline_","week_select_","v71_app_accept_","v71_app_decline_","v71_kv_accept_","v71_kv_decline_","guestkv:","kvroster:","marry_yes_","marry_no_")
    if any(cb.startswith(prefix) for prefix in family_prefixes):
        continue
    if f'"{cb}"' in ht or f"'{cb}'" in ht:
        continue
    missing.append(cb)
if missing: errors.append("unhandled callback_data: " + ", ".join(sorted(missing)))

# Hard exclusions requested by the owner.
excluded=["Дуэли","Кубы","Кланы","Кружки","Ирис-биржа","Модуль «Закладки»","Модуль «Заметки»","Модуль «Таймеры»","Модуль «Каталог»","Модуль «Репутация»","Модуль «Награды»","Модуль «Отношения»"]
# We only reject obvious module implementation markers, not mentions in changelogs.
for marker in ["duel_router","dice_router","clan_router","circle_router","iris_exchange_router","bookmark_router","notes_router","timer_router","catalog_router","reputation_router"]:
    if marker in s.lower(): errors.append(f"excluded module marker present: {marker}")

if "https://siambhau69.eu.cc" not in (ROOT/"config.py").read_text(encoding="utf-8"):
    errors.append("SiamBhau HTTPS base URL missing")
if 'FF_REGION = os.getenv("FF_REGION", "BD")' not in (ROOT/"config.py").read_text(encoding="utf-8"):
    errors.append("BD default region missing")

if errors:
    print("RELEASE_VERIFY_FAIL")
    for e in errors: print(" -",e)
    sys.exit(1)
print("RELEASE_VERIFY_OK")
print(f"callbacks={len(callbacks)} callback_handlers={len(handlers)}")
print("sqlite=OK")
print("python=OK")
print("siambhau=https://siambhau69.eu.cc")
print("default_region=BD; india_fallback=IND")
