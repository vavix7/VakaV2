#!/usr/bin/env python3
"""
Vaka V6 database migration/optimization utility.

Usage:
  python tools/migrate_database.py path/to/legacy.db data/guild_activity.db

The migration is additive and non-destructive. The source database is never
modified. It copies rows from known legacy tables where schemas overlap,
then lets V6's Database class create missing tables/columns and indexes.
"""
import argparse, shutil, sqlite3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from database import Database

TABLE_KEYS = {
    "players": ["player_id"],
    "weeks": ["week_start"],
    "week_players": ["week_start", "player_id"],
    "rewards": ["week_start", "player_id"],
    "logs": ["id"],
    "referrals": ["telegram_id"],
    "referral_links": ["telegram_id"],
    "coin_balances": ["telegram_id"],
    "coin_transactions": ["id"],
    "moderation_warnings": ["id"],
    "moderation_bans": ["chat_id", "user_id"],
    "shop_requests": ["request_id"],
    "admin_roles": ["telegram_id"],
}

def cols(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]

def copy_table(src, dst, table):
    if not src.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
        return 0
    s_cols = cols(src, table)
    d_cols = cols(dst, table)
    common = [c for c in s_cols if c in d_cols]
    if not common:
        return 0
    placeholders = ",".join("?" for _ in common)
    names = ",".join(common)
    rows = src.execute(f"SELECT {names} FROM {table}").fetchall()
    key = TABLE_KEYS.get(table, [])
    copied = 0
    for row in rows:
        values = tuple(row)
        if key:
            where = " AND ".join(f"{k}=?" for k in key if k in common)
            if where:
                kvals = tuple(row[s_cols.index(k)] for k in key if k in common)
                exists = dst.execute(f"SELECT 1 FROM {table} WHERE {where} LIMIT 1", kvals).fetchone()
                if exists:
                    continue
        try:
            dst.execute(f"INSERT INTO {table} ({names}) VALUES ({placeholders})", values)
            copied += 1
        except sqlite3.IntegrityError:
            pass
    return copied

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("destination")
    args = ap.parse_args()
    source = Path(args.source)
    dest = Path(args.destination)
    if not source.exists():
        raise SystemExit(f"Source not found: {source}")
    if dest.exists():
        backup = dest.with_suffix(dest.suffix + ".before_migration.bak")
        shutil.copy2(dest, backup)
    db = Database(str(dest))
    src = sqlite3.connect(source)
    src.row_factory = sqlite3.Row
    try:
        total = 0
        for table in TABLE_KEYS:
            total += copy_table(src, db.conn, table)
        db._migrate()
        db.optimize()
        print(f"Migration complete. Rows copied: {total}")
        print(f"Integrity: {'OK' if db.integrity_check() else 'FAILED'}")
    finally:
        src.close()
        db.close()

if __name__ == "__main__":
    main()
