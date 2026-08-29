import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime
from typing import Optional


class Database:
    """SQLite storage with additive, non-destructive migrations."""

    def __init__(self, path: str):
        self.path = path
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_tables()
        self._migrate()

    def _now(self):
        return datetime.now().isoformat(timespec="seconds")

    @contextmanager
    def _connect(self):
        """Compatibility connection context for optional AI/storage helpers."""
        yield self.conn


    def _table_columns(self, table: str):
        return {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()}

    def _add_missing_columns(self, table: str, columns: dict):
        existing = self._table_columns(table)
        for col, col_type in columns.items():
            if col not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    def _create_tables(self):
        c = self.conn.cursor()
        now = self._now()
        c.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id TEXT PRIMARY KEY,
                nick TEXT NOT NULL,
                telegram_id INTEGER UNIQUE,
                telegram_username TEXT,
                total_activity INTEGER NOT NULL DEFAULT 0,
                weeks_count INTEGER NOT NULL DEFAULT 0,
                region TEXT,
                level INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                avatar_url TEXT,
                guild_id TEXT,
                guild_name TEXT,
                guild_level INTEGER DEFAULT 0,
                guild_members INTEGER DEFAULT 0,
                guild_capacity INTEGER DEFAULT 0,
                guild_owner TEXT,
                api_data TEXT,
                is_banned BOOLEAN DEFAULT 0,
                last_api_check TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS weeks (
                week_start TEXT PRIMARY KEY,
                week_end TEXT NOT NULL,
                total_activity INTEGER NOT NULL DEFAULT 0,
                players_count INTEGER NOT NULL DEFAULT 0,
                published INTEGER NOT NULL DEFAULT 0,
                publish_requested_at TEXT,
                publish_confirmed_at TEXT,
                published_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS week_players (
                week_start TEXT NOT NULL,
                player_id TEXT NOT NULL,
                nick TEXT NOT NULL,
                activity INTEGER NOT NULL DEFAULT 0,
                rank_name TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (week_start, player_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS rewards (
                reward_id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL,
                player_id TEXT NOT NULL,
                reward_type TEXT NOT NULL,
                required_activity INTEGER NOT NULL,
                actual_activity INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'earned',
                awarded_at TEXT,
                awarded_by INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(week_start, player_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                actor_id INTEGER,
                action TEXT NOT NULL,
                details TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_players_telegram ON players(telegram_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_week_players_week ON week_players(week_start)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_week_players_player ON week_players(player_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_rewards_week ON rewards(week_start)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_rewards_status ON rewards(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_created ON logs(created_at DESC)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                telegram_id INTEGER PRIMARY KEY,
                referrer_telegram_id INTEGER,
                referrer_player_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE(telegram_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS referral_links (
                telegram_id INTEGER PRIMARY KEY,
                invite_link TEXT UNIQUE,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS coin_balances (
                telegram_id INTEGER PRIMARY KEY,
                coins INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS coin_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                meta TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS moderation_warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                admin_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                points INTEGER NOT NULL DEFAULT 1,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS moderation_bans (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                admin_id INTEGER NOT NULL,
                reason TEXT,
                banned_until TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                PRIMARY KEY(chat_id,user_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS shop_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                request_type TEXT NOT NULL,
                coins_cost INTEGER NOT NULL,
                reward_amount INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                handled_at TEXT,
                handled_by INTEGER
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_shop_requests_status ON shop_requests(status,request_id DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_telegram_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_coin_tx_user ON coin_transactions(telegram_id,id DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_warn_user ON moderation_warnings(chat_id,user_id,id DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bans_chat ON moderation_bans(chat_id,active)")
        c.execute("CREATE TABLE IF NOT EXISTS admin_roles (telegram_id INTEGER PRIMARY KEY, role_level INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_admin_roles_level ON admin_roles(role_level)")
        # V6 indexes: optimize the most frequent profile/ranking/history queries.
        c.execute("CREATE INDEX IF NOT EXISTS idx_players_activity ON players(total_activity DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_players_updated ON players(updated_at DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_week_players_activity ON week_players(week_start, activity DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_referrals_status ON referrals(status, referrer_telegram_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bans_user_active ON moderation_bans(user_id, active)")
        # Iris compatibility storage. Additive only: never removes or rewrites existing data.
        c.execute("""
            CREATE TABLE IF NOT EXISTS iris_blacklist (
                chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, added_by INTEGER NOT NULL,
                reason TEXT, created_at TEXT NOT NULL, PRIMARY KEY(chat_id,user_id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_iris_blacklist_user ON iris_blacklist(user_id)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS iris_chat_settings (
                chat_id INTEGER PRIMARY KEY, rp_enabled INTEGER NOT NULL DEFAULT 1,
                commands_notice INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS iris_rp_custom (
                id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL,
                name TEXT NOT NULL UNIQUE, emoji TEXT NOT NULL DEFAULT '✨', template TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS kv_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL DEFAULT 'КВ',
                match_date TEXT, match_time TEXT, purpose TEXT,
                enemy_guild TEXT, enemy_members TEXT NOT NULL DEFAULT '[]',
                our_members TEXT NOT NULL DEFAULT '[]', status TEXT NOT NULL DEFAULT 'planned',
                proposer_id INTEGER, created_by INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS kv_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kv_id INTEGER,
                match_date TEXT NOT NULL,
                our_score INTEGER NOT NULL DEFAULT 0,
                enemy_score INTEGER NOT NULL DEFAULT 0,
                result TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(kv_id) REFERENCES kv_matches(id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS guild_applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT, telegram_id INTEGER NOT NULL,
                username TEXT, uid TEXT, nick TEXT, status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL, handled_at TEXT, handled_by INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS marriages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user1_id INTEGER NOT NULL, user2_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', proposer_id INTEGER NOT NULL, created_at TEXT NOT NULL, accepted_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS rp_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id INTEGER NOT NULL, target_id INTEGER NOT NULL,
                action TEXT NOT NULL, created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS kv_rosters (
                slot INTEGER PRIMARY KEY,
                members TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL
            )
        """)
        for _slot in (1,2,3):
            c.execute("INSERT OR IGNORE INTO kv_rosters(slot,members,updated_at) VALUES(?,?,?)", (_slot, '[]', self._now()))
        c.execute("CREATE INDEX IF NOT EXISTS idx_kv_status ON kv_matches(status,match_date,match_time)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_apps_status ON guild_applications(status,created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_marriages_users ON marriages(user1_id,user2_id,status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_rp_actions_created ON rp_actions(created_at DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_rewards_player ON rewards(player_id, week_start DESC)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS activity_coin_awards (
                week_start TEXT NOT NULL,
                player_id TEXT NOT NULL,
                units_awarded INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (week_start, player_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS shop_products (
                product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price INTEGER NOT NULL,
                description TEXT DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_activity_coin_awards_week ON activity_coin_awards(week_start)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_shop_products_active ON shop_products(active, product_id)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS tournaments (
                tournament_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_by INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS tournament_players (
                tournament_id INTEGER NOT NULL,
                player_id TEXT NOT NULL,
                points INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(tournament_id, player_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                achievement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                threshold INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS player_achievements (
                player_id TEXT NOT NULL,
                achievement_id INTEGER NOT NULL,
                awarded_at TEXT NOT NULL,
                PRIMARY KEY(player_id, achievement_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS anti_cheat_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL,
                player_id TEXT NOT NULL,
                previous_activity INTEGER NOT NULL,
                current_activity INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                reviewed INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_tournament_players_points ON tournament_players(tournament_id, points DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_achievements_player ON player_achievements(player_id, awarded_at DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_anticheat_week ON anti_cheat_events(week_start, created_at DESC)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS monitoring_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL,
                player_id TEXT NOT NULL,
                activity INTEGER NOT NULL,
                game_total INTEGER,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_monitoring_snapshots_player ON monitoring_snapshots(week_start, player_id, id DESC)")

        c.execute("CREATE TABLE IF NOT EXISTS migration_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        # Optional AI memory/settings carried forward from the V4/V5 design.
        c.execute("""
            CREATE TABLE IF NOT EXISTS ai_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_ai_memory_user ON ai_memory(telegram_id, id DESC)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def _migrate(self):
        # Additive migration: never delete existing rows.
        player_columns_before = self._table_columns("players")
        self._add_missing_columns("guild_applications", {
            "why_join": "TEXT",
            "how_found": "TEXT",
            "extra_info": "TEXT",
            "review_note": "TEXT",
        })
        self._add_missing_columns("kv_matches", {
            "rules_accepted": "INTEGER NOT NULL DEFAULT 0",
            "proposer_guild": "TEXT",
            "proposer_note": "TEXT",
        })
        self._add_missing_columns("kv_matches", {
            "warning_sent": "INTEGER NOT NULL DEFAULT 0",
        })
        self._add_missing_columns("players", {
            "telegram_username": "TEXT", "total_activity": "INTEGER NOT NULL DEFAULT 0",
            "weeks_count": "INTEGER NOT NULL DEFAULT 0", "region": "TEXT", "level": "INTEGER DEFAULT 0",
            "likes": "INTEGER DEFAULT 0", "avatar_url": "TEXT", "guild_id": "TEXT", "guild_name": "TEXT",
            "guild_level": "INTEGER DEFAULT 0", "guild_members": "INTEGER DEFAULT 0",
            "guild_capacity": "INTEGER DEFAULT 0", "guild_owner": "TEXT", "api_data": "TEXT",
            "is_banned": "BOOLEAN DEFAULT 0", "last_api_check": "TEXT",
            "created_at": "TEXT", "updated_at": "TEXT",
        })
        self._add_missing_columns("weeks", {
            "week_end": "TEXT", "total_activity": "INTEGER NOT NULL DEFAULT 0",
            "players_count": "INTEGER NOT NULL DEFAULT 0", "published": "INTEGER NOT NULL DEFAULT 0",
            "publish_requested_at": "TEXT", "publish_confirmed_at": "TEXT", "publish_started_at": "TEXT", "published_at": "TEXT",
            "created_at": "TEXT", "updated_at": "TEXT",
            "rollover_done": "INTEGER NOT NULL DEFAULT 0", "rollover_at": "TEXT",
        })
        self._add_missing_columns("week_players", {
            "nick": "TEXT", "activity": "INTEGER NOT NULL DEFAULT 0",
            "rank_name": "TEXT", "created_at": "TEXT", "updated_at": "TEXT",
        })
        self._add_missing_columns("rewards", {
            "week_start": "TEXT", "player_id": "TEXT", "reward_type": "TEXT",
            "required_activity": "INTEGER DEFAULT 0", "actual_activity": "INTEGER DEFAULT 0",
            "status": "TEXT DEFAULT 'earned'", "awarded_at": "TEXT", "awarded_by": "INTEGER",
            "created_at": "TEXT", "updated_at": "TEXT",
        })
        self._add_missing_columns("logs", {
            "created_at": "TEXT", "actor_id": "INTEGER", "action": "TEXT", "details": "TEXT",
        })
        now = self._now()
        self.conn.execute("UPDATE players SET created_at=COALESCE(created_at, ?), updated_at=COALESCE(updated_at, ?)", (now, now))
        self.conn.execute("UPDATE weeks SET created_at=COALESCE(created_at, ?), updated_at=COALESCE(updated_at, ?)", (now, now))
        self.conn.execute("UPDATE week_players SET created_at=COALESCE(created_at, ?), updated_at=COALESCE(updated_at, ?)", (now, now))
        # If lifetime fields were newly introduced, reconstruct them from the weekly history.
        if "total_activity" not in player_columns_before:
            self.conn.execute("""
                UPDATE players SET total_activity = COALESCE((
                    SELECT SUM(wp.activity) FROM week_players wp WHERE wp.player_id = players.player_id
                ), 0)
            """)
        if "weeks_count" not in player_columns_before:
            self.conn.execute("""
                UPDATE players SET weeks_count = COALESCE((
                    SELECT COUNT(*) FROM week_players wp WHERE wp.player_id = players.player_id
                ), 0)
            """)
        self.conn.execute("""
            UPDATE weeks SET
                total_activity=COALESCE((SELECT SUM(activity) FROM week_players wp WHERE wp.week_start=weeks.week_start),0),
                players_count=COALESCE((SELECT COUNT(*) FROM week_players wp WHERE wp.week_start=weeks.week_start),0),
                updated_at=?
        """, (now,))
        # V6.8.2 transition: previous builds counted the open/current week
        # in lifetime immediately. Normalize that one time, then mark closed
        # historical weeks as already rolled over so they are never double-added.
        try:
            from datetime import date, timedelta
            current_monday = date.today() - timedelta(days=date.today().weekday())
            current_week = current_monday.isoformat()
            marker = self.conn.execute("SELECT 1 FROM migration_meta WHERE key=?", ("v682_activity_rollover",)).fetchone()
            if not marker:
                self.conn.execute("UPDATE players SET total_activity=MAX(0,total_activity-COALESCE((SELECT activity FROM week_players wp WHERE wp.player_id=players.player_id AND wp.week_start=?),0))", (current_week,))
                self.conn.execute("UPDATE weeks SET rollover_done=1, rollover_at=COALESCE(rollover_at, ?) WHERE week_start < ?", ("migrated_v682", current_week))
                self.conn.execute("INSERT INTO migration_meta(key,value) VALUES(?,?)", ("v682_activity_rollover", now))
        except Exception:
            pass
        self.conn.execute("UPDATE weeks SET published=0,publish_started_at=NULL WHERE published=2 AND publish_started_at < datetime('now','-15 minutes')")
        self.seed_achievements()
        self.conn.commit()

    # ---------------- players ----------------
    def get_player(self, player_id: str):
        return self.conn.execute("SELECT * FROM players WHERE player_id=?", (str(player_id),)).fetchone()

    def get_player_by_telegram(self, telegram_id: int):
        return self.conn.execute("SELECT * FROM players WHERE telegram_id=?", (int(telegram_id),)).fetchone()

    def get_players_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]

    def get_players_page(self, limit: int, offset: int):
        return self.conn.execute("SELECT * FROM players ORDER BY nick COLLATE NOCASE LIMIT ? OFFSET ?", (limit, offset)).fetchall()

    def get_all_players(self):
        return self.conn.execute("SELECT * FROM players ORDER BY nick COLLATE NOCASE").fetchall()

    def update_telegram_username(self, telegram_id: int, username: Optional[str]):
        self.conn.execute("UPDATE players SET telegram_username=?, updated_at=? WHERE telegram_id=?", (username, self._now(), telegram_id))
        self.conn.commit()

    def _api_fields(self, api_data):
        """Extract API-owned player fields from both normalized and raw API JSON."""
        api_data = api_data or {}
        basic = api_data.get("basicInfo") or {}
        clan = api_data.get("clanBasicInfo") or {}

        region = api_data.get("region") or basic.get("region")
        level = api_data.get("level", basic.get("level", 0))
        likes = api_data.get("likes", basic.get("liked", 0))
        avatar_url = api_data.get("avatar_url") or api_data.get("avatarUrl") or basic.get("avatarUrl")
        guild_id = api_data.get("guild_id") or api_data.get("clanId") or clan.get("clanId")
        guild_name = api_data.get("guild_name") or api_data.get("clanName") or clan.get("clanName")
        guild_level = api_data.get("guild_level", api_data.get("clanLevel", clan.get("clanLevel", 0)))
        guild_members = api_data.get("guild_members", api_data.get("memberNum", clan.get("memberNum", 0)))
        guild_capacity = api_data.get("guild_capacity", api_data.get("capacity", clan.get("capacity", 0)))
        guild_owner = api_data.get("guild_owner") or api_data.get("captainId") or clan.get("captainId")
        is_banned = api_data.get("is_banned", False)

        return (
            region, level or 0, likes or 0, avatar_url,
            str(guild_id) if guild_id is not None else None, guild_name, guild_level or 0,
            guild_members or 0, guild_capacity or 0,
            str(guild_owner) if guild_owner is not None else None,
            json.dumps(api_data, ensure_ascii=False), 1 if is_banned else 0, self._now()
        )

    def register_player(self, player_id: str, nick: str, telegram_id: int, telegram_username=None, api_data=None):
        player_id, nick = str(player_id).strip(), str(nick).strip()
        existing = self.get_player(player_id)
        existing_tg = self.get_player_by_telegram(telegram_id)
        if existing and existing["telegram_id"] and existing["telegram_id"] != telegram_id:
            raise ValueError("Этот Free Fire ID уже привязан к другому Telegram.")
        if not existing and existing_tg:
            raise ValueError("Этот Telegram уже привязан к другому Free Fire ID.")
        now = self._now()
        fields = self._api_fields(api_data)
        if existing:
            self.conn.execute("""UPDATE players SET nick=?, telegram_id=?, telegram_username=?, region=?, level=?, likes=?, avatar_url=?, guild_id=?, guild_name=?, guild_level=?, guild_members=?, guild_capacity=?, guild_owner=?, api_data=?, is_banned=?, last_api_check=?, updated_at=? WHERE player_id=?""",
                (nick, telegram_id, telegram_username, *fields, now, player_id))
        else:
            self.conn.execute("""INSERT INTO players(player_id,nick,telegram_id,telegram_username,total_activity,weeks_count,region,level,likes,avatar_url,guild_id,guild_name,guild_level,guild_members,guild_capacity,guild_owner,api_data,is_banned,last_api_check,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (player_id,nick,telegram_id,telegram_username,0,0,*fields,now,now))
        self.conn.commit()

    def add_or_update_player(self, player_id, nick, telegram_id=None, telegram_username=None, api_data=None):
        player_id, nick = str(player_id).strip(), str(nick).strip()
        existing = self.get_player(player_id)
        if existing:
            if telegram_id is not None:
                other = self.get_player_by_telegram(telegram_id)
                if other and other["player_id"] != player_id:
                    raise ValueError("Этот Telegram уже привязан к другому игроку.")
            self.conn.execute("UPDATE players SET nick=?, telegram_id=?, telegram_username=?, updated_at=? WHERE player_id=?", (nick,telegram_id,telegram_username,self._now(),player_id))
        else:
            now=self._now(); fields=self._api_fields(api_data)
            self.conn.execute("""INSERT INTO players(player_id,nick,telegram_id,telegram_username,total_activity,weeks_count,region,level,likes,avatar_url,guild_id,guild_name,guild_level,guild_members,guild_capacity,guild_owner,api_data,is_banned,last_api_check,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (player_id,nick,telegram_id,telegram_username,0,0,*fields,now,now))
        self.conn.commit()

    def update_player_profile(self, player_id: str, api_data: dict):
        """Synchronize the stored Free Fire profile without touching activity.

        API profile fields (nickname, level, likes, guild data, ban flag, raw
        API payload, etc.) are refreshed.  Internal bot state is deliberately
        preserved: Telegram binding, ``total_activity``, ``weeks_count`` and
        every weekly ``week_players.activity`` value are never overwritten by
        a profile refresh.
        """
        player_id = str(player_id).strip()
        p = self.get_player(player_id)
        if not p:
            raise ValueError("Игрок не найден.")

        raw = api_data or {}
        basic = raw.get("basicInfo") or {}
        nickname = raw.get("nickname") or basic.get("nickname") or p["nick"]
        nickname = str(nickname).strip() or p["nick"]

        # _api_fields contains only API-owned profile columns.  Do not add
        # total_activity / weeks_count / telegram fields to this update.
        (region, level, likes, avatar_url, guild_id, guild_name, guild_level,
         guild_members, guild_capacity, guild_owner, api_json, is_banned,
         last_api_check) = self._api_fields(raw)
        now = self._now()
        self.conn.execute(
            """UPDATE players SET
                nick=?, region=?, level=?, likes=?, avatar_url=?,
                guild_id=?, guild_name=?, guild_level=?, guild_members=?,
                guild_capacity=?, guild_owner=?, api_data=?, is_banned=?,
                last_api_check=?, updated_at=?
               WHERE player_id=?""",
            (nickname, region, level, likes, avatar_url, guild_id, guild_name,
             guild_level, guild_members, guild_capacity, guild_owner, api_json,
             is_banned, last_api_check, now, player_id),
        )

        # Weekly snapshots keep the current nickname for reports, but their
        # activity value is intentionally untouched.
        self.conn.execute(
            "UPDATE week_players SET nick=?, updated_at=? WHERE player_id=?",
            (nickname, now, player_id),
        )
        self.conn.commit()

    def set_coin_balance(self, telegram_id: int, amount: int, admin_id: int | None = None):
        amount = int(amount)
        if amount < 0:
            raise ValueError("Количество коинов не может быть отрицательным.")
        now = self._now()
        old = self.get_coin_balance(telegram_id)
        delta = amount - old
        self.conn.execute("INSERT INTO coin_balances(telegram_id,coins,updated_at) VALUES(?,?,?) ON CONFLICT(telegram_id) DO UPDATE SET coins=excluded.coins,updated_at=excluded.updated_at", (int(telegram_id), amount, now))
        if delta:
            self.conn.execute("INSERT INTO coin_transactions(telegram_id,amount,reason,created_at,meta) VALUES(?,?,?,?,?)", (int(telegram_id), delta, "admin_set", now, json.dumps({"admin_id": admin_id, "old": old, "new": amount}, ensure_ascii=False)))
        self.conn.commit()
        return amount

    def rollover_week(self, week_start: str, coins_per_100: int = 60):
        """Atomically close a week: add its weekly activity to lifetime and award coins once."""
        now = self._now()
        c = self.conn.cursor()
        week = c.execute("SELECT * FROM weeks WHERE week_start=?", (week_start,)).fetchone()
        if not week:
            return {"done": False, "players": 0, "coins": 0, "activity": 0}
        if int(week["rollover_done"] or 0):
            return {"done": True, "players": 0, "coins": 0, "activity": 0}
        rows = c.execute("SELECT wp.player_id, wp.activity, p.telegram_id FROM week_players wp LEFT JOIN players p ON p.player_id=wp.player_id WHERE wp.week_start=?", (week_start,)).fetchall()
        total_activity = 0
        total_coins = 0
        players = 0
        try:
            for r in rows:
                activity = max(0, int(r["activity"] or 0))
                if activity <= 0:
                    continue
                players += 1
                total_activity += activity
                c.execute("UPDATE players SET total_activity=total_activity+?, updated_at=? WHERE player_id=?", (activity, now, str(r["player_id"])))
                tg = r["telegram_id"]
                if tg:
                    units = activity // 100
                    award_row = c.execute("SELECT units_awarded FROM activity_coin_awards WHERE week_start=? AND player_id=?", (week_start, str(r["player_id"]))).fetchone()
                    old_units = int(award_row[0]) if award_row else 0
                    delta_units = max(0, units - old_units)
                    if delta_units:
                        amount = delta_units * int(coins_per_100)
                        c.execute("INSERT INTO activity_coin_awards(week_start,player_id,units_awarded,updated_at) VALUES(?,?,?,?) ON CONFLICT(week_start,player_id) DO UPDATE SET units_awarded=excluded.units_awarded,updated_at=excluded.updated_at", (week_start, str(r["player_id"]), units, now))
                        c.execute("INSERT INTO coin_balances(telegram_id,coins,updated_at) VALUES(?,?,?) ON CONFLICT(telegram_id) DO UPDATE SET coins=coins+excluded.coins,updated_at=excluded.updated_at", (int(tg), amount, now))
                        c.execute("INSERT INTO coin_transactions(telegram_id,amount,reason,created_at,meta) VALUES(?,?,?,?,?)", (int(tg), amount, "activity_weekly", now, json.dumps({"week_start":week_start,"player_id":str(r["player_id"]),"activity":activity,"units":delta_units}, ensure_ascii=False)))
                        total_coins += amount
            c.execute("UPDATE weeks SET rollover_done=1, rollover_at=?, updated_at=? WHERE week_start=? AND rollover_done=0", (now, now, week_start))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {"done": True, "players": players, "coins": total_coins, "activity": total_activity}

    def get_week_activity_report(self, week_start: str):
        return self.conn.execute(
            """SELECT p.player_id, p.nick, p.telegram_id, p.telegram_username, COALESCE(wp.activity,0) AS activity
               FROM players p
               LEFT JOIN week_players wp ON wp.player_id=p.player_id AND wp.week_start=?
               ORDER BY activity DESC, p.nick COLLATE NOCASE ASC""",
            (week_start,),
        ).fetchall()

    def add_shop_product(self, name: str, price: int, description: str = ''):
        now=self._now()
        cur=self.conn.execute("INSERT INTO shop_products(name,price,description,active,created_at,updated_at) VALUES(?,?,?,1,?,?)", (str(name).strip(),int(price),str(description).strip(),now,now))
        self.conn.commit()
        return cur.lastrowid

    def get_shop_products(self, active_only=True):
        q="SELECT * FROM shop_products"
        if active_only: q += " WHERE active=1"
        return self.conn.execute(q+" ORDER BY product_id").fetchall()

    def deactivate_shop_product(self, product_id: int):
        cur=self.conn.execute("UPDATE shop_products SET active=0,updated_at=? WHERE product_id=? AND active=1", (self._now(),int(product_id)))
        self.conn.commit()
        return cur.rowcount > 0

    def unbind_player(self, player_id):
        self.conn.execute("UPDATE players SET telegram_id=NULL,telegram_username=NULL,updated_at=? WHERE player_id=?", (self._now(),str(player_id))); self.conn.commit()

    def delete_player(self, player_id):
        p=self.get_player(player_id)
        if not p: return False
        self.conn.execute("DELETE FROM players WHERE player_id=?", (str(player_id),)); self.conn.commit(); return True

    # ---------------- weeks ----------------
    def week_exists(self, week_start): return self.get_week(week_start) is not None
    def ensure_week(self, week_start, week_end):
        """Создать неделю только в момент её фактического начала (04:00 МСК)."""
        now = self._now()
        self.conn.execute(
            "INSERT OR IGNORE INTO weeks(week_start,week_end,created_at,updated_at) VALUES(?,?,?,?)",
            (week_start, week_end, now, now),
        )
        self.conn.commit()
        return self.get_week(week_start)

    def get_week(self, week_start): return self.conn.execute("SELECT * FROM weeks WHERE week_start=?",(week_start,)).fetchone()
    def get_latest_week(self): return self.conn.execute("SELECT * FROM weeks ORDER BY week_start DESC LIMIT 1").fetchone()
    def get_previous_week(self, week_start): return self.conn.execute("SELECT * FROM weeks WHERE week_start<? ORDER BY week_start DESC LIMIT 1",(week_start,)).fetchone()
    def get_history(self, limit=30): return self.conn.execute("SELECT * FROM weeks ORDER BY week_start DESC LIMIT ?",(limit,)).fetchall()
    def get_unpublished_completed_weeks(self,today): return self.conn.execute("SELECT * FROM weeks WHERE published=0 AND week_end<? ORDER BY week_start ASC",(today,)).fetchall()

    def save_week(self, week_start, week_end, entries):
        now = self._now()
        c = self.conn.cursor()
        try:
            c.execute("INSERT OR IGNORE INTO weeks(week_start,week_end,created_at,updated_at) VALUES(?,?,?,?)", (week_start, week_end, now, now))
            for e in entries:
                pid = str(e.player_id)
                p = self.get_player(pid)
                if not p:
                    c.execute("INSERT INTO players(player_id,nick,created_at,updated_at) VALUES(?,?,?,?)", (pid, e.nick, now, now))
                else:
                    c.execute("UPDATE players SET nick=?,updated_at=? WHERE player_id=?", (e.nick, now, pid))
                old = c.execute("SELECT activity FROM week_players WHERE week_start=? AND player_id=?", (week_start, pid)).fetchone()
                if old:
                    c.execute("UPDATE week_players SET nick=?,activity=?,rank_name=?,updated_at=? WHERE week_start=? AND player_id=?", (e.nick, int(e.activity), None, now, week_start, pid))
                else:
                    c.execute("INSERT INTO week_players(week_start,player_id,nick,activity,created_at,updated_at) VALUES(?,?,?,?,?,?)", (week_start, pid, e.nick, int(e.activity), now, now))
                    c.execute("UPDATE players SET weeks_count=weeks_count+1 WHERE player_id=?", (pid,))
            self._recalculate_week(week_start, c)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def save_monitoring_snapshot(self, week_start, week_end, entries):
        """Save an absolute monitoring snapshot for an open week.

        Monitoring values replace the weekly snapshot; they never touch lifetime
        activity or Telegram bindings. The latest prior snapshot in the same
        week is used only for anomaly detection.
        """
        now = self._now()
        anomalies = []
        updated = 0
        c = self.conn.cursor()
        try:
            c.execute("INSERT OR IGNORE INTO weeks(week_start,week_end,created_at,updated_at) VALUES(?,?,?,?)", (week_start, week_end, now, now))
            week = c.execute("SELECT rollover_done FROM weeks WHERE week_start=?", (week_start,)).fetchone()
            if week and int(week[0] or 0):
                raise ValueError("Эта неделя уже закрыта. Данные мониторинга больше не принимаются.")
            for e in entries:
                pid = str(e.player_id)
                activity = max(0, int(e.activity))
                previous = c.execute("SELECT activity FROM monitoring_snapshots WHERE week_start=? AND player_id=? ORDER BY id DESC LIMIT 1", (week_start, pid)).fetchone()
                if previous is not None:
                    prev = int(previous[0])
                    if prev >= 1000 and (activity >= max(5000, prev * 4) or activity - prev >= 10000 or activity < prev):
                        reason = f"Аномальный скачок: {prev} → {activity}" if activity >= prev else f"Активность уменьшилась: {prev} → {activity}"
                        anomalies.append((pid, prev, activity, reason))
                p = c.execute("SELECT player_id FROM players WHERE player_id=?", (pid,)).fetchone()
                if not p:
                    c.execute("INSERT INTO players(player_id,nick,created_at,updated_at) VALUES(?,?,?,?)", (pid, e.nick, now, now))
                else:
                    c.execute("UPDATE players SET nick=?,updated_at=? WHERE player_id=?", (e.nick, now, pid))
                old = c.execute("SELECT 1 FROM week_players WHERE week_start=? AND player_id=?", (week_start, pid)).fetchone()
                if old:
                    c.execute("UPDATE week_players SET nick=?,activity=?,updated_at=? WHERE week_start=? AND player_id=?", (e.nick, activity, now, week_start, pid))
                else:
                    c.execute("INSERT INTO week_players(week_start,player_id,nick,activity,created_at,updated_at) VALUES(?,?,?,?,?,?)", (week_start,pid,e.nick,activity,now,now))
                    c.execute("UPDATE players SET weeks_count=weeks_count+1 WHERE player_id=?", (pid,))
                c.execute("INSERT INTO monitoring_snapshots(week_start,player_id,activity,game_total,created_at) VALUES(?,?,?,?,?)", (week_start,pid,activity,getattr(e,'game_total',None),now))
                updated += 1
            self._recalculate_week(week_start, c)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {"updated": updated, "anomalies": anomalies}

    def upsert_monitoring_activity(self, week_start, week_end, player_id, nick, activity, game_total=None):
        """Backward-compatible single-player monitoring update.

        This is deliberately routed through the V6.8.4 snapshot path so it
        updates only the current week's activity and never lifetime activity
        or coins. Repeated calls remain snapshots of the same open week.
        """
        entry = type("MonitoringEntryCompat", (), {
            "player_id": str(player_id),
            "nick": str(nick),
            "activity": int(activity),
            "game_total": game_total,
        })()
        return self.save_monitoring_snapshot(week_start, week_end, [entry])

    def add_activity(self, week_start, week_end, player_id, delta):
        player_id = str(player_id).strip()
        p = self.get_player(player_id)
        if not p:
            raise ValueError("Игрок с таким Free Fire ID не зарегистрирован.")
        delta = int(delta)
        now = self._now()
        c = self.conn.cursor()
        try:
            c.execute("INSERT OR IGNORE INTO weeks(week_start,week_end,created_at,updated_at) VALUES(?,?,?,?)", (week_start, week_end, now, now))
            closed = c.execute("SELECT rollover_done FROM weeks WHERE week_start=?", (week_start,)).fetchone()
            if closed and int(closed[0] or 0):
                raise ValueError("Эта неделя уже закрыта. Изменение активности после rollover запрещено, чтобы не нарушить начисление коинов и активности за всё время.")
            row = c.execute("SELECT activity FROM week_players WHERE week_start=? AND player_id=?", (week_start, player_id)).fetchone()
            old = int(row[0]) if row else 0
            new = old + delta
            if new < 0:
                raise ValueError("Активность не может быть отрицательной.")
            if row:
                c.execute("UPDATE week_players SET activity=?,nick=?,updated_at=? WHERE week_start=? AND player_id=?", (new, p["nick"], now, week_start, player_id))
            else:
                c.execute("INSERT INTO week_players(week_start,player_id,nick,activity,created_at,updated_at) VALUES(?,?,?,?,?,?)", (week_start, player_id, p["nick"], new, now, now))
                c.execute("UPDATE players SET weeks_count=weeks_count+1 WHERE player_id=?", (player_id,))
            self._recalculate_week(week_start, c)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {"old_activity": old, "new_activity": new, "delta": delta, "total_activity": int(p["total_activity"] or 0)}

    def set_week_activity(self, week_start, week_end, player_id, new_activity):
        player_id = str(player_id).strip()
        new_activity = int(new_activity)
        if new_activity < 0:
            raise ValueError("Активность не может быть отрицательной.")
        p = self.get_player(player_id)
        if not p:
            raise ValueError("Игрок с таким Free Fire ID не зарегистрирован.")
        row = self.get_week_player(week_start, player_id)
        old = int(row["activity"]) if row else 0
        return self.add_activity(week_start, week_end, player_id, new_activity - old)

    def _recalculate_week(self,week_start,cursor=None):
        c=cursor or self.conn.cursor(); row=c.execute("SELECT COALESCE(SUM(activity),0),COUNT(*) FROM week_players WHERE week_start=?",(week_start,)).fetchone(); c.execute("UPDATE weeks SET total_activity=?,players_count=?,updated_at=? WHERE week_start=?",(row[0],row[1],self._now(),week_start))
        if cursor is None:self.conn.commit()

    def mark_publish_requested(self, week_start):
        now = self._now()
        self.conn.execute("UPDATE weeks SET publish_requested_at=COALESCE(publish_requested_at,?),updated_at=? WHERE week_start=?", (now, now, week_start))
        self.conn.commit()

    def confirm_publish(self, week_start):
        now = self._now()
        self.conn.execute("UPDATE weeks SET publish_confirmed_at=COALESCE(publish_confirmed_at,?),updated_at=? WHERE week_start=?", (now, now, week_start))
        self.conn.commit()

    def claim_publish(self, week_start):
        now = self._now()
        cur = self.conn.execute("UPDATE weeks SET published=2,publish_started_at=?,updated_at=? WHERE week_start=? AND published=0 AND publish_confirmed_at IS NOT NULL", (now, now, week_start))
        self.conn.commit()
        return cur.rowcount == 1

    def release_publish_claim(self, week_start):
        self.conn.execute("UPDATE weeks SET published=0,publish_started_at=NULL,updated_at=? WHERE week_start=? AND published=2", (self._now(), week_start))
        self.conn.commit()

    def mark_published(self, week_start):
        self.conn.execute("UPDATE weeks SET published=1,published_at=COALESCE(published_at,?),updated_at=? WHERE week_start=? AND published IN (0,2)", (self._now(), self._now(), week_start))
        self.conn.commit()

    # ---------------- queries ----------------
    def get_week_players(self,week_start): return self.conn.execute("SELECT wp.*,p.telegram_id,p.telegram_username,p.total_activity FROM week_players wp LEFT JOIN players p ON p.player_id=wp.player_id WHERE wp.week_start=? ORDER BY wp.activity DESC,wp.player_id",(week_start,)).fetchall()
    def get_week_player(self,week_start,player_id): return self.conn.execute("SELECT wp.*,p.telegram_id,p.telegram_username FROM week_players wp LEFT JOIN players p ON p.player_id=wp.player_id WHERE wp.week_start=? AND wp.player_id=?",(week_start,str(player_id))).fetchone()
    def get_week_player_position(self,week_start,player_id):
        rows=self.get_week_players(week_start)
        for i,r in enumerate(rows,1):
            if r["player_id"]==str(player_id): return i
        return None
    def get_low_activity_players(self,week_start,limit,threshold): return self.conn.execute("SELECT wp.player_id,wp.nick,wp.activity,p.telegram_id,p.telegram_username FROM week_players wp LEFT JOIN players p ON p.player_id=wp.player_id WHERE wp.week_start=? AND wp.activity<? ORDER BY wp.activity ASC LIMIT ?",(week_start,threshold,limit)).fetchall()
    def get_top_players(self, limit=20, week_start=None):
        if week_start:
            return self.conn.execute("SELECT wp.*,p.telegram_username,p.total_activity FROM week_players wp LEFT JOIN players p ON p.player_id=wp.player_id WHERE wp.week_start=? ORDER BY wp.activity DESC,wp.player_id LIMIT ?", (week_start, limit)).fetchall()
        return self.conn.execute("SELECT * FROM players ORDER BY total_activity DESC LIMIT ?", (limit,)).fetchall()
    def get_player_history(self,player_id,limit=5): return self.conn.execute("SELECT * FROM week_players WHERE player_id=? ORDER BY week_start DESC LIMIT ?",(str(player_id),limit)).fetchall()
    def get_all_time_total(self): return self.conn.execute("SELECT COALESCE(SUM(total_activity),0) FROM players").fetchone()[0]

    def set_total_activity(self, player_id: str, total_activity: int):
        """Set a player's lifetime activity without touching any weekly record."""
        player_id = str(player_id).strip()
        total_activity = int(total_activity)
        if total_activity < 0:
            raise ValueError("Общее количество очков не может быть отрицательным.")
        player = self.get_player(player_id)
        if not player:
            raise ValueError("Игрок не найден.")
        old_value = int(player["total_activity"] or 0)
        now = self._now()
        self.conn.execute(
            "UPDATE players SET total_activity=?, updated_at=? WHERE player_id=?",
            (total_activity, now, player_id),
        )
        self.conn.commit()
        return {
            "old_total": old_value,
            "new_total": total_activity,
            "delta": total_activity - old_value,
        }

    def adjust_total_activity(self, player_id: str, delta: int):
        """Adjust lifetime activity without touching any weekly record."""
        player_id = str(player_id).strip()
        player = self.get_player(player_id)
        if not player:
            raise ValueError("Игрок не найден.")
        old_value = int(player["total_activity"] or 0)
        new_value = old_value + int(delta)
        if new_value < 0:
            raise ValueError("Общее количество очков не может быть отрицательным.")
        return self.set_total_activity(player_id, new_value)

    def get_player_count_with_week(self, week_start):
        return self.conn.execute("SELECT COUNT(*) FROM week_players WHERE week_start=?", (week_start,)).fetchone()[0]

    def get_week_totals(self):
        return self.conn.execute("SELECT COALESCE(SUM(total_activity),0) AS lifetime_total, COUNT(*) AS players_count, COALESCE(SUM(CASE WHEN total_activity>0 THEN 1 ELSE 0 END),0) AS active_players FROM players").fetchone()

    # ---------------- rewards ----------------
    def calculate_rewards(self,week_start,threshold_low=5000,threshold_high=12000):
        rows=self.get_week_players(week_start); now=self._now(); c=self.conn.cursor()
        for r in rows:
            if r["activity"]>=threshold_high: typ,req="monthly",threshold_high
            elif r["activity"]>=threshold_low: typ,req="weekly",threshold_low
            else: continue
            c.execute("INSERT INTO rewards(week_start,player_id,reward_type,required_activity,actual_activity,status,created_at,updated_at) VALUES(?,?,?,?,?,'earned',?,?) ON CONFLICT(week_start,player_id) DO UPDATE SET reward_type=excluded.reward_type,required_activity=excluded.required_activity,actual_activity=excluded.actual_activity,updated_at=excluded.updated_at",(week_start,r["player_id"],typ,req,r["activity"],now,now))
        self.conn.commit()
        return self.get_rewards(week_start)
    def get_rewards(self,week_start,status=None):
        q="SELECT r.*,p.nick FROM rewards r LEFT JOIN players p ON p.player_id=r.player_id WHERE r.week_start=?"; args=[week_start]
        if status: q+=" AND r.status=?"; args.append(status)
        return self.conn.execute(q+" ORDER BY required_activity DESC,actual_activity DESC",args).fetchall()
    def mark_reward_awarded(self,reward_id,admin_id):
        self.conn.execute("UPDATE rewards SET status='awarded',awarded_at=?,awarded_by=?,updated_at=? WHERE reward_id=? AND status!='awarded'",(self._now(),admin_id,self._now(),reward_id)); self.conn.commit()
    def cancel_reward(self,reward_id): self.conn.execute("UPDATE rewards SET status='cancelled',updated_at=? WHERE reward_id=?",(self._now(),reward_id)); self.conn.commit()

    # ---------------- logs ----------------
    def log(self, action, actor_id=None, details=None):
        """Append an immutable audit event. Details may be a dict and is stored as JSON."""
        if isinstance(details, dict):
            details = json.dumps(details, ensure_ascii=False, default=str)
        self.conn.execute(
            "INSERT INTO logs(created_at,actor_id,action,details) VALUES(?,?,?,?)",
            (self._now(), actor_id, action, details),
        )
        self.conn.commit()

    def get_logs(self, limit=20, offset=0):
        return self.conn.execute(
            "SELECT * FROM logs ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()

    def clear_logs(self):
        self.conn.execute("DELETE FROM logs")
        self.conn.commit()


    # ---------------- admin roles ----------------
    def get_admin_role(self, telegram_id):
        row=self.conn.execute("SELECT role_level FROM admin_roles WHERE telegram_id=?",(int(telegram_id),)).fetchone()
        return int(row[0]) if row else 1

    def set_admin_role(self, telegram_id, role_level):
        role_level = int(role_level)
        if not 1 <= role_level <= 8:
            raise ValueError("Недопустимый уровень роли.")
        self.conn.execute(
            "INSERT INTO admin_roles(telegram_id,role_level,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(telegram_id) DO UPDATE SET role_level=excluded.role_level,updated_at=excluded.updated_at",
            (int(telegram_id), role_level, self._now()),
        )
        self.conn.commit()

    def delete_admin_role(self, telegram_id):
        self.conn.execute("DELETE FROM admin_roles WHERE telegram_id=?", (int(telegram_id),))
        self.conn.commit()

    def get_admin_roles(self):
        return self.conn.execute("SELECT * FROM admin_roles ORDER BY role_level DESC, telegram_id").fetchall()

    def get_player_by_telegram_username(self, username):
        username = str(username).lstrip("@")
        return self.conn.execute(
            "SELECT * FROM players WHERE lower(telegram_username)=lower(?) LIMIT 1",
            (username,)
        ).fetchone()

    # ---------------- referrals / coins ----------------
    def get_referral_pending(self, telegram_id):
        return self.conn.execute("SELECT * FROM referrals WHERE telegram_id=? AND status='pending'", (int(telegram_id),)).fetchone()

    def set_referral_pending(self, telegram_id, referrer_telegram_id, referrer_player_id=None):
        if int(telegram_id) == int(referrer_telegram_id):
            return False
        existing = self.conn.execute("SELECT * FROM referrals WHERE telegram_id=?", (int(telegram_id),)).fetchone()
        if existing:
            return False
        self.conn.execute("INSERT INTO referrals(telegram_id,referrer_telegram_id,referrer_player_id,status,created_at) VALUES(?,?,?,'pending',?)", (int(telegram_id), int(referrer_telegram_id), referrer_player_id, self._now()))
        self.conn.commit()
        return True

    def complete_referral(self, telegram_id):
        row = self.get_referral_pending(telegram_id)
        if not row:
            return None
        self.conn.execute("UPDATE referrals SET status='completed',completed_at=? WHERE telegram_id=? AND status='pending'", (self._now(), int(telegram_id)))
        self.conn.commit()
        return row

    def get_coin_balance(self, telegram_id):
        row = self.conn.execute("SELECT coins FROM coin_balances WHERE telegram_id=?", (int(telegram_id),)).fetchone()
        return int(row[0]) if row else 0

    def add_coins(self, telegram_id, amount, reason, meta=None):
        amount = int(amount)
        if amount == 0:
            return self.get_coin_balance(telegram_id)
        now = self._now()
        c = self.conn.cursor()
        try:
            c.execute("INSERT INTO coin_balances(telegram_id,coins,updated_at) VALUES(?,?,?) ON CONFLICT(telegram_id) DO UPDATE SET coins=coins+excluded.coins,updated_at=excluded.updated_at", (int(telegram_id), amount, now))
            balance = c.execute("SELECT coins FROM coin_balances WHERE telegram_id=?", (int(telegram_id),)).fetchone()[0]
            if balance < 0:
                raise ValueError("Недостаточно коинов.")
            c.execute("INSERT INTO coin_transactions(telegram_id,amount,reason,created_at,meta) VALUES(?,?,?,?,?)", (int(telegram_id), amount, reason, now, meta))
            self.conn.commit()
            return int(balance)
        except Exception:
            self.conn.rollback()
            raise

    def spend_coins(self, telegram_id, amount, reason, meta=None):
        amount = int(amount)
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной.")
        balance = self.get_coin_balance(telegram_id)
        if balance < amount:
            raise ValueError(f"Недостаточно коинов: нужно {amount}, есть {balance}.")
        return self.add_coins(telegram_id, -amount, reason, meta)

    def get_coin_transactions(self, telegram_id, limit=10):
        return self.conn.execute("SELECT * FROM coin_transactions WHERE telegram_id=? ORDER BY id DESC LIMIT ?", (int(telegram_id), int(limit))).fetchall()

    # ---------------- shop requests ----------------
    def create_shop_request(self, telegram_id, request_type, coins_cost, reward_amount):
        cur=self.conn.execute("INSERT INTO shop_requests(telegram_id,request_type,coins_cost,reward_amount,status,created_at) VALUES(?,?,?,?,?,?)", (int(telegram_id),request_type,int(coins_cost),int(reward_amount),'pending',self._now()))
        self.conn.commit()
        return cur.lastrowid

    def get_shop_requests(self, status='pending', limit=50):
        return self.conn.execute("SELECT * FROM shop_requests WHERE status=? ORDER BY request_id DESC LIMIT ?", (status,int(limit))).fetchall()

    def get_shop_request(self, request_id):
        return self.conn.execute("SELECT * FROM shop_requests WHERE request_id=?", (int(request_id),)).fetchone()

    def handle_shop_request(self, request_id, admin_id, approve=True):
        row=self.get_shop_request(request_id)
        if not row or row['status']!='pending':
            raise ValueError('Заявка уже обработана или не найдена.')
        if approve:
            self.spend_coins(row['telegram_id'], row['coins_cost'], 'shop_exchange', json.dumps({'request_id':request_id,'type':row['request_type']},ensure_ascii=False))
            status='approved'
        else:
            status='declined'
        self.conn.execute("UPDATE shop_requests SET status=?,handled_at=?,handled_by=? WHERE request_id=? AND status='pending'", (status,self._now(),int(admin_id),int(request_id)))
        self.conn.commit()
        return status

    # ---------------- moderation ----------------
    def add_warning(self, chat_id, user_id, admin_id, reason, points=1):
        now = self._now()
        self.conn.execute("INSERT INTO moderation_warnings(chat_id,user_id,admin_id,reason,points,created_at) VALUES(?,?,?,?,?,?)", (int(chat_id),int(user_id),int(admin_id),reason,int(points),now))
        self.conn.commit()
        return self.get_warning_count(chat_id, user_id)

    def get_warning_count(self, chat_id, user_id):
        row = self.conn.execute("SELECT COALESCE(SUM(points),0) FROM moderation_warnings WHERE chat_id=? AND user_id=? AND active=1 AND datetime(created_at) >= datetime('now','-7 days')", (int(chat_id),int(user_id))).fetchone()
        return int(row[0])

    def get_warnings(self, chat_id, user_id, limit=10):
        return self.conn.execute("SELECT * FROM moderation_warnings WHERE chat_id=? AND user_id=? AND active=1 AND datetime(created_at) >= datetime('now','-7 days') ORDER BY id DESC LIMIT ?", (int(chat_id),int(user_id),int(limit))).fetchall()

    def clear_warnings(self, chat_id, user_id):
        self.conn.execute("UPDATE moderation_warnings SET active=0 WHERE chat_id=? AND user_id=? AND active=1", (int(chat_id),int(user_id)))
        self.conn.commit()

    def set_ban(self, chat_id, user_id, admin_id, reason, banned_until=None):
        now = self._now()
        self.conn.execute("INSERT INTO moderation_bans(chat_id,user_id,admin_id,reason,banned_until,active,created_at) VALUES(?,?,?,?,?,1,?) ON CONFLICT(chat_id,user_id) DO UPDATE SET admin_id=excluded.admin_id,reason=excluded.reason,banned_until=excluded.banned_until,active=1,created_at=excluded.created_at", (int(chat_id),int(user_id),int(admin_id),reason,banned_until,now))
        self.conn.commit()

    def clear_ban(self, chat_id, user_id):
        self.conn.execute("UPDATE moderation_bans SET active=0 WHERE chat_id=? AND user_id=?", (int(chat_id),int(user_id)))
        self.conn.commit()

    def get_ban(self, chat_id, user_id):
        return self.conn.execute("SELECT * FROM moderation_bans WHERE chat_id=? AND user_id=? AND active=1", (int(chat_id),int(user_id))).fetchone()

    def get_kv_roster(self, slot):
        row=self.conn.execute("SELECT * FROM kv_rosters WHERE slot=?", (int(slot),)).fetchone()
        if not row: return []
        try: return json.loads(row["members"] or "[]")
        except Exception: return []
    def set_kv_roster(self, slot, members):
        self.conn.execute("INSERT INTO kv_rosters(slot,members,updated_at) VALUES(?,?,?) ON CONFLICT(slot) DO UPDATE SET members=excluded.members,updated_at=excluded.updated_at", (int(slot),json.dumps(list(members),ensure_ascii=False),self._now())); self.conn.commit()
    def get_kv_rosters(self):
        return {slot:self.get_kv_roster(slot) for slot in (1,2,3)}

    def create_kv(self, title, match_date, match_time, purpose, enemy_guild, enemy_members, our_members, created_by, proposer_id=None):
        now=self._now(); cur=self.conn.execute("INSERT INTO kv_matches(title,match_date,match_time,purpose,enemy_guild,enemy_members,our_members,created_by,proposer_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (title,match_date,match_time,purpose,enemy_guild,json.dumps(enemy_members,ensure_ascii=False),json.dumps(our_members,ensure_ascii=False),created_by,proposer_id,now,now)); self.conn.commit(); return cur.lastrowid
    def get_kvs(self, status=None, limit=10):
        q="SELECT * FROM kv_matches"; args=[]
        if status: q+=" WHERE status=?"; args.append(status)
        q+=" ORDER BY match_date,match_time,id LIMIT ?"; args.append(int(limit)); return self.conn.execute(q,args).fetchall()
    def set_kv_status(self, kv_id, status):
        self.conn.execute("UPDATE kv_matches SET status=?,updated_at=? WHERE id=?",(status,self._now(),int(kv_id))); self.conn.commit()
    def get_kv_history(self, limit=20):
        return self.conn.execute(
            "SELECT h.*, k.enemy_guild, k.our_members, k.enemy_members FROM kv_history h "
            "LEFT JOIN kv_matches k ON k.id=h.kv_id ORDER BY h.match_date DESC, h.id DESC LIMIT ?",
            (int(limit),)
        ).fetchall()

    def add_kv_history(self, kv_id, match_date, our_score, enemy_score, result):
        cur = self.conn.execute(
            "INSERT INTO kv_history(kv_id,match_date,our_score,enemy_score,result,created_at) VALUES(?,?,?,?,?,?)",
            (int(kv_id) if kv_id is not None else None, str(match_date), int(our_score), int(enemy_score), str(result), self._now())
        )
        if kv_id is not None:
            self.conn.execute("UPDATE kv_matches SET status='finished',updated_at=? WHERE id=?", (self._now(), int(kv_id)))
        self.conn.commit()
        return cur.lastrowid

    def mark_kv_warning_sent(self, kv_id):
        self.conn.execute("UPDATE kv_matches SET warning_sent=1,updated_at=? WHERE id=?", (self._now(), int(kv_id)))
        self.conn.commit()

    def get_player_by_nick(self, nick):
        return self.conn.execute(
            "SELECT * FROM players WHERE lower(nick)=lower(?) LIMIT 1", (str(nick).strip(),)
        ).fetchone()

    def create_application(self, telegram_id, username, uid, nick, why_join=None, how_found=None, extra_info=None):
        cur=self.conn.execute(
            "INSERT INTO guild_applications(telegram_id,username,uid,nick,why_join,how_found,extra_info,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(telegram_id),username,uid,nick,why_join,how_found,extra_info,self._now())
        )
        self.conn.commit(); return cur.lastrowid
    def get_applications(self,status='pending',limit=30): return self.conn.execute("SELECT * FROM guild_applications WHERE status=? ORDER BY id DESC LIMIT ?",(status,int(limit))).fetchall()
    def get_application(self, app_id): return self.conn.execute("SELECT * FROM guild_applications WHERE id=?",(int(app_id),)).fetchone()
    def set_application_status(self, app_id,status,handled_by,review_note=None):
        self.conn.execute("UPDATE guild_applications SET status=?,handled_at=?,handled_by=?,review_note=? WHERE id=?",(status,self._now(),int(handled_by),review_note,int(app_id))); self.conn.commit()
    def create_marriage(self,u1,u2,proposer):
        now=self._now(); cur=self.conn.execute("INSERT INTO marriages(user1_id,user2_id,proposer_id,created_at) VALUES(?,?,?,?)",(int(u1),int(u2),int(proposer),now)); self.conn.commit(); return cur.lastrowid
    def pending_marriage(self,u1,u2): return self.conn.execute("SELECT * FROM marriages WHERE status='pending' AND ((user1_id=? AND user2_id=?) OR (user1_id=? AND user2_id=?)) ORDER BY id DESC LIMIT 1",(int(u1),int(u2),int(u2),int(u1))).fetchone()
    def active_marriage(self,u): return self.conn.execute("SELECT * FROM marriages WHERE status='active' AND (user1_id=? OR user2_id=?) ORDER BY id DESC LIMIT 1",(int(u),int(u))).fetchone()
    def accept_marriage(self,mid): self.conn.execute("UPDATE marriages SET status='active',accepted_at=? WHERE id=? AND status='pending'",(self._now(),int(mid))); self.conn.commit()
    def divorce(self,u): self.conn.execute("UPDATE marriages SET status='divorced' WHERE status='active' AND (user1_id=? OR user2_id=?)",(int(u),int(u))); self.conn.commit()
    def log_rp_action(self,actor,target,action): self.conn.execute("INSERT INTO rp_actions(actor_id,target_id,action,created_at) VALUES(?,?,?,?)",(int(actor),int(target),action,self._now())); self.conn.commit()

    def list_active_marriages(self, limit=1000, offset=0):
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='active' ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (int(limit), int(offset))
        ).fetchall()

    def marriage_pending_for_target(self, target_id):
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='pending' AND user2_id=? ORDER BY id DESC LIMIT 1",
            (int(target_id),)
        ).fetchone()

    def marriage_by_id(self, mid):
        return self.conn.execute("SELECT * FROM marriages WHERE id=?", (int(mid),)).fetchone()

    def decline_marriage(self, mid):
        self.conn.execute("UPDATE marriages SET status='declined' WHERE id=? AND status='pending'", (int(mid),))
        self.conn.commit()

    def active_marriage_between(self, u1, u2):
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='active' AND ((user1_id=? AND user2_id=?) OR (user1_id=? AND user2_id=?)) LIMIT 1",
            (int(u1), int(u2), int(u2), int(u1))
        ).fetchone()


    # ---------------- chat-scoped marriage compatibility ----------------
    def _ensure_marriage_chat_id(self):
        cols=[r[1] for r in self.conn.execute("PRAGMA table_info(marriages)").fetchall()]
        if "chat_id" not in cols:
            self.conn.execute("ALTER TABLE marriages ADD COLUMN chat_id INTEGER")
            self.conn.commit()

    def create_chat_marriage(self, chat_id, u1, u2, proposer):
        self._ensure_marriage_chat_id()
        now=self._now()
        cur=self.conn.execute(
            "INSERT INTO marriages(user1_id,user2_id,proposer_id,created_at,chat_id) VALUES(?,?,?,?,?)",
            (int(u1),int(u2),int(proposer),now,int(chat_id))
        )
        self.conn.commit()
        return cur.lastrowid

    def active_chat_marriage(self, chat_id, u):
        self._ensure_marriage_chat_id()
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='active' AND chat_id=? AND (user1_id=? OR user2_id=?) ORDER BY id DESC LIMIT 1",
            (int(chat_id),int(u),int(u))
        ).fetchone()

    def pending_chat_marriage_for_target(self, chat_id, target_id):
        self._ensure_marriage_chat_id()
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='pending' AND chat_id=? AND user2_id=? ORDER BY id DESC LIMIT 1",
            (int(chat_id),int(target_id))
        ).fetchone()

    def pending_chat_marriage_between(self, chat_id, u1, u2):
        self._ensure_marriage_chat_id()
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='pending' AND chat_id=? AND ((user1_id=? AND user2_id=?) OR (user1_id=? AND user2_id=?)) ORDER BY id DESC LIMIT 1",
            (int(chat_id),int(u1),int(u2),int(u2),int(u1))
        ).fetchone()

    def list_chat_active_marriages(self, chat_id, limit=100, offset=0):
        self._ensure_marriage_chat_id()
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='active' AND chat_id=? ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (int(chat_id),int(limit),int(offset))
        ).fetchall()

    def active_chat_marriage_between(self, chat_id, u1, u2):
        self._ensure_marriage_chat_id()
        return self.conn.execute(
            "SELECT * FROM marriages WHERE status='active' AND chat_id=? AND ((user1_id=? AND user2_id=?) OR (user1_id=? AND user2_id=?)) LIMIT 1",
            (int(chat_id),int(u1),int(u2),int(u2),int(u1))
        ).fetchone()

    def decline_chat_marriage(self, chat_id, mid):
        self._ensure_marriage_chat_id()
        self.conn.execute("UPDATE marriages SET status='declined' WHERE id=? AND chat_id=? AND status='pending'",(int(mid),int(chat_id)))
        self.conn.commit()

    def accept_chat_marriage(self, chat_id, mid):
        self._ensure_marriage_chat_id()
        self.conn.execute("UPDATE marriages SET status='active',accepted_at=? WHERE id=? AND chat_id=? AND status='pending'",(self._now(),int(mid),int(chat_id)))
        self.conn.commit()

    def divorce_chat_marriage(self, chat_id, u):
        self._ensure_marriage_chat_id()
        self.conn.execute("UPDATE marriages SET status='divorced' WHERE status='active' AND chat_id=? AND (user1_id=? OR user2_id=?)",(int(chat_id),int(u),int(u)))
        self.conn.commit()

    def get_referral_stats(self, telegram_id):
        row = self.conn.execute("SELECT COUNT(*) FROM referrals WHERE referrer_telegram_id=? AND status='completed'", (int(telegram_id),)).fetchone()
        return int(row[0])

    # ---------------- gamification ----------------
    def get_monthly_coin_ranking(self, year_month: str, limit=20):
        return self.conn.execute("""
            SELECT ct.telegram_id, COALESCE(p.nick, CAST(ct.telegram_id AS TEXT)) AS nick, p.telegram_username,
                   SUM(ct.amount) AS earned
            FROM coin_transactions ct
            LEFT JOIN players p ON p.telegram_id=ct.telegram_id
            WHERE substr(ct.created_at,1,7)=? AND ct.amount>0 AND ct.reason NOT IN ('admin_set')
            GROUP BY ct.telegram_id
            ORDER BY earned DESC, nick COLLATE NOCASE ASC
            LIMIT ?
        """, (year_month, int(limit))).fetchall()

    def get_player_streak(self, player_id: str):
        weeks = self.conn.execute("SELECT week_start, activity FROM week_players WHERE player_id=? ORDER BY week_start DESC", (str(player_id),)).fetchall()
        if not weeks: return 0
        streak = 0
        from datetime import date, timedelta
        expected = None
        for row in weeks:
            d = date.fromisoformat(row['week_start'])
            if expected is None:
                expected = d
            if d != expected: break
            if int(row['activity'] or 0) <= 0: break
            streak += 1
            expected = d - timedelta(days=7)
        return streak

    def get_player_progress(self, player_id: str, limit=8):
        return self.conn.execute("SELECT week_start, activity FROM week_players WHERE player_id=? ORDER BY week_start DESC LIMIT ?", (str(player_id), int(limit))).fetchall()[::-1]

    def seed_achievements(self):
        items = [
            ('activity_1k','🔰 Первый шаг','Набрать 1 000 активности за всё время',1000),
            ('activity_5k','🔥 Активист','Набрать 5 000 активности за всё время',5000),
            ('activity_10k','⚡ 10K','Набрать 10 000 активности за всё время',10000),
            ('activity_50k','💎 50K','Набрать 50 000 активности за всё время',50000),
            ('activity_100k','👑 100K','Набрать 100 000 активности за всё время',100000),
            ('streak_3','🔥 Серия x3','Три недели подряд с активностью',3),
            ('streak_5','🔥 Серия x5','Пять недель подряд с активностью',5),
            ('streak_10','🔥 Серия x10','Десять недель подряд с активностью',10),
        ]
        for code,name,desc,threshold in items:
            self.conn.execute("INSERT OR IGNORE INTO achievements(code,name,description,threshold) VALUES(?,?,?,?)", (code,name,desc,threshold))
        self.conn.commit()

    def award_achievements(self, player_id: str):
        self.seed_achievements()
        p=self.get_player(player_id)
        if not p: return []
        total=int(p['total_activity'] or 0); streak=self.get_player_streak(player_id)
        earned=[]
        for a in self.conn.execute("SELECT * FROM achievements ORDER BY achievement_id").fetchall():
            ok = total >= int(a['threshold']) if a['code'].startswith('activity_') else streak >= int(a['threshold'])
            if not ok: continue
            exists=self.conn.execute("SELECT 1 FROM player_achievements WHERE player_id=? AND achievement_id=?", (str(player_id),a['achievement_id'])).fetchone()
            if not exists:
                self.conn.execute("INSERT INTO player_achievements(player_id,achievement_id,awarded_at) VALUES(?,?,?)", (str(player_id),a['achievement_id'],self._now()))
                earned.append(a)
        self.conn.commit()
        return earned

    def get_player_achievements(self, player_id: str):
        self.seed_achievements()
        return self.conn.execute("""
            SELECT a.*, pa.awarded_at FROM player_achievements pa
            JOIN achievements a ON a.achievement_id=pa.achievement_id
            WHERE pa.player_id=? ORDER BY pa.awarded_at DESC
        """, (str(player_id),)).fetchall()

    def create_tournament(self, name, start_date=None, end_date=None, created_by=None):
        now=self._now(); cur=self.conn.execute("INSERT INTO tournaments(name,start_date,end_date,status,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?)", (name,start_date,end_date,'active',created_by,now,now)); self.conn.commit(); return cur.lastrowid

    def get_tournaments(self, status=None, limit=20):
        if status: return self.conn.execute("SELECT * FROM tournaments WHERE status=? ORDER BY tournament_id DESC LIMIT ?", (status,int(limit))).fetchall()
        return self.conn.execute("SELECT * FROM tournaments ORDER BY tournament_id DESC LIMIT ?", (int(limit),)).fetchall()

    def get_tournament(self, tournament_id):
        return self.conn.execute("SELECT * FROM tournaments WHERE tournament_id=?", (int(tournament_id),)).fetchone()

    def set_tournament_points(self, tournament_id, player_id, points):
        if not self.get_tournament(tournament_id): raise ValueError('Турнир не найден.')
        if not self.get_player(player_id): raise ValueError('Игрок не найден.')
        self.conn.execute("INSERT INTO tournament_players(tournament_id,player_id,points,updated_at) VALUES(?,?,?,?) ON CONFLICT(tournament_id,player_id) DO UPDATE SET points=excluded.points,updated_at=excluded.updated_at", (int(tournament_id),str(player_id),int(points),self._now())); self.conn.commit()

    def get_tournament_players(self, tournament_id, limit=50):
        return self.conn.execute("""
            SELECT tp.*, p.nick, p.telegram_id, p.telegram_username
            FROM tournament_players tp JOIN players p ON p.player_id=tp.player_id
            WHERE tp.tournament_id=? ORDER BY tp.points DESC, p.nick COLLATE NOCASE LIMIT ?
        """, (int(tournament_id),int(limit))).fetchall()

    def close_tournament(self, tournament_id):
        self.conn.execute("UPDATE tournaments SET status='finished',updated_at=? WHERE tournament_id=?", (self._now(),int(tournament_id))); self.conn.commit()

    def add_anticheat_event(self, week_start, player_id, previous_activity, current_activity, reason):
        self.conn.execute("INSERT INTO anti_cheat_events(week_start,player_id,previous_activity,current_activity,reason,created_at) VALUES(?,?,?,?,?,?)", (week_start,str(player_id),int(previous_activity),int(current_activity),reason,self._now())); self.conn.commit()

    def get_anticheat_events(self, limit=20):
        return self.conn.execute("""
            SELECT a.*, p.nick FROM anti_cheat_events a LEFT JOIN players p ON p.player_id=a.player_id
            ORDER BY a.id DESC LIMIT ?
        """, (int(limit),)).fetchall()

    # ---------------- backup / restore ----------------
    def backup_to(self, destination: str):
        """Create a consistent SQLite snapshot, including WAL contents."""
        dest = sqlite3.connect(destination, timeout=30)
        try:
            with dest:
                self.conn.backup(dest)
        finally:
            dest.close()

    def restore_from(self, source: str):
        """Validate and replace the live database with a SQLite backup."""
        source_conn = sqlite3.connect(source, timeout=30)
        try:
            row = source_conn.execute("PRAGMA integrity_check").fetchone()
            if not row or str(row[0]).lower() != "ok":
                raise ValueError("Резервная копия не прошла integrity_check.")
        finally:
            source_conn.close()

        self.conn.close()
        import shutil
        shutil.copy2(source, self.path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_tables()
        self._migrate()

    def integrity_check(self) -> bool:
        row = self.conn.execute("PRAGMA integrity_check").fetchone()
        return bool(row and str(row[0]).lower() == "ok")

    def optimize(self):
        """Run safe SQLite maintenance without deleting application data."""
        self.conn.execute("PRAGMA optimize")
        self.conn.execute("ANALYZE")
        self.conn.commit()

    # ---------------- Iris compatibility helpers ----------------
    def iris_blacklist_add(self, chat_id, user_id, added_by, reason=''):
        self.conn.execute("INSERT OR REPLACE INTO iris_blacklist(chat_id,user_id,added_by,reason,created_at) VALUES(?,?,?,?,?)",
                          (int(chat_id), int(user_id), int(added_by), str(reason or ''), self._now()))
        self.conn.commit()

    def iris_blacklist_remove(self, chat_id, user_id):
        cur=self.conn.execute("DELETE FROM iris_blacklist WHERE chat_id=? AND user_id=?",(int(chat_id),int(user_id)))
        self.conn.commit(); return cur.rowcount > 0

    def iris_blacklist_has(self, chat_id, user_id):
        return self.conn.execute("SELECT 1 FROM iris_blacklist WHERE chat_id=? AND user_id=?",(int(chat_id),int(user_id))).fetchone() is not None

    def iris_blacklist_list(self, chat_id, limit=100):
        return self.conn.execute("SELECT * FROM iris_blacklist WHERE chat_id=? ORDER BY created_at DESC LIMIT ?",(int(chat_id),int(limit))).fetchall()

    def iris_chat_settings(self, chat_id):
        row=self.conn.execute("SELECT * FROM iris_chat_settings WHERE chat_id=?",(int(chat_id),)).fetchone()
        if row: return row
        self.conn.execute("INSERT INTO iris_chat_settings(chat_id,rp_enabled,commands_notice,updated_at) VALUES(?,?,?,?)",(int(chat_id),1,1,self._now())); self.conn.commit()
        return self.conn.execute("SELECT * FROM iris_chat_settings WHERE chat_id=?",(int(chat_id),)).fetchone()

    def iris_set_rp(self, chat_id, enabled):
        self.iris_chat_settings(chat_id)
        self.conn.execute("UPDATE iris_chat_settings SET rp_enabled=?,updated_at=? WHERE chat_id=?",(1 if enabled else 0,self._now(),int(chat_id))); self.conn.commit()

    def iris_set_commands_notice(self, chat_id, enabled):
        self.iris_chat_settings(chat_id)
        self.conn.execute("UPDATE iris_chat_settings SET commands_notice=?,updated_at=? WHERE chat_id=?",(1 if enabled else 0,self._now(),int(chat_id))); self.conn.commit()

    def iris_add_custom_rp(self, owner_id, name, emoji, template):
        self.conn.execute("INSERT INTO iris_rp_custom(owner_id,name,emoji,template,created_at) VALUES(?,?,?,?,?)",(int(owner_id),str(name).lower().strip(),str(emoji or '✨'),str(template),self._now())); self.conn.commit()

    def iris_delete_custom_rp(self, owner_id, name):
        cur=self.conn.execute("DELETE FROM iris_rp_custom WHERE owner_id=? AND name=?",(int(owner_id),str(name).lower().strip())); self.conn.commit(); return cur.rowcount>0

    def iris_custom_rp(self, name):
        return self.conn.execute("SELECT * FROM iris_rp_custom WHERE name=?",(str(name).lower().strip(),)).fetchone()

    def iris_custom_rps(self, owner_id):
        return self.conn.execute("SELECT * FROM iris_rp_custom WHERE owner_id=? ORDER BY id DESC",(int(owner_id),)).fetchall()

    def close(self): self.conn.close()


    # --- V4 Groq AI usage ---
    def init_ai_usage_table(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ai_usage (
                    usage_date TEXT PRIMARY KEY,
                    key1_requests INTEGER NOT NULL DEFAULT 0,
                    key2_requests INTEGER NOT NULL DEFAULT 0,
                    key3_requests INTEGER NOT NULL DEFAULT 0,
                    ai_enabled INTEGER NOT NULL DEFAULT 1,
                    last_error TEXT,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def get_ai_usage(self, usage_date):
        self.init_ai_usage_table()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM ai_usage WHERE usage_date = ?", (usage_date,)
            ).fetchone()
            if row:
                return dict(row)
            conn.execute(
                "INSERT INTO ai_usage(usage_date) VALUES (?)", (usage_date,)
            )
            row = conn.execute(
                "SELECT * FROM ai_usage WHERE usage_date = ?", (usage_date,)
            ).fetchone()
            return dict(row)

    def set_ai_usage(self, usage_date, key_index, count, ai_enabled=1, last_error=None):
        self.init_ai_usage_table()
        col = {1:"key1_requests", 2:"key2_requests", 3:"key3_requests"}[int(key_index)]
        with self._connect() as conn:
            conn.execute(
                f"UPDATE ai_usage SET {col}=?, ai_enabled=?, last_error=?, updated_at=CURRENT_TIMESTAMP WHERE usage_date=?",
                (int(count), int(ai_enabled), last_error, usage_date)
            )

    def set_ai_enabled(self, usage_date, enabled, last_error=None):
        self.init_ai_usage_table()
        with self._connect() as conn:
            conn.execute(
                "UPDATE ai_usage SET ai_enabled=?, last_error=?, updated_at=CURRENT_TIMESTAMP WHERE usage_date=?",
                (int(enabled), last_error, usage_date)
            )
