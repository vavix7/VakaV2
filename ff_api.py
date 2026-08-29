import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import aiohttp

logger = logging.getLogger(__name__)

# SiamBhau Premium API. BD is the default region; IND is used as a fallback
# for Indian accounts when the BD lookup does not return a profile.
SIAMBHAU_BASE = "https://siambhau69.eu.cc"


@dataclass
class FFPlayerProfile:
    uid: str
    nickname: str = "Неизвестно"
    level: int = 0
    region: str = "BD"
    rank_br: int = 0
    rank_br_points: int = 0
    rank_cs: int = 0
    rank_cs_points: int = 0
    likes: int = 0
    exp: int = 0
    created_at: str = "Неизвестно"
    last_login: str = "Неизвестно"
    is_banned: bool = False
    ban_reason: str = ""
    guild_id: Optional[str] = None
    guild_name: Optional[str] = None
    guild_level: int = 0
    guild_members: int = 0
    guild_capacity: int = 0
    guild_owner: Optional[str] = None
    raw_data: dict = field(default_factory=dict)


class FreeFireAPI:
    """SiamBhau Free Fire API client used by Vaka.

    The client deliberately centralizes all API access so changing the API
    host/key/region never requires editing individual bot commands.
    """

    def __init__(self, api_key: str = "", region: str = "BD", provider: str = "siambhau", base_url: str = ""):
        self.api_key = (api_key or "").strip()
        self.region = (region or "BD").strip().upper() or "BD"
        self.provider = "siambhau"
        self.base_url = (base_url or SIAMBHAU_BASE).strip().rstrip("/")
        self.cache: Dict[str, Any] = {}
        self.cache_ttl = timedelta(minutes=5)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=25)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _params(self, params: Optional[Dict[str, Any]] = None, region: Optional[str] = None) -> Dict[str, Any]:
        out = dict(params or {})
        if self.api_key:
            out["key"] = self.api_key
        if region:
            out["region"] = (region or self.region).upper()
        return out

    async def request_json(self, path: str, params: Optional[Dict[str, Any]] = None,
                           method: str = "GET", json_body: Optional[dict] = None) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            raise RuntimeError("SiamBhau API key не указан в .env (FF_API_KEY).")
        url = self.base_url + (path if path.startswith("/") else "/" + path)
        session = await self._get_session()
        request_kwargs = {"params": self._params(params)}
        if json_body is not None:
            request_kwargs["json"] = json_body
        try:
            async with session.request(method.upper(), url, **request_kwargs) as response:
                text = await response.text()
                if response.status != 200:
                    logger.warning("SiamBhau %s %s -> HTTP %s: %s", method.upper(), url, response.status, text[:400])
                    return None
                try:
                    data = await response.json(content_type=None)
                except Exception:
                    logger.warning("SiamBhau returned non-JSON for %s: %s", url, text[:400])
                    return None
                if isinstance(data, dict) and str(data.get("error", "")).strip():
                    logger.warning("SiamBhau API error for %s: %s", url, data.get("error"))
                    return None
                return data if isinstance(data, dict) else None
        except asyncio.TimeoutError:
            logger.warning("SiamBhau timeout: %s", url)
            return None
        except aiohttp.ClientError as exc:
            logger.warning("SiamBhau network error: %s: %s", url, exc)
            return None

    async def request_binary(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[bytes]:
        if not self.api_key:
            raise RuntimeError("SiamBhau API key не указан в .env (FF_API_KEY).")
        url = self.base_url + (path if path.startswith("/") else "/" + path)
        session = await self._get_session()
        try:
            async with session.get(url, params=self._params(params)) as response:
                if response.status != 200:
                    logger.warning("SiamBhau binary %s -> HTTP %s", url, response.status)
                    return None
                return await response.read()
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            logger.warning("SiamBhau binary error %s: %s", url, exc)
            return None

    def endpoint_url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Return a safe-to-display API URL without exposing the API key."""
        from urllib.parse import urlencode
        query = dict(params or {})
        if "region" in query:
            query["region"] = str(query["region"]).upper()
        query.pop("key", None)
        return self.base_url + (path if path.startswith("/") else "/" + path) + ("?" + urlencode(query) if query else "")

    async def get_player_profile(self, uid: str, region: Optional[str] = None) -> Optional[FFPlayerProfile]:
        uid = str(uid).strip()
        if not uid.isdigit():
            raise ValueError("UID должен состоять только из цифр.")
        if len(uid) < 5 or len(uid) > 15:
            raise ValueError("UID должен содержать от 5 до 15 цифр.")

        requested = (region or self.region).upper()
        regions = [requested]
        # Owner guidance: BD has the broadest server coverage; India is the
        # important exception. If BD has no result, retry once against IND.
        if requested == "BD":
            regions.append("IND")

        for current_region in regions:
            cache_key = f"profile:{current_region}:{uid}"
            cached = self.cache.get(cache_key)
            if cached and datetime.now() - cached[0] < self.cache_ttl:
                return cached[1]
            data = await self.request_json("/freefireinfo/bhau", {"uid": uid, "region": current_region})
            if not data:
                continue
            profile = self._parse_player_data(data, uid, current_region)
            if profile.guild_id:
                guild = await self.get_guild_info(profile.guild_id)
                if guild:
                    self._merge_guild(profile, guild)
            self.cache[cache_key] = (datetime.now(), profile)
            return profile
        return None

    async def get_player(self, uid: str, region: Optional[str] = None) -> Optional[Dict[str, Any]]:
        profile = await self.get_player_profile(uid, region)
        return self.profile_to_dict(profile) if profile else None

    async def get_stats(self, uid: str, region: Optional[str] = None, gamemode: str = "br", matchmode: str = "CAREER") -> Optional[Dict[str, Any]]:
        uid = str(uid).strip()
        region = (region or self.region).upper()
        data = await self.request_json("/freefireinfo/stats", {
            "uid": uid, "region": region, "gamemode": gamemode.lower(), "matchmode": matchmode.upper()
        })
        if not data and region == "BD":
            data = await self.request_json("/freefireinfo/stats", {
                "uid": uid, "region": "IND", "gamemode": gamemode.lower(), "matchmode": matchmode.upper()
            })
        return data

    async def get_guild_info(self, clan_id: str) -> Optional[Dict[str, Any]]:
        return await self.request_json("/guild/info", {"clan_id": str(clan_id).strip()})

    async def ban_check(self, uid: str, region: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return await self.request_json("/bancheck/bancheck", {"uid": str(uid).strip(), "region": (region or self.region).upper()})

    async def get_full_player_data(self, uid: str, region: Optional[str] = None) -> Optional[Dict[str, Any]]:
        profile = await self.get_player_profile(uid, region)
        if not profile:
            return None
        actual_region = profile.region or (region or self.region).upper()
        br, cs, ban = await asyncio.gather(
            self.get_stats(uid, actual_region, "br", "CAREER"),
            self.get_stats(uid, actual_region, "cs", "RANKED"),
            self.ban_check(uid, actual_region),
        )
        return {"profile": self.profile_to_dict(profile), "br_stats": br, "cs_stats": cs, "ban": ban}

    async def get_banner(self, uid: str, region: Optional[str] = None) -> Optional[bytes]:
        return await self.request_binary("/banner/profile", {"uid": str(uid), "region": (region or self.region).upper()})

    async def get_outfit(self, uid: str, region: Optional[str] = None) -> Optional[bytes]:
        return await self.request_binary("/outfits/outfit", {"uid": str(uid), "region": (region or self.region).upper()})

    async def call_endpoint(self, path: str, params: Optional[Dict[str, Any]] = None, method: str = "GET", json_body: Optional[dict] = None):
        """Unified escape hatch for every documented SiamBhau endpoint.

        Commands and modules should use this method instead of duplicating HTTP
        logic. It intentionally keeps the API key inside the client.
        """
        return await self.request_json(path, params, method=method, json_body=json_body)

    async def call_binary_endpoint(self, path: str, params: Optional[Dict[str, Any]] = None):
        return await self.request_binary(path, params)

    @staticmethod
    def profile_to_dict(profile: Optional[FFPlayerProfile]) -> Optional[Dict[str, Any]]:
        if not profile:
            return None
        return {
            "uid": profile.uid, "nickname": profile.nickname, "nick": profile.nickname,
            "level": profile.level, "region": profile.region, "rank": profile.rank_br,
            "rankingPoints": profile.rank_br_points, "csRank": profile.rank_cs,
            "csRankingPoints": profile.rank_cs_points, "liked": profile.likes, "exp": profile.exp,
            "guildId": profile.guild_id, "guildName": profile.guild_name,
            "guildLevel": profile.guild_level, "guildMembers": profile.guild_members,
            "guildCapacity": profile.guild_capacity, "guildOwner": profile.guild_owner,
            "raw": profile.raw_data,
        }

    @staticmethod
    def _first(d: Dict[str, Any], *keys, default=None):
        for key in keys:
            if key in d and d[key] is not None:
                return d[key]
        return default

    @staticmethod
    def _fmt_ts(value):
        if not value:
            return "Неизвестно"
        try:
            return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return str(value)

    def _parse_player_data(self, data: Dict[str, Any], uid: str, region: str) -> FFPlayerProfile:
        basic = data.get("basicInfo") or data.get("basicinfo") or {}
        clan = data.get("clanBasicInfo") or data.get("clanbasicinfo") or data.get("clanInfo") or {}
        guild_id = self._first(clan, "clanId", "clanID", "guildId", "guildID")
        return FFPlayerProfile(
            uid=uid,
            nickname=str(self._first(basic, "nickname", "nick", default="Неизвестно")),
            level=int(self._first(basic, "level", default=0) or 0),
            region=str(self._first(basic, "region", default=region)).upper(),
            rank_br=int(self._first(basic, "rank", "brRank", default=0) or 0),
            rank_br_points=int(self._first(basic, "rankingPoints", "brRankingPoints", default=0) or 0),
            rank_cs=int(self._first(basic, "csRank", default=0) or 0),
            rank_cs_points=int(self._first(basic, "csRankingPoints", default=0) or 0),
            likes=int(self._first(basic, "liked", "likes", default=0) or 0),
            exp=int(self._first(basic, "exp", default=0) or 0),
            created_at=self._fmt_ts(self._first(basic, "createAt", "createat")),
            last_login=self._fmt_ts(self._first(basic, "lastLoginAt", "lastloginat")),
            guild_id=str(guild_id) if guild_id else None,
            guild_name=self._first(clan, "clanName", "guildName"),
            guild_level=int(self._first(clan, "clanLevel", "guildLevel", default=0) or 0),
            guild_members=int(self._find_member_count(clan) or 0),
            guild_capacity=int(self._find_capacity(clan) or 0),
            guild_owner=str(self._first(clan, "captainId", "ownerId", default="")) or None,
            raw_data=data,
        )

    @classmethod
    def _find_member_count(cls, obj: Any) -> Optional[int]:
        """Extract a real guild member count from all known SiamBhau shapes.

        Different API revisions can expose the same value as memberNum,
        memberCount, total_members, totalMembers, etc., and it may be nested.
        Never treat a list/dict under a generic `members` key as the count.
        """
        count_keys = (
            "memberNum", "memberCount", "member_count",
            "total_members", "totalMembers", "totalMember",
            "membersCount", "members_count", "numMembers", "num_members",
        )
        if isinstance(obj, dict):
            for key in count_keys:
                value = obj.get(key)
                if value not in (None, "", 0, "0"):
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        pass
            # Some responses use `members` as a scalar number.
            value = obj.get("members")
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
            if isinstance(value, str) and value.strip().isdigit() and int(value) > 0:
                return int(value.strip())
            for value in obj.values():
                found = cls._find_member_count(value)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for value in obj:
                found = cls._find_member_count(value)
                if found is not None:
                    return found
        return None

    @classmethod
    def _find_capacity(cls, obj: Any) -> Optional[int]:
        capacity_keys = ("capacity", "maxMembers", "max_members", "memberCapacity", "member_capacity")
        if isinstance(obj, dict):
            for key in capacity_keys:
                value = obj.get(key)
                if value not in (None, "", 0, "0"):
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        pass
            for value in obj.values():
                found = cls._find_capacity(value)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for value in obj:
                found = cls._find_capacity(value)
                if found is not None:
                    return found
        return None

    def _merge_guild(self, profile: FFPlayerProfile, guild: Dict[str, Any]):
        profile.guild_id = str(self._first(guild, "id", "clanId", "clanID", "guildId", "guildID", default=profile.guild_id))
        profile.guild_name = self._first(guild, "clan_name", "clanName", "guildName", default=profile.guild_name)
        profile.guild_level = int(self._first(guild, "level", "clanLevel", "guildLevel", default=profile.guild_level) or profile.guild_level or 0)

        # SiamBhau documents memberNum in clanBasicInfo and total_members in
        # guild_details.  We scan the complete response as a safety net so a
        # partial response can never turn a real member count into 0.
        members = self._find_member_count(guild)
        capacity = self._find_capacity(guild)

        if members is not None and members > 0:
            profile.guild_members = members
        if capacity is not None and capacity > 0:
            profile.guild_capacity = capacity

        profile.guild_owner = str(self._first(guild, "captainId", "ownerId", default=profile.guild_owner or "")) or profile.guild_owner
        profile.raw_data["guildInfo"] = guild
