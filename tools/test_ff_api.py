import argparse
import asyncio
import json
import os
import sys

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ff_api import FreeFireAPI  # noqa: E402


async def main():
    parser = argparse.ArgumentParser(description="Live Free Fire API test for Vaka")
    parser.add_argument("uid", help="Free Fire UID")
    parser.add_argument("--region", default=os.getenv("FF_REGION", "BD"), help="Region, default: FF_REGION/BD")
    args = parser.parse_args()

    api = FreeFireAPI(
        api_key=os.getenv("FF_API_KEY", ""),
        region=args.region,
        provider="siambhau",
        base_url=os.getenv("FF_API_BASE", "https://siambhau69.eu.cc"),
    )
    try:
        print("=== VAKA FREE FIRE API TEST ===")
        print(f"Provider: {api.provider}")
        print(f"Base URL: {api.base_url}")
        print(f"Region: {args.region.upper()}")
        print(f"UID: {args.uid}")
        profile = await api.get_player_profile(args.uid, args.region)
        if not profile:
            print("RESULT: FAIL - API не вернул профиль игрока")
            return 2
        print("RESULT: OK")
        print(json.dumps({
            "uid": profile.uid,
            "nickname": profile.nickname,
            "level": profile.level,
            "region": profile.region,
            "guild_id": profile.guild_id,
            "guild_name": profile.guild_name,
            "guild_level": profile.guild_level,
            "guild_members": profile.guild_members,
            "guild_capacity": profile.guild_capacity,
            "guild_owner": profile.guild_owner,
        }, ensure_ascii=False, indent=2))
        return 0
    finally:
        await api.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
