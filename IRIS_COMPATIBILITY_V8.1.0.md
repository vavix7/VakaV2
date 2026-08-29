# Vaka V8.1.0 — Iris compatibility audit

## Source basis

The implementation was checked against the available Iris command documentation and community command lists, plus the user's project specification.

Documented Iris areas include moderation, warnings/bans, triggers, command-access settings, chat cleanup/settings, chat networks, profiles/statistics, moderator themes, ban voting, anti-spam/SCAM, VIP bonuses, entertainment/RP, the Koronairis mini-game, marriages, map, and other modules. The project specification explicitly excludes Duels, Cubes, Clans, Circles, Relationships, Reputation, Rewards, Bookmarks, Notes, Timers, Catalog, Iris Exchange, Inline mode, Business Bot, Reports, Giveaways, and Telegram integration.

## Implemented/verified in V8.1.0

- Existing Vaka moderation and role system retained.
- Existing warnings, mute, ban, unban, kick retained.
- RP engine expanded and unified.
- RP works by reply or @username.
- RP output uses action emoji + actor + target.
- `Ударить` and the other non-explicit RP actions use the same engine.
- RP aliases supported.
- `+рп` / `-рп` chat toggle.
- `рп команды` help.
- Iris-style `!` and `.` prefixes for supported Vaka/Iris-compatible commands.
- Iris blacklist stored per chat, with add/remove/list and RP blocking.
- Personal RP commands (`+мрп`, `мрп`, `-мрп`) stored in SQLite.
- Existing marriage module retained.
- Existing profile/statistics modules retained.
- Existing Vaka coins/shop/rewards/achievements retained.
- Existing Guest/Participant/Admin panels retained.
- Existing KV workflow retained.
- Existing Free Fire API client retained and extended with a unified endpoint escape hatch.
- `/bancheck` corrected to the documented `/bancheck/bancheck` endpoint.
- Legacy database rows and values were preserved.

## Explicit safety boundary

The RP engine does not add explicit sexual content or sexual violence. Non-explicit romantic/adult-flavored RP that is already represented by the project's safe action engine can remain. Explicit sexual-violence actions are not implemented.

## Not claimed as complete

This file deliberately does NOT claim that every historical Iris command is implemented. Some Iris modules require detailed command semantics and Telegram chat-side behavior that are not present in the Vaka source or in the available public documentation. They must not be fabricated.

## Excluded exactly as requested

- Duels
- Cubes
- Clans
- Circles
- Relationships
- Reputation
- Rewards module
- Bookmarks
- Notes
- Timers
- Catalog
- Iris Exchange
- Inline mode
- Business Bot
- Reports
- Giveaways
- Telegram integration
