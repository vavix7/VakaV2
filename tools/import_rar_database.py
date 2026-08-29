#!/usr/bin/env python3
"""Extract a RAR5 database archive with an installed 7-Zip/WinRAR/unrar and migrate it."""
import shutil, subprocess, sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
archive=Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/"migration_source_данныеБАзы.rar"
out=ROOT/"migration_source_extracted"
if not archive.exists():
    raise SystemExit(f"Archive not found: {archive}")

tools=[shutil.which(x) for x in ("7z","7zz","unrar","WinRAR","winrar") if shutil.which(x)]
if not tools:
    raise SystemExit("Не найден 7-Zip/unrar/WinRAR. Установите один из них и повторите.")
exe=tools[0]
out.mkdir(parents=True,exist_ok=True)
subprocess.run([exe,"x","-y",str(archive),f"-o{out}"],check=True)

candidates=list(out.rglob("guild_activity_updated.db"))+list(out.rglob("guild_activity.db"))
if not candidates:
    raise SystemExit("В архиве не найдена SQLite база.")
source=candidates[0]
print(f"Использую: {source}")
subprocess.run([sys.executable,str(ROOT/"tools"/"migrate_database.py"),str(source),str(ROOT/"data"/"guild_activity.db")],check=True)
