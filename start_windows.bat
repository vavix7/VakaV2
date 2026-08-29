@echo off
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] .venv not found. Create the environment and install requirements.txt first.
  pause
  exit /b 1
)
.venv\Scripts\python.exe bot.py
pause
