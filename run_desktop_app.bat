@echo off
chcp 65001 > nul
title Product Categories - Desktop App
echo Starting Product Categories Desktop Application...
echo.
echo Both web interface and desktop app will be available
echo Web interface: http://localhost:5000
echo.
cd /d "%~dp0"
python desktop.py
pause
