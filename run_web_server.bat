@echo off
chcp 65001 > nul
title Product Categories - Web Server
echo Starting Product Categories Web Server...
echo.
echo Web interface will be available at: http://localhost:5000
echo Press Ctrl+C to stop the server
echo.
cd /d "%~dp0"
python app.py
pause
