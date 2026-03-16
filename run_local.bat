@echo off
title PHS Local Server
cd /d "%~dp0"

echo Stopping any process already using port 5000...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":5000 " ^| findstr LISTENING') do (
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 1 /nobreak >nul

echo.
echo  Starting PHS...
echo  Site will open at http://127.0.0.1:5000
echo  Close this window to stop the server.
echo.

:: Open browser 3 seconds after server starts (separate process, browser stays open after window closes)
start "" /b cmd /c "timeout /t 3 /nobreak >nul && start http://127.0.0.1:5000"

:: Run server in foreground — closing this window kills the server
python -m waitress --host=127.0.0.1 --port=5000 app:app

echo.
echo Server stopped.
pause
