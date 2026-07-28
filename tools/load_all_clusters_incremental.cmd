@echo off
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0load_all_clusters_incremental.ps1" %*
exit /b %ERRORLEVEL%
