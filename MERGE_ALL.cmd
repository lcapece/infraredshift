@echo off
REM ============================================================
REM  Databa6ix — combine the 4 per-cluster files into the
REM  analyzer warehouse. Run AFTER all LOAD_*.cmd finish.
REM ============================================================
setlocal
cd /d "%~dp0"
set "APP="
if exist "%~dp0DataBa6ix.py"  set "APP=%~dp0DataBa6ix.py"
if not defined APP if exist "%~dp0DataBa6ix.txt" set "APP=%~dp0DataBa6ix.txt"
if not defined APP ( echo ERROR: DataBa6ix.py not found next to this script. & pause & exit /b 1 )
echo Merging all cluster files into the analyzer warehouse...
python "%APP%" --loader-merge
set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (echo DONE. Open the app to see all clusters.) else (echo Merge stopped code %RC%.)
pause
exit /b %RC%
