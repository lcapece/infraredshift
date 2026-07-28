@echo off
REM ============================================================
REM  Databa6ix — LOAD DATA. Double-click this on the corp laptop.
REM  Loads all 4 clusters in parallel and promotes when done.
REM  Safe: existing data stays live until the new load finishes.
REM ============================================================
setlocal
cd /d "%~dp0"

set "DB=%USERPROFILE%\RQP\data\redshift.duckdb"
if not exist "%USERPROFILE%\RQP\data" mkdir "%USERPROFILE%\RQP\data"

set "APP="
if exist "%~dp0DataBa6ix.py"           set "APP=%~dp0DataBa6ix.py"
if not defined APP if exist "%~dp0DataBa6ix.txt" set "APP=%~dp0DataBa6ix.txt"
if not defined APP (
    echo ERROR: DataBa6ix.py not found next to this script.
    echo Put LOAD.cmd in the same folder as DataBa6ix.py, then run it again.
    pause
    exit /b 1
)

echo Loading all clusters in parallel. Leave this window open.
echo You can keep using the app while this runs.
echo.

python "%APP%" --loader refresh --duckdb-path "%DB%" --days 2 --promote --skip-external --external-timeout-action skip
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (
    echo DONE. The load ran to the end without halting.
    echo   - If every table loaded, the new data was promoted automatically.
    echo   - If any table/cluster was SKIPPED, they are listed above and in:
    echo         "%USERPROFILE%\RQP\data\load_report.txt"
    echo     Live data was left untouched. Re-run this to resume and finish the
    echo     rest, then it promotes once the load is fully clean.
) else (
    echo Load stopped with code %RC% before finishing. Run this again to resume.
    echo If it says a HOST is required, open the app once and save Local Credentials first.
)
pause
exit /b %RC%
