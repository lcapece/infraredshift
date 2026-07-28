@echo off
REM  Databa6ix - load CONSUMER 3 only, into its own DuckDB file.
REM  Run all four LOAD_*.cmd at once. Each writes its own
REM  redshift.<cluster>.duckdb so they cannot clobber each other
REM  or the live warehouse. Run MERGE_ALL.cmd when all four finish.
setlocal
cd /d "%~dp0"

set "APP="
if exist "%~dp0DataBa6ix.py"  set "APP=%~dp0DataBa6ix.py"
if not defined APP if exist "%~dp0DataBa6ix.txt" set "APP=%~dp0DataBa6ix.txt"
if not defined APP ( echo ERROR: DataBa6ix.py not found next to this script. ^& pause ^& exit /b 1 )

set "DATABA6IX_NO_LOCK=1"
set "DBDIR=%USERPROFILE%\RQP\data"
if not exist "%DBDIR%" mkdir "%DBDIR%"
set "DB=%DBDIR%\redshift.consumer_3.duckdb"

REM Enable ONLY Consumer 3 for this window.
set "REDSHIFT_ENABLED=false"
set "REDSHIFT_PRODUCER_ENABLED=false"
set "REDSHIFT_CONSUMER_1_ENABLED=false"
set "REDSHIFT_CONSUMER_2_ENABLED=false"
set "REDSHIFT_CONSUMER_3_ENABLED=true"

echo Loading CONSUMER 3 into "%DB%"
echo Leave this window open. The other clusters can load at the same time.
echo.
python "%APP%" --loader refresh --duckdb-path "%DB%" --days 2 --promote --skip-external --external-timeout-action skip
set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (echo CONSUMER 3 DONE. When all four windows finish, run MERGE_ALL.cmd.) else (echo CONSUMER 3 stopped code %RC%. Re-run to resume, or open the app once to save Local Credentials.)
pause
exit /b %RC%
