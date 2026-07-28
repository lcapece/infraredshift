@echo off
setlocal
set "SOURCE_FOLDER=%~dp0"

set "REQUIREMENTS="
if exist "%SOURCE_FOLDER%redshift_analyzer_requirements.txt" set "REQUIREMENTS=%SOURCE_FOLDER%redshift_analyzer_requirements.txt"
if exist "%SOURCE_FOLDER%DataBa6ix_requirements.txt" set "REQUIREMENTS=%SOURCE_FOLDER%DataBa6ix_requirements.txt"
if exist "%SOURCE_FOLDER%Infraredshift_requirements.txt" set "REQUIREMENTS=%SOURCE_FOLDER%Infraredshift_requirements.txt"
if not defined REQUIREMENTS (
    echo ERROR: Missing requirements file.
    echo Expected one of:
    echo   redshift_analyzer_requirements.txt
    echo   Infraredshift_requirements.txt
    exit /b 1
)

set "PORTABLE_PROFILES=%SOURCE_FOLDER%redshift_cluster_profiles.json"
set "DATA_FOLDER=%USERPROFILE%\RQP\data"
set "DUCKDB_PATH=%DATA_FOLDER%\redshift.duckdb"

REM Prefer Infraredshift.txt for email-safe delivery (mail filters often block .py).
REM Legacy DataBa6ix names stay accepted so an existing kit keeps working.
set "LAUNCHER="
if exist "%SOURCE_FOLDER%Infraredshift.txt" set "LAUNCHER=%SOURCE_FOLDER%Infraredshift.txt"
if not defined LAUNCHER if exist "%SOURCE_FOLDER%Infraredshift.py" set "LAUNCHER=%SOURCE_FOLDER%Infraredshift.py"
if not defined LAUNCHER if exist "%SOURCE_FOLDER%DataBa6ix.txt" set "LAUNCHER=%SOURCE_FOLDER%DataBa6ix.txt"
if not defined LAUNCHER if exist "%SOURCE_FOLDER%DataBa6ix.py" set "LAUNCHER=%SOURCE_FOLDER%DataBa6ix.py"
if not defined LAUNCHER if exist "%SOURCE_FOLDER%redshift_analyzer_text.txt" set "LAUNCHER=%SOURCE_FOLDER%redshift_analyzer_text.txt"
if not defined LAUNCHER if exist "%SOURCE_FOLDER%redshift_analyzer_text.py" set "LAUNCHER=%SOURCE_FOLDER%redshift_analyzer_text.py"
if not defined LAUNCHER (
    echo ERROR: Missing Infraredshift application launcher.
    echo Expected one of:
    echo   Infraredshift.txt  ^(email-safe, preferred^)
    echo   Infraredshift.py
    echo   DataBa6ix.txt  ^(legacy alias^)
    echo   redshift_analyzer_text.txt  ^(legacy alias^)
    exit /b 1
)

echo Checking Python...
python -c "import sys; assert sys.version_info >= (3,9), 'Python 3.9 or newer is required'; print(sys.version)"
if errorlevel 1 exit /b 1

echo Using requirements: %REQUIREMENTS%
echo Using launcher: %LAUNCHER%
echo Installing Infraredshift dependencies for this Windows user...
python -m pip install --user -r "%REQUIREMENTS%"
if errorlevel 1 exit /b 1

if not exist "%DATA_FOLDER%" mkdir "%DATA_FOLDER%"

echo Creating or upgrading DuckDB schema...
python "%LAUNCHER%" --index-duckdb --duckdb-path "%DUCKDB_PATH%"
if errorlevel 1 exit /b 1

echo.
echo Verifying the install...
python "%LAUNCHER%" --verify --duckdb-path "%DUCKDB_PATH%"
if errorlevel 1 (
    echo.
    echo Setup finished but verification reported a problem - see above.
    exit /b 1
)

echo.
echo Ready. DuckDB: %DUCKDB_PATH%
echo Launch: python "%LAUNCHER%"
if exist "%PORTABLE_PROFILES%" (
    echo Profiles: %PORTABLE_PROFILES%
) else (
    echo WARNING: redshift_cluster_profiles.json not found - rename redshift_cluster_profiles.json.txt if needed
)
echo Enter Local Credentials in the app, then run load_all_clusters_incremental.cmd
exit /b 0
