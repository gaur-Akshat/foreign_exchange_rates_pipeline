@echo off
echo FX Pipeline Automatic Scheduler
echo Started: %date% %time%

:: Change directory to the script's folder
cd /d "%~dp0"

:: Ensure logs directory exists
if not exist "data\logs" mkdir "data\logs"

:: Activate virtual environment
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else (
    echo [WARNING] Virtual environment not found at .venv\Scripts\activate.bat
)

:: Run pipeline and append all output to scheduler.log
echo [INFO] Executing main.py...
python main.py >> data\logs\scheduler.log 2>&1

echo [INFO] Pipeline run finished. Check data\logs\scheduler.log for details.
echo Completed: %date% %time%