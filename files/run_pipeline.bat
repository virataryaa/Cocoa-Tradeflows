@echo off
REM ── Cocoa Flows TDM Pipeline ─────────────────────────────────────────────────
REM Run daily via Windows Task Scheduler
REM Schedule: daily at 09:30

SET PYTHON=C:\Users\virat.arya\AppData\Local\anaconda3\python.exe
SET DIR=%~dp0

echo ============================================================
echo  Cocoa Flows Pipeline  ^|  %DATE% %TIME%
echo ============================================================

echo.
echo [1/3] Cocoa Exports...
"%PYTHON%" "%DIR%cocoa_exports_ingest.py"

echo.
echo [2/3] Cocoa Imports...
"%PYTHON%" "%DIR%cocoa_imports_ingest.py"

echo.
echo [3/3] Cocoa Imports (EU)...
"%PYTHON%" "%DIR%cocoa_imports_eu_ingest.py"

echo.
echo ============================================================
echo  Pushing data to GitHub...
echo ============================================================
cd /d "C:\Users\virat.arya\ETG\SoftsDatabase - Documents\Database\Hardmine\Fundamental\TDM\Cocoa Flows"
git add files/data/*.parquet
git commit -m "data: daily ingest update %DATE%"
git push
echo  GitHub push complete.

echo.
echo ============================================================
echo  Pipeline complete  ^|  %DATE% %TIME%
echo ============================================================
