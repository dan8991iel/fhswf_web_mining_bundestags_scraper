@echo off
REM ────────────────────────────────────────────────────────────────
REM  Run the politician content spider
REM ────────────────────────────────────────────────────────────────
echo.
echo =====  SCRAPY: politician_content_spider  =====
echo.
setlocal EnableDelayedExpansion

REM ---- config --------------------------------------------------
set "BATCH_SIZE=2000"            REM must equal spider’s BATCH_SIZE
set "TOTAL_BATCHES=%~1"          REM arg 1 or default
if "%TOTAL_BATCHES%"=="" (
    set "TOTAL_BATCHES=5"
)

echo =============================================================
echo  Running %TOTAL_BATCHES% batches  (size %BATCH_SIZE%)
echo =============================================================

for /L %%B in (0,1,%TOTAL_BATCHES%-1) do (
    echo.
    echo =====  Batch %%B / %TOTAL_BATCHES% =====
    scrapy crawl politician_content_spider -a batch=%%B
    if errorlevel 1 (
        echo ===== Scrapy returned an error in batch %%B – aborting =====
        exit /b 1
    )
)

echo.
echo =====  ALL BATCHES FINISHED  =====
endlocal
exit /b 0