@echo off
REM ===============================================================
REM  Stop the Neo4j instance started by start_neo4j.bat
REM ===============================================================
setlocal EnableDelayedExpansion
for %%I in ("%~dp0..") do set "PRJ=%%~fI"
set "NEO4J_HOME=%PRJ%\neo4j_database\neo4j_home"

echo.
echo ===============================================================
echo  Stopping Neo4j ...
echo ===============================================================
echo.

"%NEO4J_HOME%\bin\neo4j.bat" stop

endlocal
