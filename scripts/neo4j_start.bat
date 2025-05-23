@echo off
REM ===============================================================
REM  Start Neo4j 5.x in *console* mode, using project-local folders
REM  - Data / logs:  neo4j_database\data  , neo4j_database\logs
REM  - Config file : neo4j_database\conf\neo4j.conf
REM  - Binaries    : neo4j_database\neo4j_home
REM ===============================================================
setlocal EnableDelayedExpansion

REM ── Resolve project root (folder *above* scripts) ───────────────
for %%I in ("%~dp0..") do set "PRJ=%%~fI"

REM ── Define paths ------------------------------------------------
set "NEO4J_HOME=%PRJ%\neo4j_database\neo4j_home"
set "NEO4J_CONF=%PRJ%\neo4j_database\conf"

REM ── Export env-vars for neo4j.bat ------------------------------
set "NEO4J_HOME=%NEO4J_HOME%"
set "NEO4J_CONF=%NEO4J_CONF%"

echo.
echo ===============================================================
echo  Starting Neo4j 5.x
echo  NEO4J_HOME : %NEO4J_HOME%
echo  NEO4J_CONF : %NEO4J_CONF%
echo ===============================================================
echo.

"%NEO4J_HOME%\bin\neo4j.bat" console

endlocal
