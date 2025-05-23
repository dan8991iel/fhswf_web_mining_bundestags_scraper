@echo off
setlocal
for %%I in ("%~dp0..") do set "PRJ=%%~fI"
set "NEO4J_HOME=%PRJ%\neo4j_database\neo4j_home"

"%NEO4J_HOME%\bin\neo4j.bat" status
endlocal
