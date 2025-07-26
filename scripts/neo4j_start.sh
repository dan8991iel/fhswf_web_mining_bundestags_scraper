#!/bin/bash
# ===============================================================
# Start Neo4j 5.x in *console* mode, using project-local folders
# - Data / logs:  neo4j_database/data  , neo4j_database/logs
# - Config file : neo4j_database/conf/neo4j.conf
# - Binaries    : neo4j_database/neo4j_home
# ===============================================================

# Resolve project root (folder *above* scripts)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRJ="$(cd "$SCRIPT_DIR/.." && pwd)"

# Define paths
NEO4J_HOME="$PRJ/neo4j_database/neo4j_home"
NEO4J_CONF="$PRJ/neo4j_database/conf"

# Export env-vars for neo4j
export NEO4J_HOME="$NEO4J_HOME"
export NEO4J_CONF="$NEO4J_CONF"

echo
echo "==============================================================="
echo " Starting Neo4j 5.x"
echo " NEO4J_HOME : $NEO4J_HOME"
echo " NEO4J_CONF : $NEO4J_CONF"
echo "==============================================================="
echo

"$NEO4J_HOME/bin/neo4j" console 