#!/bin/bash
# ===============================================================
# Stop the Neo4j instance started by start_neo4j.sh
# ===============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRJ="$(cd "$SCRIPT_DIR/.." && pwd)"
NEO4J_HOME="$PRJ/neo4j_database/neo4j_home"

echo
echo "==============================================================="
echo " Stopping Neo4j ..."
echo "==============================================================="
echo

"$NEO4J_HOME/bin/neo4j" stop 