#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRJ="$(cd "$SCRIPT_DIR/.." && pwd)"
NEO4J_HOME="$PRJ/neo4j_database/neo4j_home"

"$NEO4J_HOME/bin/neo4j" status 