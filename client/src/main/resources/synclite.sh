#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA_CMD="$JAVA_HOME/bin/java"
else
  JAVA_CMD="java"
fi

"$JAVA_CMD" -classpath "$SCRIPT_DIR/synclite-client-2023.08.26.jar:$SCRIPT_DIR/*" com.synclite.client.Main $1 $2 $3 $4 $5
