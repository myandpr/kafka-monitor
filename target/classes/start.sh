#!/bin/sh
PRG="$0"
PRGDIR=`dirname "$PRG"`
EXECUTABLE=bigdata-monitor.sh
exec "$PRGDIR"/"$EXECUTABLE" start "$@"