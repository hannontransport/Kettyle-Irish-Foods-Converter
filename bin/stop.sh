#!/bin/bash

# Define the path to the PID file
PID_FILE="/home/jlamont//Kettyle-Irish-Foods-Converter/bin/pid_file.pid"

# Check if the PID file exists
if [ ! -f $PID_FILE ]; then
  echo "PID file not found. Is the tool running?"
  exit 1
fi

# Read the PID from the file
PID=$(cat $PID_FILE)

# Kill the process with the PID
kill $PID

# Remove the PID file
rm $PID_FILE

echo "Python tool stopped"