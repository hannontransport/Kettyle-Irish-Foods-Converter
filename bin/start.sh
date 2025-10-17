#!/bin/bash

# Define the path to your Python script
PYTHON_SCRIPT="/home/jlamont/Kettyle-Irish-Foods-Converter/converter.py"

# Define the path to your log file
LOG_FILE="/home/jlamont/Kettyle-Irish-Foods-Converter/logs/converter.txt"

# Define the path to the PID file
PID_FILE="/home/jlamont/Kettyle-Irish-Foods-Converter/bin/pid_file.pid"

# Define the logs directory
LOGS_DIR="/home/jlamont/Kettyle-Irish-Foods-Converter/logs"

# Create logs directory if it doesn't exist
if [ ! -d "$LOGS_DIR" ]; then
  mkdir -p "$LOGS_DIR"
fi

# Activate the virtual environment
source /home/jlamont/Kettyle-Irish-Foods-Converter/venv/bin/activate

# Set PYTHONPATH to include the project directory
export PYTHONPATH=/home/jlamont/Kettyle-Irish-Foods-Converter

# Start the Python script in the background using nohup
nohup python3 $PYTHON_SCRIPT > $LOG_FILE 2>&1 &

# Save the process ID (PID) of the background process to a file
echo $! > $PID_FILE

echo "Python tool started with PID $(cat $PID_FILE)"