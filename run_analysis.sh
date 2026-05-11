#!/bin/bash

# Navigate to project directory
cd /home/miracle/project

# Define the path to your virtual environment's python
VENV_PYTHON="./sparkenv311/bin/python"

# Environment Setup
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export PYSPARK_PYTHON=$VENV_PYTHON
export PYSPARK_DRIVER_PYTHON=$VENV_PYTHON

echo "Starting data pipeline: $(date)"

# Run using the VENV python
$VENV_PYTHON analysis_pipeline.py

if [ $? -eq 0 ]; then
    echo "Success: Pipeline finished at $(date)"
else
    echo "Error: Pipeline failed at $(date)"
    exit 1
fi
