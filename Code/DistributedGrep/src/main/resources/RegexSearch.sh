#!/usr/bin/bash

#Check HADOOP_HOME
if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME is not set. Please update your environment variables and run again"
    exit 1
fi

#JAR Path
HADOOP_JAR="/home/user/git/PROJECT/Code/DistributedGrep/target/DistributedGrep-1.0.0.jar"

#Set Driver
MAIN_CLASS=main.java.org.finalyearproject.distributedgrep.Driver

#Check for command-line arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_path> <output_path>"
    exit 1
fi

#Set Inputs
INPUT_PATH=$1
OUTPUT_PATH=$2

#Run command
${HADOOP_HOME}/bin/hadoop jar $HADOOP_JAR $MAIN_CLASS $INPUT_PATH $OUTPUT_PATH
