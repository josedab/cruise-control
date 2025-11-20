#!/bin/bash
# Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.
#
# Validates Cruise Control configuration without starting the service

if [ $# -lt 1 ]; then
  echo "USAGE: $0 <config-file>"
  echo
  echo "This tool validates a Cruise Control configuration file without starting the service."
  echo "It checks for syntax errors, invalid ranges, dependency issues, and connectivity problems."
  echo
  echo "Exit codes:"
  echo "  0 - Configuration is valid"
  echo "  1 - Configuration has errors"
  echo "  2 - Configuration has warnings only"
  echo "  3 - Tool error (invalid arguments, file not found, etc.)"
  exit 3
fi

CONFIG_FILE=$1

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Configuration file not found: $CONFIG_FILE"
  exit 3
fi

base_dir=$(dirname $0)

# Construct classpath
if [ -z "$CLASSPATH" ]; then
  CLASSPATH="$base_dir/cruise-control/build/libs/*:$base_dir/cruise-control/build/dependant-libs/*"
  CLASSPATH="$CLASSPATH:$base_dir/cruise-control-core/build/libs/*:$base_dir/cruise-control-core/build/dependant-libs/*"
else
  CLASSPATH="$CLASSPATH:$base_dir/cruise-control/build/libs/*:$base_dir/cruise-control/build/dependant-libs/*"
  CLASSPATH="$CLASSPATH:$base_dir/cruise-control-core/build/libs/*:$base_dir/cruise-control-core/build/dependant-libs/*"
fi

# Set Java options
if [ -z "$KAFKA_OPTS" ]; then
  KAFKA_OPTS="-Xmx256M"
fi

# Run the validator
exec java $KAFKA_OPTS -cp $CLASSPATH com.linkedin.kafka.cruisecontrol.config.validation.ConfigValidatorTool "$CONFIG_FILE"
