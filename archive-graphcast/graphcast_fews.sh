#!/bin/bash
# Copyright (c) 2025 INFISYS INC

# [config_path] [model_name]
# ./graphcast.sh "/root/_git/FEWS-Archive/archive-graphcast/configurations/gfs_posix_config.xml" "GraphCastOperationalGfs"

set -e

cd ~/graphcast

source ./graphcast.sh "$1" "$2"