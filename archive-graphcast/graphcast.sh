#!/bin/bash
# Copyright (c) 2025 INFISYS INC

# [config_path] [model_name]
# ./graphcast.sh "/root/_git/FEWS-Archive/archive-graphcast/configurations/gfs_posix_config.xml" "GraphCastOperationalGfs"

set -e

d=$( dirname "$0" )

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform

if [ -f "$d/$2/.cdsapirc" ]; then cp "$d/$2/.cdsapirc" "$HOME"; fi

"$d/python/bin/python" -m pip install --root-user-action ignore --upgrade "$d/$2/graphcast.zip"
"$d/python/bin/python" -m pip cache purge
"$d/python/bin/python" -m tva_graphcast --config_path "$1" --model_path "$d/$2"
