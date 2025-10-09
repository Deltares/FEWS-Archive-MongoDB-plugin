#!/bin/bash
# Copyright (c) 2025 INFISYS INC

# [config_path] [model_path]
# ./graphcast.sh "/root/_git/FEWS-Archive/archive-tva_graphcast/configurations/gfs_posix_config.xml" "GraphCastOperationalGfs"

set -e

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform

cp "$2/.cdsapirc" "$HOME"

"python/bin/python" -m pip install --upgrade "$2/graphcast.zip"
"python/bin/python" -m pip cache purge
"python/bin/python" -m tva_graphcast --config_path "$1"
