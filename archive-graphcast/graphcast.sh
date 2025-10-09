#!/bin/bash
# Copyright (c) 2025 INFISYS INC

# [config_path] [environment_install_base_path] [model_sub_path]
# ./graphcast.sh "/root/_git/FEWS-Archive/archive-tva_graphcast/configurations/gfs_posix_config.xml" "/root/_git/FEWS-Archive/archive-tva_graphcast/build" "GraphCastOperationalGfs"

set -e

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform

cp "$2/$3/.cdsapirc" "$HOME"

"$2/python/bin/python" -m pip install --upgrade "$2/$3/graphcast.zip"
"$2/python/bin/python" -m pip cache purge
"$2/python/bin/python" -m tva_graphcast --config_path "$1"
