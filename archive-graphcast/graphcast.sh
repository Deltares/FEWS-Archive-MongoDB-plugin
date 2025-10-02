#!/bin/bash
# Copyright (c) 2025 INFISYS INC

#[config_path] [environment_install_base_path] [model_path]
#./graphcast.sh "~/_git/FEWS-Archive/archive-graphcast/configurations/gfs_posix_config.xml" "~/_git/FEWS-Archive/archive-graphcast/build" "~/_git/FEWS-Archive/archive-graphcast/models/GraphCastOperationalGfs"
set -e

cp "$3/.cdsapirc" "$HOME"

"$2/python/bin/python" -m pip install --upgrade "$3/graphcast.zip"
"$2/python/bin/python" -m pip cache purge
"$2/python/bin/python" run_graphcast.py --config_path "$1"
