#!/bin/bash
# Copyright (c) 2025 INFISYS INC

#[config_path] [environment_install_base_path] [model_path]
#./tva_graphcast.sh "~/_git/FEWS-Archive/archive-tva_graphcast/configurations/gfs_posix_config.xml" "~/_git/FEWS-Archive/archive-tva_graphcast/build" "~/_git/FEWS-Archive/archive-tva_graphcast/models/GraphCastOperationalGfs"
set -e

cp "$3/.cdsapirc" "$HOME"

"$2/python/bin/python" -m pip install --upgrade "$3/graphcast.zip"
"$2/python/bin/python" -m pip cache purge
"$2/python/bin/python" -m tva_graphcast --config_path "$1"
