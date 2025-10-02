#!/bin/bash
# Copyright (c) 2025 INFISYS INC

#[config_path] [environment_install_base_path] [model_path]
#./graphcast.sh /apps/fews/fss/201/Models/Import/GraphCast/run_info.xml /apps/fews/graphcast /apps/fews/fss/201/Modules/GraphCastOperationalGfs
set -e

cp "$3/.cdsapirc" "$HOME"

"$2/Python/python" -m pip install --upgrade "$3/graphcast.zip"
"$2/Python/python" -m pip cache purge
"$2/Python/python" run_graphcast.py --config_path "$1"